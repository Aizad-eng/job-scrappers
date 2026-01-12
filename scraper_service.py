from sqlalchemy.orm import Session
from datetime import datetime
from typing import Optional, Dict
import logging

from models import JobSearch, JobRun, ScrapedJob
from apify_service import ApifyService
from job_filter import JobFilter
from clay_service import ClayWebhookService
from config import settings

logger = logging.getLogger(__name__)


class JobScraperService:
    """Main service to orchestrate job scraping workflow - supports LinkedIn, Indeed, and Fantastic Jobs"""
    
    def __init__(self, db: Session):
        self.db = db
    
    def _update_job_run(self, job_run: JobRun, **kwargs):
        """Helper to update job run and commit immediately"""
        for key, value in kwargs.items():
            setattr(job_run, key, value)
        try:
            self.db.commit()
            self.db.refresh(job_run)
            logger.info(f"Job run {job_run.id} updated: {kwargs}")
        except Exception as e:
            logger.error(f"Failed to update job run: {e}")
            self.db.rollback()
            raise
    
    def _extract_job_id(self, job: Dict, platform: str) -> str:
        """Extract job ID based on platform"""
        if platform == "indeed":
            return str(job.get("id", job.get("jobKey", "")))
        elif platform == "fantastic_jobs":
            return str(job.get("id", ""))
        else:  # linkedin
            return str(job.get("id", ""))
    
    def _extract_job_fields(self, job: Dict, platform: str) -> Dict:
        """Extract job fields based on platform"""
        if platform == "indeed":
            company_details = job.get("companyDetails", {})
            about_section = company_details.get("aboutSectionViewModel", {})
            about_company = about_section.get("aboutCompany", {})
            website_info = about_company.get("websiteUrl", {})
            salary_snippet = job.get("salarySnippet", {})
            
            return {
                "job_id": self._extract_job_id(job, platform),
                "title": job.get("displayTitle"),
                "company_name": job.get("company"),
                "company_url": website_info.get("url", "") if isinstance(website_info, dict) else "",
                "company_linkedin_url": None,
                "company_description": about_company.get("description"),
                "company_employees_count": None,
                "company_employee_range": about_company.get("employeeRange"),
                "industries": about_company.get("industry"),
                "location": job.get("formattedLocation"),
                "posted_at": str(job.get("pubDate", "")),
                "description_text": job.get("jobDescription"),
                "apply_url": job.get("thirdPartyApplyUrl"),
                "job_url": job.get("link"),
                "salary": salary_snippet.get("text", "") if salary_snippet else None,
                "currency": salary_snippet.get("currency", "") if salary_snippet else None,
                "is_new_job": job.get("newJob", False),
                # Fantastic Jobs specific (null for Indeed)
                "experience_level": None,
                "work_arrangement": None,
                "key_skills": None,
                "source_domain": None,
            }
        
        elif platform == "fantastic_jobs":
            # Get location from derived array
            locations_derived = job.get("locations_derived", [])
            location = locations_derived[0] if locations_derived else ""
            
            # Get salary info
            salary_value = job.get("ai_salary_value")
            salary_min = job.get("ai_salary_minvalue")
            salary_max = job.get("ai_salary_maxvalue")
            salary_currency = job.get("ai_salary_currency", "")
            
            # Build salary string
            salary_str = ""
            if salary_value:
                salary_str = f"{salary_currency}{salary_value}"
            elif salary_min and salary_max:
                salary_str = f"{salary_currency}{salary_min} - {salary_currency}{salary_max}"
            
            return {
                "job_id": self._extract_job_id(job, platform),
                "title": job.get("title"),
                "company_name": job.get("organization"),
                "company_url": job.get("organization_url"),
                "company_linkedin_url": job.get("linkedin_org_url"),
                "company_description": job.get("linkedin_org_description"),
                "company_employees_count": job.get("linkedin_org_employees"),
                "company_employee_range": job.get("linkedin_org_size"),
                "industries": job.get("linkedin_org_industry"),
                "location": location,
                "posted_at": job.get("date_posted"),
                "description_text": job.get("description_text"),
                "apply_url": job.get("url"),
                "job_url": job.get("url"),
                "salary": salary_str,
                "currency": salary_currency,
                "is_new_job": None,
                # Fantastic Jobs specific fields
                "experience_level": job.get("ai_experience_level"),
                "work_arrangement": job.get("ai_work_arrangement"),
                "key_skills": job.get("ai_key_skills"),
                "source_domain": job.get("source_domain"),
            }
        
        else:  # linkedin
            return {
                "job_id": self._extract_job_id(job, platform),
                "title": job.get("title"),
                "company_name": job.get("companyName"),
                "company_url": job.get("companyWebsite"),
                "company_linkedin_url": job.get("companyLinkedinUrl"),
                "company_description": job.get("companyDescription"),
                "company_employees_count": job.get("companyEmployeesCount"),
                "company_employee_range": None,
                "industries": job.get("industries"),
                "location": job.get("location"),
                "posted_at": job.get("postedAt"),
                "description_text": job.get("descriptionText"),
                "apply_url": job.get("applyUrl"),
                "job_url": job.get("link"),
                "salary": None,
                "currency": None,
                "is_new_job": None,
                # Fantastic Jobs specific (null for LinkedIn)
                "experience_level": None,
                "work_arrangement": None,
                "key_skills": None,
                "source_domain": None,
            }
    
    async def execute_job_search(self, job_search_id: int) -> JobRun:
        """
        Execute a complete job search workflow for LinkedIn, Indeed, or Fantastic Jobs
        """
        # Fetch fresh job_search from DB
        job_search = self.db.query(JobSearch).filter(JobSearch.id == job_search_id).first()
        
        if not job_search:
            raise ValueError(f"Job search with ID {job_search_id} not found")
        
        if not job_search.is_active:
            raise ValueError(f"Job search '{job_search.name}' is not active")
        
        platform = job_search.platform or "linkedin"
        logger.info(f"========== STARTING {platform.upper()} JOB: {job_search.name} ==========")
        
        # Create job run record
        job_run = JobRun(
            job_search_id=job_search.id,
            status="running",
            started_at=datetime.utcnow()
        )
        self.db.add(job_run)
        self.db.commit()
        self.db.refresh(job_run)
        
        logger.info(f"Created job run ID: {job_run.id}")
        
        apify_service = None
        clay_service = None
        
        try:
            # Get Apify token
            apify_token = job_search.apify_token or settings.DEFAULT_APIFY_TOKEN
            if not apify_token:
                raise ValueError("No Apify token configured")
            
            apify_service = ApifyService(apify_token)
            clay_service = ClayWebhookService(
                webhook_url=job_search.clay_webhook_url,
                batch_size=job_search.batch_size,
                batch_interval_ms=job_search.batch_interval_ms
            )
            
            # ============================================================
            # STEP 1: Start Apify actor
            # ============================================================
            logger.info(f"[STEP 1] Starting {platform} Apify actor...")
            
            if platform == "fantastic_jobs":
                # Use title_search, location_search, etc. for Fantastic Jobs
                run_data = await apify_service.start_actor_run(
                    platform=platform,
                    title_search=job_search.title_search or [],
                    location_search=job_search.location_search or ["united states"],
                    employer_search=job_search.employer_search or [],
                    time_range=job_search.time_range or "7d",
                    count=job_search.max_results,
                    min_employees=job_search.min_employee_count,
                    max_employees=job_search.max_employee_count,
                    include_ai=job_search.include_ai if hasattr(job_search, 'include_ai') else True,
                    include_linkedin=job_search.include_linkedin if hasattr(job_search, 'include_linkedin') else True,
                )
            else:
                # Use URL-based search for LinkedIn/Indeed
                run_data = await apify_service.start_actor_run(
                    platform=platform,
                    urls=job_search.search_url,
                    count=job_search.max_results,
                    scrape_company=job_search.scrape_company
                )
            
            # Update job run with Apify IDs
            self._update_job_run(
                job_run,
                apify_run_id=run_data["id"],
                apify_dataset_id=run_data.get("defaultDatasetId")
            )
            
            logger.info(f"[STEP 1] Apify run started: {run_data['id']}")
            
            # ============================================================
            # STEP 2: Wait for Apify completion
            # ============================================================
            logger.info(f"[STEP 2] Waiting for Apify to complete...")
            
            final_status = await apify_service.wait_for_completion(
                run_id=run_data["id"],
                platform=platform
            )
            
            apify_status = final_status.get("status")
            logger.info(f"[STEP 2] Apify finished with status: {apify_status}")
            
            if apify_status != "SUCCEEDED":
                raise Exception(f"Apify run failed with status: {apify_status}")
            
            # ============================================================
            # STEP 3: Fetch results from dataset
            # ============================================================
            logger.info(f"[STEP 3] Fetching results from Apify dataset...")
            
            dataset_id = final_status.get("defaultDatasetId") or job_run.apify_dataset_id
            jobs = await apify_service.get_dataset_items(dataset_id)
            
            self._update_job_run(job_run, jobs_found=len(jobs))
            logger.info(f"[STEP 3] Found {len(jobs)} jobs")
            
            # Handle no jobs found
            if not jobs:
                logger.info("[STEP 3] No jobs found - marking as success")
                self._update_job_run(
                    job_run,
                    status="success",
                    completed_at=datetime.utcnow(),
                    jobs_filtered=0,
                    jobs_sent=0
                )
                job_search.last_run_at = datetime.utcnow()
                job_search.last_status = "success"
                self.db.commit()
                return job_run
            
            # ============================================================
            # STEP 4: Filter jobs
            # ============================================================
            logger.info(f"[STEP 4] Filtering {len(jobs)} jobs...")
            
            job_filter = JobFilter(
                platform=platform,
                company_name_excludes=job_search.company_name_excludes or [],
                industries_excludes=job_search.industries_excludes or [],
                max_employee_count=job_search.max_employee_count,
                min_employee_count=job_search.min_employee_count,
                job_description_filters=job_search.job_description_filters
            )
            
            passed_jobs, filtered_jobs = job_filter.filter_jobs(jobs)
            
            self._update_job_run(job_run, jobs_filtered=len(filtered_jobs))
            logger.info(f"[STEP 4] Filter results: {len(passed_jobs)} passed, {len(filtered_jobs)} filtered")
            
            # ============================================================
            # STEP 5: Save jobs to database
            # ============================================================
            logger.info(f"[STEP 5] Saving jobs to database...")
            
            saved_count = 0
            duplicate_count = 0
            
            for job in jobs:
                should_filter = job in filtered_jobs
                filter_reason = job.get("filter_reason") if should_filter else None
                
                try:
                    fields = self._extract_job_fields(job, platform)
                    job_id = fields.get("job_id")
                    
                    if not job_id:
                        continue
                    
                    # Check for duplicate
                    existing = self.db.query(ScrapedJob).filter(
                        ScrapedJob.job_id == job_id
                    ).first()
                    
                    if existing:
                        duplicate_count += 1
                        continue
                    
                    # Save new job
                    scraped_job = ScrapedJob(
                        job_run_id=job_run.id,
                        **fields,
                        filtered_out=should_filter,
                        filter_reason=filter_reason
                    )
                    self.db.add(scraped_job)
                    saved_count += 1
                    
                    # Batch commit every 50
                    if saved_count % 50 == 0:
                        self.db.commit()
                        logger.info(f"[STEP 5] Saved {saved_count} jobs...")
                    
                except Exception as e:
                    logger.error(f"[STEP 5] Error saving job: {e}")
                    continue
            
            self.db.commit()
            logger.info(f"[STEP 5] Saved {saved_count} new jobs, {duplicate_count} duplicates skipped")
            
            # ============================================================
            # STEP 6: Send to Clay
            # ============================================================
            sent_count = 0
            
            # Determine URL for Clay payload (use search_url or build from title_search)
            if platform == "fantastic_jobs":
                url_scraped = f"fantastic_jobs:{','.join(job_search.title_search or [])}"
            else:
                url_scraped = job_search.search_url
            
            if passed_jobs:
                logger.info(f"[STEP 6] Sending {len(passed_jobs)} jobs to Clay...")
                
                sent_count = await clay_service.send_jobs(
                    jobs=passed_jobs,
                    keyword=job_search.keyword,
                    url_scraped=url_scraped,
                    platform=platform
                )
                
                self._update_job_run(job_run, jobs_sent=sent_count)
                logger.info(f"[STEP 6] Sent {sent_count} jobs to Clay")
                
                # Mark jobs as sent
                for job in passed_jobs:
                    job_id = self._extract_job_id(job, platform)
                    try:
                        self.db.query(ScrapedJob).filter(
                            ScrapedJob.job_run_id == job_run.id,
                            ScrapedJob.job_id == job_id
                        ).update({"sent_to_clay": True})
                    except:
                        pass
                
                self.db.commit()
            else:
                logger.info("[STEP 6] No jobs to send (all filtered)")
                self._update_job_run(job_run, jobs_sent=0)
            
            # ============================================================
            # FINAL: Mark as success
            # ============================================================
            logger.info(f"[FINAL] Marking job run {job_run.id} as SUCCESS")
            
            self._update_job_run(
                job_run,
                status="success",
                completed_at=datetime.utcnow()
            )
            
            job_search.last_run_at = datetime.utcnow()
            job_search.last_status = "success"
            self.db.commit()
            
            logger.info(f"========== COMPLETED: {job_run.jobs_found} found, {job_run.jobs_filtered} filtered, {job_run.jobs_sent} sent ==========")
            
            return job_run
        
        except Exception as e:
            logger.error(f"========== FAILED: {e} ==========", exc_info=True)
            
            # Always update status on failure
            try:
                job_run.status = "failed"
                job_run.error_message = str(e)[:1000]
                job_run.completed_at = datetime.utcnow()
                job_search.last_run_at = datetime.utcnow()
                job_search.last_status = "failed"
                self.db.commit()
                logger.info(f"Job run {job_run.id} marked as FAILED")
            except Exception as commit_error:
                logger.error(f"Failed to update failure status: {commit_error}")
                try:
                    self.db.rollback()
                    job_run.status = "failed"
                    job_run.error_message = str(e)[:500]
                    job_run.completed_at = datetime.utcnow()
                    self.db.commit()
                except:
                    logger.error("Could not update job status at all")
            
            return job_run
        
        finally:
            # Always close services
            if apify_service:
                try:
                    await apify_service.close()
                except Exception as e:
                    logger.error(f"Error closing Apify service: {e}")
            
            if clay_service:
                try:
                    await clay_service.close()
                except Exception as e:
                    logger.error(f"Error closing Clay service: {e}")
