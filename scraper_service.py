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
    """Main service to orchestrate job scraping workflow - supports LinkedIn and Indeed"""
    
    def __init__(self, db: Session):
        self.db = db
    
    def _extract_job_id(self, job: Dict, platform: str) -> str:
        """Extract job ID based on platform"""
        if platform == "indeed":
            # Indeed might not have a direct ID field, create one from URL or other unique field
            return str(job.get("id", job.get("jobKey", "")))
        else:  # linkedin
            return str(job.get("id", ""))
    
    def _extract_job_fields(self, job: Dict, platform: str) -> Dict:
        """Extract job fields based on platform"""
        if platform == "indeed":
            company_details = job.get("companyDetails", {})
            about_section = company_details.get("aboutSectionViewModel", {})
            about_company = about_section.get("aboutCompany", {})
            hq_location = about_company.get("headquartersLocation", {})
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
                "is_new_job": job.get("newJob", False)
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
                "is_new_job": None
            }
    
    async def execute_job_search(self, job_search_id: int) -> JobRun:
        """
        Execute a complete job search workflow for LinkedIn or Indeed:
        1. Start Apify actor
        2. Wait for completion
        3. Fetch results
        4. Filter jobs
        5. Send to Clay
        6. Save to database
        
        Returns:
            JobRun instance with execution results
        """
        # Get job search configuration
        job_search = self.db.query(JobSearch).filter(JobSearch.id == job_search_id).first()
        
        if not job_search:
            raise ValueError(f"Job search with ID {job_search_id} not found")
        
        if not job_search.is_active:
            raise ValueError(f"Job search '{job_search.name}' is not active")
        
        platform = job_search.platform or "linkedin"
        logger.info(f"Starting {platform} job search: {job_search.name}")
        
        # Create job run record
        job_run = JobRun(
            job_search_id=job_search.id,
            status="running",
            started_at=datetime.utcnow()
        )
        self.db.add(job_run)
        self.db.commit()
        self.db.refresh(job_run)
        
        logger.info(f"Created job run {job_run.id} for {platform} job search '{job_search.name}'")
        
        # Initialize services
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
            
            # Step 1: Start Apify actor
            logger.info(f"Starting {platform} Apify actor for URL: {job_search.search_url}")
            run_data = await apify_service.start_actor_run(
                actor_id=job_search.apify_actor_id,
                platform=platform,
                urls=[job_search.search_url],
                count=job_search.max_results,
                scrape_company=job_search.scrape_company,
                use_browser=job_search.use_browser if platform == "indeed" else False,
                use_apify_proxy=job_search.use_apify_proxy if platform == "indeed" else True
            )
            
            job_run.apify_run_id = run_data["id"]
            job_run.apify_dataset_id = run_data.get("defaultDatasetId")
            self.db.commit()
            
            logger.info(f"Apify run started: {run_data['id']}")
            
            # Step 2: Wait for completion
            logger.info(f"Waiting for {platform} Apify run {run_data['id']} to complete")
            final_status = await apify_service.wait_for_completion(run_data["id"])
            
            logger.info(f"Apify run completed with status: {final_status['status']}")
            
            if final_status["status"] != "SUCCEEDED":
                raise Exception(f"Apify run failed with status: {final_status['status']}")
            
            # Step 3: Fetch results
            dataset_id = final_status.get("defaultDatasetId") or job_run.apify_dataset_id
            logger.info(f"Fetching {platform} data from dataset {dataset_id}")
            jobs = await apify_service.get_dataset_items(dataset_id)
            
            job_run.jobs_found = len(jobs)
            self.db.commit()
            
            logger.info(f"Found {len(jobs)} jobs")
            
            if not jobs:
                logger.warning(f"No {platform} jobs found in dataset")
                job_run.status = "success"
                job_run.completed_at = datetime.utcnow()
                job_search.last_run_at = datetime.utcnow()
                job_search.last_status = "success"
                self.db.commit()
                return job_run
            
            # Step 4: Filter jobs
            logger.info(f"Filtering {len(jobs)} {platform} jobs")
            job_filter = JobFilter(
                platform=platform,
                company_name_excludes=job_search.company_name_excludes or [],
                industries_excludes=job_search.industries_excludes or [],
                max_employee_count=job_search.max_employee_count,
                min_employee_count=job_search.min_employee_count,
                job_description_filters=job_search.job_description_filters
            )
            
            passed_jobs, filtered_jobs = job_filter.filter_jobs(jobs)
            job_run.jobs_filtered = len(filtered_jobs)
            self.db.commit()
            
            logger.info(f"Filter results: {len(passed_jobs)} passed, {len(filtered_jobs)} filtered")
            
            # Save all jobs to database (skip duplicates)
            saved_count = 0
            duplicate_count = 0
            
            for job in jobs:
                should_filter = job in filtered_jobs
                filter_reason = job.get("filter_reason") if should_filter else None
                
                try:
                    fields = self._extract_job_fields(job, platform)
                    job_id = fields.get("job_id")
                    
                    # Check if job already exists
                    existing = self.db.query(ScrapedJob).filter(
                        ScrapedJob.job_id == job_id
                    ).first()
                    
                    if existing:
                        duplicate_count += 1
                        logger.debug(f"Skipping duplicate job: {job_id}")
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
                    
                    # Commit in batches of 100 to avoid huge transactions
                    if saved_count % 100 == 0:
                        self.db.commit()
                        logger.debug(f"Committed batch of 100 jobs (total: {saved_count})")
                    
                except Exception as e:
                    logger.error(f"Error saving job to database: {e}")
                    continue
            
            # Final commit
            self.db.commit()
            logger.info(f"Saved {saved_count} new jobs to database ({duplicate_count} duplicates skipped)")
            
            # Step 5: Send to Clay
            if passed_jobs:
                logger.info(f"Sending {len(passed_jobs)} {platform} jobs to Clay")
                sent_count = await clay_service.send_jobs(
                    jobs=passed_jobs,
                    keyword=job_search.keyword,
                    url_scraped=job_search.search_url,
                    platform=platform
                )
                
                job_run.jobs_sent = sent_count
                
                # Update sent status in database (only for new jobs)
                for job in passed_jobs:
                    job_id = self._extract_job_id(job, platform)
                    try:
                        # Only update if job exists in current run
                        updated = self.db.query(ScrapedJob).filter(
                            ScrapedJob.job_run_id == job_run.id,
                            ScrapedJob.job_id == job_id
                        ).update({"sent_to_clay": True})
                        
                        if updated == 0:
                            logger.debug(f"Job {job_id} not found in current run (likely duplicate)")
                    except Exception as e:
                        logger.error(f"Error updating sent status: {e}")
                
                self.db.commit()
                logger.info(f"Sent {sent_count} jobs to Clay")
            else:
                logger.info("No jobs passed filters, nothing to send to Clay")
            
            # Mark as successful
            job_run.status = "success"
            job_run.completed_at = datetime.utcnow()
            
            # Update job search
            job_search.last_run_at = datetime.utcnow()
            job_search.last_status = "success"
            
            self.db.commit()
            
            logger.info(
                f"{platform.capitalize()} job run {job_run.id} completed successfully: "
                f"{job_run.jobs_found} found, {job_run.jobs_filtered} filtered, "
                f"{job_run.jobs_sent} sent to Clay"
            )
            
            return job_run
        
        except Exception as e:
            logger.error(f"{platform.capitalize()} job run {job_run.id} failed: {e}", exc_info=True)
            
            # Update job run status
            job_run.status = "failed"
            job_run.error_message = str(e)[:1000]  # Limit error message length
            job_run.completed_at = datetime.utcnow()
            
            # Update job search status
            job_search.last_run_at = datetime.utcnow()
            job_search.last_status = "failed"
            
            self.db.commit()
            
            logger.error(f"Job run {job_run.id} marked as failed")
            
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
