from sqlalchemy.orm import Session
from datetime import datetime
from typing import Optional
import logging

from models import JobSearch, JobRun, ScrapedJob
from apify_service import ApifyService
from job_filter import JobFilter
from clay_service import ClayWebhookService
from config import settings

logger = logging.getLogger(__name__)


class JobScraperService:
    """Main service to orchestrate job scraping workflow"""
    
    def __init__(self, db: Session):
        self.db = db
    
    async def execute_job_search(self, job_search_id: int) -> JobRun:
        """
        Execute a complete job search workflow:
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
        
        # Create job run record
        job_run = JobRun(
            job_search_id=job_search.id,
            status="running"
        )
        self.db.add(job_run)
        self.db.commit()
        self.db.refresh(job_run)
        
        logger.info(f"Starting job run {job_run.id} for job search '{job_search.name}'")
        
        # Initialize services
        apify_token = job_search.apify_token or settings.DEFAULT_APIFY_TOKEN
        if not apify_token:
            error_msg = "No Apify token configured"
            job_run.status = "failed"
            job_run.error_message = error_msg
            job_run.completed_at = datetime.utcnow()
            self.db.commit()
            raise ValueError(error_msg)
        
        apify_service = ApifyService(apify_token)
        clay_service = ClayWebhookService(
            webhook_url=job_search.clay_webhook_url,
            batch_size=job_search.batch_size,
            batch_interval_ms=job_search.batch_interval_ms
        )
        
        try:
            # Step 1: Start Apify actor
            logger.info(f"Starting Apify actor for URL: {job_search.search_url}")
            run_data = await apify_service.start_actor_run(
                actor_id=job_search.apify_actor_id,
                urls=[job_search.search_url],
                count=job_search.max_results,
                scrape_company=job_search.scrape_company
            )
            
            job_run.apify_run_id = run_data["id"]
            job_run.apify_dataset_id = run_data.get("defaultDatasetId")
            self.db.commit()
            
            # Step 2: Wait for completion
            logger.info(f"Waiting for Apify run {run_data['id']} to complete")
            final_status = await apify_service.wait_for_completion(run_data["id"])
            
            if final_status["status"] != "SUCCEEDED":
                raise Exception(f"Apify run failed with status: {final_status['status']}")
            
            # Step 3: Fetch results
            dataset_id = final_status.get("defaultDatasetId") or job_run.apify_dataset_id
            logger.info(f"Fetching data from dataset {dataset_id}")
            jobs = await apify_service.get_dataset_items(dataset_id)
            
            job_run.jobs_found = len(jobs)
            self.db.commit()
            
            if not jobs:
                logger.warning("No jobs found in dataset")
                job_run.status = "success"
                job_run.completed_at = datetime.utcnow()
                self.db.commit()
                return job_run
            
            # Step 4: Filter jobs
            logger.info(f"Filtering {len(jobs)} jobs")
            job_filter = JobFilter(
                company_name_excludes=job_search.company_name_excludes or [],
                industries_excludes=job_search.industries_excludes or [],
                max_employee_count=job_search.max_employee_count,
                min_employee_count=job_search.min_employee_count
            )
            
            passed_jobs, filtered_jobs = job_filter.filter_jobs(jobs)
            job_run.jobs_filtered = len(filtered_jobs)
            self.db.commit()
            
            # Save all jobs to database
            for job in jobs:
                should_filter = job in filtered_jobs
                filter_reason = job.get("filter_reason") if should_filter else None
                
                scraped_job = ScrapedJob(
                    job_run_id=job_run.id,
                    job_id=str(job.get("id", "")),
                    title=job.get("title"),
                    company_name=job.get("companyName"),
                    company_url=job.get("companyWebsite"),
                    company_linkedin_url=job.get("companyLinkedinUrl"),
                    company_description=job.get("companyDescription"),
                    company_employees_count=job.get("companyEmployeesCount"),
                    industries=job.get("industries"),
                    location=job.get("location"),
                    posted_at=job.get("postedAt"),
                    description_text=job.get("descriptionText"),
                    apply_url=job.get("applyUrl"),
                    job_url=job.get("link"),
                    filtered_out=should_filter,
                    filter_reason=filter_reason
                )
                self.db.add(scraped_job)
            
            self.db.commit()
            
            # Step 5: Send to Clay
            if passed_jobs:
                logger.info(f"Sending {len(passed_jobs)} jobs to Clay")
                sent_count = await clay_service.send_jobs(
                    jobs=passed_jobs,
                    keyword=job_search.keyword,
                    url_scraped=job_search.search_url
                )
                
                job_run.jobs_sent = sent_count
                
                # Update sent status in database
                for job in passed_jobs:
                    self.db.query(ScrapedJob).filter(
                        ScrapedJob.job_run_id == job_run.id,
                        ScrapedJob.job_id == str(job.get("id", ""))
                    ).update({"sent_to_clay": True})
                
                self.db.commit()
            
            # Mark as successful
            job_run.status = "success"
            job_run.completed_at = datetime.utcnow()
            
            # Update job search
            job_search.last_run_at = datetime.utcnow()
            job_search.last_status = "success"
            
            self.db.commit()
            
            logger.info(
                f"Job run {job_run.id} completed successfully: "
                f"{job_run.jobs_found} found, {job_run.jobs_filtered} filtered, "
                f"{job_run.jobs_sent} sent to Clay"
            )
            
            return job_run
        
        except Exception as e:
            logger.error(f"Job run {job_run.id} failed: {e}", exc_info=True)
            
            job_run.status = "failed"
            job_run.error_message = str(e)
            job_run.completed_at = datetime.utcnow()
            
            job_search.last_run_at = datetime.utcnow()
            job_search.last_status = "failed"
            
            self.db.commit()
            
            raise
        
        finally:
            await apify_service.close()
            await clay_service.close()
