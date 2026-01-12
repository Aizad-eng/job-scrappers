"""
Scraper Service - Orchestrates the complete job scraping workflow.

Workflow:
1. Load actor config
2. Run Apify actor
3. Extract and filter jobs
4. Send to Clay webhook
5. Update database with results
"""

import logging
from datetime import datetime
from typing import Dict, Any, Optional
from sqlalchemy.orm import Session

from models import JobSearch, JobRun, ScrapedJob, ActorConfig
from actor_registry import ActorRegistry
from apify_service import ApifyService
from filter_service import FilterService
from clay_service import ClayService
from template_engine import extract_fields

logger = logging.getLogger(__name__)


class ScraperService:
    """Orchestrates the complete scraping workflow"""
    
    def __init__(self, db: Session, apify_token: str):
        self.db = db
        self.apify_token = apify_token
        self.actor_registry = ActorRegistry(db)
        self.apify_service = ApifyService(apify_token)
        self.filter_service = FilterService()
        self.clay_service = ClayService()
    
    async def execute_search(self, job_search_id: int) -> Dict[str, Any]:
        """
        Execute a complete job search workflow.
        
        Returns execution stats and any errors.
        """
        # Load the job search
        job_search = self.db.query(JobSearch).filter(
            JobSearch.id == job_search_id
        ).first()
        
        if not job_search:
            raise ValueError(f"Job search {job_search_id} not found")
        
        # Load the actor config
        actor_config = self.actor_registry.get_actor(job_search.actor_key)
        
        # Create a job run record
        job_run = JobRun(
            job_search_id=job_search.id,
            status="running"
        )
        self.db.add(job_run)
        self.db.commit()
        
        try:
            # Step 1: Run the Apify actor
            logger.info(f"Starting scrape for '{job_search.name}' using {actor_config.display_name}")
            
            apify_result = await self.apify_service.run_actor(
                actor_config=actor_config,
                job_search=job_search
            )
            
            job_run.apify_run_id = apify_result["run_id"]
            job_run.apify_dataset_id = apify_result["dataset_id"]
            
            raw_jobs = apify_result["items"]
            job_run.jobs_found = len(raw_jobs)
            
            logger.info(f"Found {len(raw_jobs)} jobs from Apify")
            
            # Step 2: Filter jobs
            passed_jobs, filtered_jobs = self.filter_service.filter_jobs(
                jobs=raw_jobs,
                actor_config=actor_config,
                job_search=job_search
            )
            
            job_run.jobs_filtered = len(filtered_jobs)
            
            # Step 3: Store scraped jobs
            for job_data in passed_jobs:
                extracted = extract_fields(job_data, actor_config.output_mapping or {})
                
                scraped_job = ScrapedJob(
                    job_run_id=job_run.id,
                    job_id=extracted.get("job_id"),
                    raw_data=job_data,
                    extracted_data=extracted,
                    sent_to_clay=False,
                    filtered_out=False
                )
                self.db.add(scraped_job)
            
            for job_data, reason in filtered_jobs:
                extracted = extract_fields(job_data, actor_config.output_mapping or {})
                
                scraped_job = ScrapedJob(
                    job_run_id=job_run.id,
                    job_id=extracted.get("job_id"),
                    raw_data=job_data,
                    extracted_data=extracted,
                    sent_to_clay=False,
                    filtered_out=True,
                    filter_reason=reason
                )
                self.db.add(scraped_job)
            
            self.db.commit()
            
            # Step 4: Send to Clay
            if passed_jobs and job_search.clay_webhook_url:
                # Build extra fields to include
                extra_fields = {
                    "platform": actor_config.actor_key,
                    "search_name": job_search.name
                }
                
                # Add keyword if available from inputs
                actor_inputs = job_search.actor_inputs or {}
                if actor_inputs.get("title_search"):
                    titles = actor_inputs["title_search"]
                    if isinstance(titles, list):
                        extra_fields["keyword"] = ", ".join(titles)
                    else:
                        extra_fields["keyword"] = titles
                
                clay_result = await self.clay_service.send_jobs(
                    jobs=passed_jobs,
                    actor_config=actor_config,
                    job_search=job_search,
                    extra_fields=extra_fields
                )
                
                job_run.jobs_sent = clay_result["sent"]
                
                # Update sent status
                if clay_result["sent"] > 0:
                    self.db.query(ScrapedJob).filter(
                        ScrapedJob.job_run_id == job_run.id,
                        ScrapedJob.filtered_out == False
                    ).update({"sent_to_clay": True})
                
                logger.info(f"Sent {clay_result['sent']} jobs to Clay")
            
            # Mark as complete
            job_run.status = "completed"
            job_run.completed_at = datetime.utcnow()
            
            # Update job search last run info
            job_search.last_run_at = datetime.utcnow()
            job_search.last_status = "completed"
            
            self.db.commit()
            
            return {
                "success": True,
                "run_id": job_run.id,
                "jobs_found": job_run.jobs_found,
                "jobs_filtered": job_run.jobs_filtered,
                "jobs_sent": job_run.jobs_sent,
                "error": None
            }
        
        except Exception as e:
            logger.exception(f"Error executing search: {e}")
            
            job_run.status = "failed"
            job_run.error_message = str(e)
            job_run.completed_at = datetime.utcnow()
            
            job_search.last_run_at = datetime.utcnow()
            job_search.last_status = "failed"
            
            self.db.commit()
            
            return {
                "success": False,
                "run_id": job_run.id,
                "jobs_found": 0,
                "jobs_filtered": 0,
                "jobs_sent": 0,
                "error": str(e)
            }
    
    async def test_actor(
        self,
        actor_key: str,
        actor_inputs: Dict,
        filter_rules: Optional[Dict] = None,
        limit: int = 5
    ) -> Dict[str, Any]:
        """
        Test an actor configuration without creating a job search.
        Useful for previewing results before saving.
        """
        actor_config = self.actor_registry.get_actor(actor_key)
        
        # Create a temporary job search object
        temp_search = JobSearch(
            name="Test",
            actor_key=actor_key,
            cron_schedule="0 0 * * *",
            actor_inputs={**actor_inputs, "max_results": limit},
            filter_rules=filter_rules or {},
            clay_webhook_url=""
        )
        
        # Run the actor
        apify_result = await self.apify_service.run_actor(
            actor_config=actor_config,
            job_search=temp_search
        )
        
        raw_jobs = apify_result["items"]
        
        # Filter
        passed_jobs, filtered_jobs = self.filter_service.filter_jobs(
            jobs=raw_jobs,
            actor_config=actor_config,
            job_search=temp_search
        )
        
        # Preview Clay payloads
        clay_previews = []
        for job in passed_jobs[:3]:
            preview = self.clay_service.preview_payload(job, actor_config)
            clay_previews.append(preview)
        
        return {
            "total_found": len(raw_jobs),
            "passed_filter": len(passed_jobs),
            "filtered_out": len(filtered_jobs),
            "sample_jobs": passed_jobs[:5],
            "clay_previews": clay_previews,
            "filter_reasons": [reason for _, reason in filtered_jobs[:10]]
        }
    
    def get_run_history(self, job_search_id: int, limit: int = 10) -> list:
        """Get recent runs for a job search"""
        runs = self.db.query(JobRun).filter(
            JobRun.job_search_id == job_search_id
        ).order_by(JobRun.started_at.desc()).limit(limit).all()
        
        return runs
    
    def get_run_details(self, run_id: int) -> Dict[str, Any]:
        """Get detailed info about a specific run"""
        run = self.db.query(JobRun).filter(JobRun.id == run_id).first()
        
        if not run:
            return None
        
        jobs = self.db.query(ScrapedJob).filter(
            ScrapedJob.job_run_id == run_id
        ).all()
        
        return {
            "run": run,
            "jobs": jobs,
            "passed": [j for j in jobs if not j.filtered_out],
            "filtered": [j for j in jobs if j.filtered_out]
        }
