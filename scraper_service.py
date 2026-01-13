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
from typing import Dict, Any, Optional, List, Tuple
from sqlalchemy.orm import Session

from models import JobSearch, JobRun, ScrapedJob, ActorConfig
from actor_registry import ActorRegistry
from apify_service import ApifyService
from filter_service import FilterService
from clay_service import ClayService
from template_engine import extract_fields
from run_logger import start_run_logging, finish_run_logging
# Compatibility layer no longer needed with clean database, update_job_run_safe

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
            started_at=datetime.utcnow(),
            status="running",
            jobs_found=0,
            jobs_filtered=0,
            jobs_sent=0
        )
        self.db.add(job_run)
        self.db.commit()
        self.db.refresh(job_run)
        
        # Start capturing logs for this run
        start_run_logging(job_run.id)
        
        # Update job search status to running
        job_search.last_status = "running"
        job_search.last_run_at = datetime.utcnow()
        
        self.db.commit()
        
        try:
            # Step 1: Run the Apify actor
            logger.info(f"[STEP 1] Starting scrape for '{job_search.name}' using {actor_config.display_name}")
            logger.info(f"[STEP 1] Actor inputs: {job_search.actor_inputs}")
            logger.info(f"[STEP 1] Filter rules: {job_search.filter_rules}")
            
            # Create a progress callback that handles partial data and prevents duplicates
            async def progress_callback(partial_jobs, total_count, is_final):
                return await self._handle_progressive_data(
                    job_run, partial_jobs, actor_config, job_search, is_final
                )
            
            apify_result = await self.apify_service.run_actor(
                actor_config=actor_config,
                job_search=job_search,
                progress_callback=progress_callback
            )
            
            job_run.apify_run_id = apify_result["run_id"]
            job_run.apify_dataset_id = apify_result["dataset_id"]
            
            raw_jobs = apify_result["items"]
            job_run.jobs_found = len(raw_jobs)
            
            logger.info(f"[STEP 1] ✅ Apify actor completed successfully")
            logger.info(f"[STEP 1] Found {len(raw_jobs)} jobs from Apify")
            logger.info(f"[STEP 1] Apify run ID: {job_run.apify_run_id}")
            logger.info(f"[STEP 1] Dataset ID: {job_run.apify_dataset_id}")
            
            if len(raw_jobs) == 0:
                logger.warning(f"[STEP 1] ⚠️  No jobs returned from Apify actor - this might indicate an issue")
            
            # Step 2: Check for jobs already processed progressively
            existing_job_ids = set()
            existing_jobs = self.db.query(ScrapedJob.job_id).filter(
                ScrapedJob.job_run_id == job_run.id,
                ScrapedJob.job_id.isnot(None)
            ).all()
            existing_job_ids = {job.job_id for job in existing_jobs if job.job_id}
            
            # Get current stats from progressive processing
            current_stored_jobs = self.db.query(ScrapedJob).filter(
                ScrapedJob.job_run_id == job_run.id
            ).count()
            
            logger.info(f"[STEP 2] Total jobs from Apify: {len(raw_jobs)}")
            logger.info(f"[STEP 2] Jobs already processed progressively: {current_stored_jobs}")
            
            if current_stored_jobs > 0:
                logger.info(f"[STEP 2] ✅ Using progressive data - final verification only")
                
                # Just update final counts
                passed_count = self.db.query(ScrapedJob).filter(
                    ScrapedJob.job_run_id == job_run.id,
                    ScrapedJob.filtered_out == False
                ).count()
                
                filtered_count = self.db.query(ScrapedJob).filter(
                    ScrapedJob.job_run_id == job_run.id,
                    ScrapedJob.filtered_out == True
                ).count()
                
                job_run.jobs_found = len(raw_jobs)
                job_run.jobs_filtered = filtered_count
                
                logger.info(f"[STEP 2] Final counts - Passed: {passed_count}, Filtered: {filtered_count}")
                
                # Check if any new jobs need to be processed (edge case)
                if len(raw_jobs) > current_stored_jobs:
                    logger.info(f"[STEP 2] Processing {len(raw_jobs) - current_stored_jobs} additional jobs")
                    # Process only the new jobs using the progressive handler
                    await self._handle_progressive_data(
                        job_run, raw_jobs[current_stored_jobs:], actor_config, job_search, True
                    )
            else:
                logger.info(f"[STEP 2] No progressive data found - processing all jobs normally")
                
                # Standard processing for non-progressive runs
                passed_jobs, filtered_jobs = self.filter_service.filter_jobs(
                    jobs=raw_jobs,
                    actor_config=actor_config,
                    job_search=job_search
                )
                
                job_run.jobs_filtered = len(filtered_jobs)
                
                logger.info(f"[STEP 2] ✅ Filtering completed")
                logger.info(f"[STEP 2] Jobs passed filter: {len(passed_jobs)}")
                logger.info(f"[STEP 2] Jobs filtered out: {len(filtered_jobs)}")
                
                if len(filtered_jobs) > 0:
                    filter_reasons = [reason for _, reason in filtered_jobs[:5]]
                    logger.info(f"[STEP 2] Sample filter reasons: {filter_reasons}")
                
                # Step 3: Store scraped jobs
                logger.info(f"[STEP 3] Storing {len(passed_jobs + filtered_jobs)} jobs in database")
                
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
                logger.info(f"[STEP 3] ✅ Stored all jobs in database")
            
            # Step 4: Handle final Clay sending
            logger.info(f"[STEP 4] Checking Clay webhook requirements")
            
            # Get jobs that still need to be sent to Clay
            unsent_jobs = self.db.query(ScrapedJob).filter(
                ScrapedJob.job_run_id == job_run.id,
                ScrapedJob.filtered_out == False,
                ScrapedJob.sent_to_clay == False
            ).all()
            
            logger.info(f"[STEP 4] Jobs remaining to send: {len(unsent_jobs)}")
            logger.info(f"[STEP 4] Clay webhook URL: {job_search.clay_webhook_url[:50]}..." if job_search.clay_webhook_url else "[STEP 4] No Clay webhook URL configured")
            
            if unsent_jobs and job_search.clay_webhook_url:
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
                
                # Convert unsent jobs to raw data for Clay
                unsent_job_data = [job.raw_data for job in unsent_jobs]
                
                logger.info(f"[STEP 4] Sending {len(unsent_job_data)} remaining jobs to Clay webhook")
                logger.info(f"[STEP 4] Extra fields: {extra_fields}")
                
                clay_result = await self.clay_service.send_jobs(
                    jobs=unsent_job_data,
                    actor_config=actor_config,
                    job_search=job_search,
                    extra_fields=extra_fields
                )
                
                final_sent = clay_result["sent"]
                
                # Update sent status for the newly sent jobs
                if final_sent > 0:
                    for i, job in enumerate(unsent_jobs[:final_sent]):
                        job.sent_to_clay = True
                    
                    self.db.commit()
                
                # Update total sent count
                total_sent = self.db.query(ScrapedJob).filter(
                    ScrapedJob.job_run_id == job_run.id,
                    ScrapedJob.sent_to_clay == True
                ).count()
                
                job_run.jobs_sent = total_sent
                
                logger.info(f"[STEP 4] ✅ Final Clay sending: {final_sent} jobs sent")
                logger.info(f"[STEP 4] ✅ Total jobs sent to Clay: {total_sent}")
                
                if clay_result.get('errors'):
                    logger.warning(f"[STEP 4] ⚠️  Clay errors: {clay_result['errors']}")
            else:
                if not unsent_jobs:
                    total_sent = self.db.query(ScrapedJob).filter(
                        ScrapedJob.job_run_id == job_run.id,
                        ScrapedJob.sent_to_clay == True
                    ).count()
                    job_run.jobs_sent = total_sent
                    logger.info(f"[STEP 4] ✅ All jobs already sent to Clay progressively: {total_sent}")
                else:
                    logger.warning(f"[STEP 4] ⚠️  Skipping Clay - no webhook URL configured")
            
            # Mark as complete
            execution_logs = finish_run_logging(job_run.id)
            update_job_run_safe(self.db, job_run.id,
                status="completed",
                completed_at=datetime.utcnow(),
                execution_logs=execution_logs
            )
            
            # Update job search last run info
            job_search.last_run_at = datetime.utcnow()
            job_search.last_status = "completed"
            
            logger.info(f"✅ [SUCCESS] Job search {job_search_id} completed successfully")
            
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
            logger.error(f"❌ [ERROR] Job search {job_search_id} failed: {str(e)}")
            logger.exception(f"❌ [ERROR] Full exception details:")
            
            # Try to determine which step failed
            step_info = "Unknown step"
            if not hasattr(locals().get('apify_result', {}), 'get'):
                step_info = "STEP 1 - Apify actor execution"
            elif 'raw_jobs' not in locals():
                step_info = "STEP 1 - Processing Apify results"
            elif 'passed_jobs' not in locals():
                step_info = "STEP 2 - Job filtering"
            elif 'clay_result' not in locals():
                step_info = "STEP 4 - Clay webhook"
            else:
                step_info = "STEP 4 - Final processing"
            
            logger.error(f"❌ [ERROR] Failed during: {step_info}")
            
            execution_logs = finish_run_logging(job_run.id)
            update_job_run_safe(self.db, job_run.id,
                status="failed", 
                error_message=f"{step_info}: {str(e)}",
                completed_at=datetime.utcnow(),
                execution_logs=execution_logs
            )
            
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
    
    async def _handle_progressive_data(
        self,
        job_run: JobRun,
        partial_jobs: list[dict],
        actor_config: ActorConfig,
        job_search: JobSearch,
        is_final: bool
    ):
        """
        Handle progressive data during long-running scrapes.
        Prevents duplicates by tracking which jobs have been processed.
        """
        if not partial_jobs:
            return
        
        logger.info(f"[PROGRESSIVE] Processing {len(partial_jobs)} partial jobs (final: {is_final})")
        
        # Filter the partial jobs
        passed_jobs, filtered_jobs = self.filter_service.filter_jobs(
            jobs=partial_jobs,
            actor_config=actor_config,
            job_search=job_search
        )
        
        # Get existing job IDs to prevent duplicates
        existing_job_ids = set()
        existing_jobs = self.db.query(ScrapedJob.job_id).filter(
            ScrapedJob.job_run_id == job_run.id,
            ScrapedJob.job_id.isnot(None)
        ).all()
        existing_job_ids = {job.job_id for job in existing_jobs if job.job_id}
        
        # Process passed jobs - store new ones and collect for Clay sending
        new_jobs_for_clay = []
        new_jobs_count = 0
        
        for job_data in passed_jobs:
            extracted = extract_fields(job_data, actor_config.output_mapping or {})
            job_id = extracted.get("job_id")
            
            # Skip if we've already processed this job
            if job_id and job_id in existing_job_ids:
                continue
            
            # Store in database
            scraped_job = ScrapedJob(
                job_run_id=job_run.id,
                job_id=job_id,
                raw_data=job_data,
                extracted_data=extracted,
                sent_to_clay=False,  # Will be updated after Clay sending
                filtered_out=False
            )
            self.db.add(scraped_job)
            
            # Add to list for Clay sending
            new_jobs_for_clay.append(job_data)
            new_jobs_count += 1
            
            # Track this job ID to prevent future duplicates
            if job_id:
                existing_job_ids.add(job_id)
        
        # Process filtered jobs
        for job_data, reason in filtered_jobs:
            extracted = extract_fields(job_data, actor_config.output_mapping or {})
            job_id = extracted.get("job_id")
            
            # Skip if we've already processed this job
            if job_id and job_id in existing_job_ids:
                continue
            
            scraped_job = ScrapedJob(
                job_run_id=job_run.id,
                job_id=job_id,
                raw_data=job_data,
                extracted_data=extracted,
                sent_to_clay=False,
                filtered_out=True,
                filter_reason=reason
            )
            self.db.add(scraped_job)
            
            if job_id:
                existing_job_ids.add(job_id)
        
        # Commit new jobs to database
        self.db.commit()
        
        if new_jobs_count > 0:
            logger.info(f"[PROGRESSIVE] Stored {new_jobs_count} new jobs ({len(passed_jobs) - new_jobs_count} duplicates skipped)")
        
        # Send new jobs to Clay (only if webhook is configured and we have new jobs)
        if new_jobs_for_clay and job_search.clay_webhook_url:
            try:
                # Build extra fields
                extra_fields = {
                    "platform": actor_config.actor_key,
                    "search_name": job_search.name,
                    "progressive_batch": True,
                    "is_final": is_final
                }
                
                logger.info(f"[PROGRESSIVE] Sending {len(new_jobs_for_clay)} new jobs to Clay")
                
                clay_result = await self.clay_service.send_jobs(
                    jobs=new_jobs_for_clay,
                    actor_config=actor_config,
                    job_search=job_search,
                    extra_fields=extra_fields
                )
                
                jobs_sent = clay_result["sent"]
                
                if jobs_sent > 0:
                    # Mark the newly sent jobs as sent_to_clay
                    # Get the ScrapedJob records we just added (last new_jobs_count records for this run)
                    recent_jobs = self.db.query(ScrapedJob).filter(
                        ScrapedJob.job_run_id == job_run.id,
                        ScrapedJob.filtered_out == False,
                        ScrapedJob.sent_to_clay == False
                    ).order_by(ScrapedJob.id.desc()).limit(jobs_sent).all()
                    
                    for job in recent_jobs:
                        job.sent_to_clay = True
                    
                    self.db.commit()
                    
                    # Update job run progress stats
                    job_run.jobs_sent = (job_run.jobs_sent or 0) + jobs_sent
                    self.db.commit()
                    
                    logger.info(f"[PROGRESSIVE] ✅ Sent {jobs_sent} new jobs to Clay (total sent: {job_run.jobs_sent})")
                
            except Exception as e:
                logger.error(f"[PROGRESSIVE] ❌ Failed to send jobs to Clay: {e}")
        
        elif new_jobs_for_clay and not job_search.clay_webhook_url:
            logger.info(f"[PROGRESSIVE] ⏭️ {len(new_jobs_for_clay)} new jobs ready (no Clay webhook configured)")
            
        elif not new_jobs_for_clay:
            logger.info(f"[PROGRESSIVE] ℹ️ No new jobs to process (all were duplicates or filtered)")
            
        return new_jobs_count
    
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
