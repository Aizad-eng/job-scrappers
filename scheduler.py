from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from sqlalchemy.orm import Session
import logging
import asyncio
import pytz

from database import SessionLocal
from models import JobSearch
from scraper_service import JobScraperService

logger = logging.getLogger(__name__)

# Timezone for scheduler
SCHEDULER_TIMEZONE = pytz.timezone('America/New_York')


class JobScheduler:
    """Manage scheduled job executions"""
    
    def __init__(self):
        self.scheduler = AsyncIOScheduler(timezone=SCHEDULER_TIMEZONE)
        self.job_ids = {}  # Map job_search_id to scheduler job_id
    
    def start(self):
        """Start the scheduler"""
        if not self.scheduler.running:
            self.scheduler.start()
            logger.info("Job scheduler started")
    
    def shutdown(self):
        """Shutdown the scheduler"""
        if self.scheduler.running:
            self.scheduler.shutdown()
            logger.info("Job scheduler shutdown")
    
    async def _execute_job_wrapper(self, job_search_id: int):
        """
        Wrapper to execute job with its OWN database session.
        Critical: Each scheduled job needs its own session since
        they run in the background independent of any request.
        """
        logger.info(f"Scheduler triggered job_search_id: {job_search_id}")
        
        # Create a fresh DB session for this job
        db = SessionLocal()
        try:
            service = JobScraperService(db)
            result = await service.execute_job_search(job_search_id)
            logger.info(f"Scheduled job {job_search_id} completed with status: {result.status}")
        except Exception as e:
            logger.error(f"Scheduled job {job_search_id} failed: {e}", exc_info=True)
        finally:
            db.close()
            logger.info(f"Scheduler DB session closed for job_search_id: {job_search_id}")
    
    def add_job(self, job_search: JobSearch):
        """Add or update a scheduled job"""
        # Remove existing job if present
        if job_search.id in self.job_ids:
            self.remove_job(job_search.id)
        
        if not job_search.is_active:
            logger.info(f"Skipping inactive job search: {job_search.name}")
            return
        
        try:
            # Parse cron schedule
            trigger = CronTrigger.from_crontab(job_search.cron_schedule)
            
            # Add job to scheduler
            scheduler_job = self.scheduler.add_job(
                self._execute_job_wrapper,
                trigger=trigger,
                args=[job_search.id],
                id=f"job_search_{job_search.id}",
                name=job_search.name,
                replace_existing=True
            )
            
            self.job_ids[job_search.id] = scheduler_job.id
            
            logger.info(
                f"Scheduled job '{job_search.name}' (ID: {job_search.id}) "
                f"with cron: {job_search.cron_schedule}"
            )
        
        except Exception as e:
            logger.error(f"Failed to schedule job {job_search.id}: {e}")
            raise
    
    def remove_job(self, job_search_id: int):
        """Remove a scheduled job"""
        if job_search_id in self.job_ids:
            try:
                self.scheduler.remove_job(self.job_ids[job_search_id])
                del self.job_ids[job_search_id]
                logger.info(f"Removed scheduled job {job_search_id}")
            except Exception as e:
                logger.error(f"Failed to remove job {job_search_id}: {e}")
    
    def load_all_jobs(self):
        """Load all active jobs from database"""
        db = SessionLocal()
        try:
            job_searches = db.query(JobSearch).filter(JobSearch.is_active == True).all()
            
            logger.info(f"Loading {len(job_searches)} active job searches")
            
            for job_search in job_searches:
                try:
                    self.add_job(job_search)
                except Exception as e:
                    logger.error(f"Failed to load job {job_search.id}: {e}")
        
        finally:
            db.close()
    
    def get_scheduled_jobs(self):
        """Get list of all scheduled jobs"""
        return [
            {
                "id": job.id,
                "name": job.name,
                "next_run_time": job.next_run_time.isoformat() if job.next_run_time else None
            }
            for job in self.scheduler.get_jobs()
        ]


# Global scheduler instance
scheduler = JobScheduler()
