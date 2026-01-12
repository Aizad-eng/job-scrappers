"""
Scheduler Service - Runs job searches automatically based on cron schedules.

Uses APScheduler with EST timezone.
"""

import logging
import asyncio
from datetime import datetime
from typing import Optional

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.jobstores.memory import MemoryJobStore
from pytz import timezone

from sqlalchemy.orm import Session
from database import SessionLocal
from models import JobSearch

logger = logging.getLogger(__name__)

# EST Timezone
EST = timezone('America/New_York')


class SchedulerService:
    """Manages scheduled job searches using APScheduler"""
    
    _instance: Optional['SchedulerService'] = None
    
    def __init__(self):
        self.scheduler = AsyncIOScheduler(
            jobstores={'default': MemoryJobStore()},
            timezone=EST
        )
        self._started = False
    
    @classmethod
    def get_instance(cls) -> 'SchedulerService':
        """Get singleton instance"""
        if cls._instance is None:
            cls._instance = SchedulerService()
        return cls._instance
    
    def start(self):
        """Start the scheduler"""
        if not self._started:
            self.scheduler.start()
            self._started = True
            logger.info("Scheduler started (EST timezone)")
    
    def shutdown(self):
        """Shutdown the scheduler"""
        if self._started:
            self.scheduler.shutdown()
            self._started = False
            logger.info("Scheduler stopped")
    
    def load_all_jobs(self):
        """Load all active job searches from database and schedule them"""
        db = SessionLocal()
        try:
            active_searches = db.query(JobSearch).filter(
                JobSearch.is_active == True
            ).all()
            
            for search in active_searches:
                self.add_job(search.id, search.cron_schedule, search.name)
            
            logger.info(f"Loaded {len(active_searches)} scheduled jobs")
        finally:
            db.close()
    
    def add_job(self, job_search_id: int, cron_schedule: str, name: str = ""):
        """Add a job search to the scheduler"""
        job_id = f"job_search_{job_search_id}"
        
        # Remove existing job if any
        self.remove_job(job_search_id)
        
        try:
            # Parse cron expression (minute hour day_of_month month day_of_week)
            parts = cron_schedule.split()
            if len(parts) != 5:
                logger.error(f"Invalid cron expression: {cron_schedule}")
                return
            
            minute, hour, day, month, day_of_week = parts
            
            trigger = CronTrigger(
                minute=minute,
                hour=hour,
                day=day,
                month=month,
                day_of_week=day_of_week,
                timezone=EST
            )
            
            self.scheduler.add_job(
                func=run_scheduled_search,
                trigger=trigger,
                id=job_id,
                args=[job_search_id],
                name=f"Search: {name}",
                replace_existing=True,
                misfire_grace_time=3600  # Allow 1 hour grace period for misfires
            )
            
            next_run = self.scheduler.get_job(job_id).next_run_time
            logger.info(f"Scheduled job '{name}' (ID: {job_search_id}) - Next run: {next_run}")
            
        except Exception as e:
            logger.error(f"Failed to schedule job {job_search_id}: {e}")
    
    def remove_job(self, job_search_id: int):
        """Remove a job search from the scheduler"""
        job_id = f"job_search_{job_search_id}"
        
        try:
            if self.scheduler.get_job(job_id):
                self.scheduler.remove_job(job_id)
                logger.info(f"Removed scheduled job: {job_search_id}")
        except Exception as e:
            logger.debug(f"Job {job_search_id} not found in scheduler: {e}")
    
    def update_job(self, job_search_id: int, cron_schedule: str, name: str = "", is_active: bool = True):
        """Update a scheduled job (reschedule or remove)"""
        if is_active:
            self.add_job(job_search_id, cron_schedule, name)
        else:
            self.remove_job(job_search_id)
    
    def get_next_run(self, job_search_id: int) -> Optional[datetime]:
        """Get next run time for a job"""
        job_id = f"job_search_{job_search_id}"
        job = self.scheduler.get_job(job_id)
        
        if job:
            return job.next_run_time
        return None
    
    def get_all_scheduled_jobs(self) -> list:
        """Get info about all scheduled jobs"""
        jobs = []
        for job in self.scheduler.get_jobs():
            jobs.append({
                'id': job.id,
                'name': job.name,
                'next_run': job.next_run_time.isoformat() if job.next_run_time else None,
                'trigger': str(job.trigger)
            })
        return jobs


async def run_scheduled_search(job_search_id: int):
    """Execute a scheduled job search"""
    import os
    from scraper_service import ScraperService
    
    logger.info(f"[SCHEDULER] Starting scheduled run for job search {job_search_id}")
    
    db = SessionLocal()
    try:
        # Get Apify token
        search = db.query(JobSearch).filter(JobSearch.id == job_search_id).first()
        
        if not search:
            logger.error(f"Job search {job_search_id} not found")
            return
        
        if not search.is_active:
            logger.info(f"Job search {job_search_id} is inactive, skipping")
            return
        
        apify_token = search.apify_token or os.getenv("APIFY_API_TOKEN")
        
        if not apify_token:
            logger.error(f"No Apify token for job search {job_search_id}")
            return
        
        # Run the search
        service = ScraperService(db, apify_token)
        result = await service.execute_search(job_search_id)
        
        if result.get('success'):
            logger.info(f"[SCHEDULER] Completed job search {job_search_id}: "
                       f"Found={result.get('jobs_found')}, "
                       f"Filtered={result.get('jobs_filtered')}, "
                       f"Sent={result.get('jobs_sent')}")
        else:
            logger.error(f"[SCHEDULER] Failed job search {job_search_id}: {result.get('error')}")
    
    except Exception as e:
        logger.exception(f"[SCHEDULER] Error running job search {job_search_id}: {e}")
    finally:
        db.close()


# Global scheduler instance
scheduler = SchedulerService.get_instance()
