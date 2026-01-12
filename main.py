from fastapi import FastAPI, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session
from typing import List
import logging
import asyncio

from database import get_db, init_db, SessionLocal
from models import JobSearch, JobRun, ScrapedJob
from schemas import (
    JobSearchCreate, JobSearchUpdate, JobSearchResponse,
    JobRunResponse, ScrapedJobResponse, TriggerJobRequest, TriggerJobResponse
)
from scraper_service import JobScraperService
from scheduler import scheduler
from config import settings

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Create FastAPI app
app = FastAPI(
    title="LinkedIn Job Scraper API",
    description="API for scraping and managing LinkedIn job searches",
    version="1.0.0"
)

# Setup templates
templates = Jinja2Templates(directory="templates")


@app.on_event("startup")
async def startup_event():
    """Initialize app on startup"""
    logger.info("Starting application...")
    
    # Initialize database
    init_db()
    
    # Start scheduler
    scheduler.start()
    
    # Load all scheduled jobs
    scheduler.load_all_jobs()
    
    logger.info("Application started successfully")


@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown"""
    logger.info("Shutting down application...")
    scheduler.shutdown()
    logger.info("Application shutdown complete")


# ============================================================================
# WEB UI ROUTES
# ============================================================================

@app.get("/", response_class=HTMLResponse)
async def root(request: Request, db: Session = Depends(get_db)):
    """Dashboard homepage"""
    job_searches = db.query(JobSearch).order_by(JobSearch.created_at.desc()).all()
    
    return templates.TemplateResponse("index.html", {
        "request": request,
        "job_searches": job_searches,
        "scheduled_jobs": scheduler.get_scheduled_jobs()
    })


@app.get("/reports", response_class=HTMLResponse)
async def reports_page(request: Request, db: Session = Depends(get_db)):
    """Reports and analytics page"""
    return templates.TemplateResponse("reports.html", {
        "request": request
    })


# ============================================================================
# JOB SEARCH CRUD API
# ============================================================================

@app.post("/api/job-searches", response_model=JobSearchResponse, status_code=201)
async def create_job_search(
    job_search: JobSearchCreate,
    db: Session = Depends(get_db)
):
    """Create a new job search configuration"""
    db_job_search = JobSearch(**job_search.model_dump())
    db.add(db_job_search)
    db.commit()
    db.refresh(db_job_search)
    
    # Add to scheduler
    if db_job_search.is_active:
        scheduler.add_job(db_job_search)
    
    logger.info(f"Created job search: {db_job_search.name} (ID: {db_job_search.id})")
    
    return db_job_search


@app.get("/api/job-searches", response_model=List[JobSearchResponse])
async def list_job_searches(
    skip: int = 0,
    limit: int = 100,
    active_only: bool = False,
    db: Session = Depends(get_db)
):
    """List all job search configurations"""
    query = db.query(JobSearch)
    
    if active_only:
        query = query.filter(JobSearch.is_active == True)
    
    job_searches = query.offset(skip).limit(limit).all()
    return job_searches


@app.get("/api/job-searches/{job_search_id}", response_model=JobSearchResponse)
async def get_job_search(job_search_id: int, db: Session = Depends(get_db)):
    """Get a specific job search configuration"""
    job_search = db.query(JobSearch).filter(JobSearch.id == job_search_id).first()
    
    if not job_search:
        raise HTTPException(status_code=404, detail="Job search not found")
    
    return job_search


@app.patch("/api/job-searches/{job_search_id}", response_model=JobSearchResponse)
async def update_job_search(
    job_search_id: int,
    job_search_update: JobSearchUpdate,
    db: Session = Depends(get_db)
):
    """Update a job search configuration"""
    db_job_search = db.query(JobSearch).filter(JobSearch.id == job_search_id).first()
    
    if not db_job_search:
        raise HTTPException(status_code=404, detail="Job search not found")
    
    # Update fields
    update_data = job_search_update.model_dump(exclude_unset=True)
    for field, value in update_data.items():
        setattr(db_job_search, field, value)
    
    db.commit()
    db.refresh(db_job_search)
    
    # Update scheduler
    scheduler.add_job(db_job_search)
    
    logger.info(f"Updated job search: {db_job_search.name} (ID: {db_job_search.id})")
    
    return db_job_search


@app.delete("/api/job-searches/{job_search_id}", status_code=204)
async def delete_job_search(job_search_id: int, db: Session = Depends(get_db)):
    """Delete a job search configuration"""
    db_job_search = db.query(JobSearch).filter(JobSearch.id == job_search_id).first()
    
    if not db_job_search:
        raise HTTPException(status_code=404, detail="Job search not found")
    
    # Remove from scheduler
    scheduler.remove_job(job_search_id)
    
    # Delete from database
    db.delete(db_job_search)
    db.commit()
    
    logger.info(f"Deleted job search ID: {job_search_id}")
    
    return None


@app.post("/api/job-searches/{job_search_id}/duplicate", response_model=JobSearchResponse, status_code=201)
async def duplicate_job_search(job_search_id: int, db: Session = Depends(get_db)):
    """Duplicate an existing job search configuration"""
    original = db.query(JobSearch).filter(JobSearch.id == job_search_id).first()
    
    if not original:
        raise HTTPException(status_code=404, detail="Job search not found")
    
    # Create copy with new name
    new_job_search = JobSearch(
        name=f"{original.name} (Copy)",
        keyword=original.keyword,
        platform=original.platform,
        search_url=original.search_url,
        cron_schedule=original.cron_schedule,
        apify_actor_id=original.apify_actor_id,
        apify_token=original.apify_token,
        max_results=original.max_results,
        scrape_company=original.scrape_company,
        use_browser=original.use_browser,
        use_apify_proxy=original.use_apify_proxy,
        company_name_excludes=original.company_name_excludes,
        industries_excludes=original.industries_excludes,
        max_employee_count=original.max_employee_count,
        min_employee_count=original.min_employee_count,
        job_description_filters=original.job_description_filters,
        clay_webhook_url=original.clay_webhook_url,
        batch_size=original.batch_size,
        batch_interval_ms=original.batch_interval_ms,
        is_active=False  # Start as inactive so user can edit first
    )
    
    db.add(new_job_search)
    db.commit()
    db.refresh(new_job_search)
    
    logger.info(f"Duplicated job search '{original.name}' -> '{new_job_search.name}' (ID: {new_job_search.id})")
    
    return new_job_search


# ============================================================================
# JOB EXECUTION API
# ============================================================================

async def run_job_in_background(job_search_id: int):
    """
    Run job in background with its own database session.
    This is critical - the request's DB session closes after response,
    so background tasks need their own session.
    """
    # Create a NEW database session for this background task
    db = SessionLocal()
    try:
        logger.info(f"Background task started for job_search_id: {job_search_id}")
        service = JobScraperService(db)
        result = await service.execute_job_search(job_search_id)
        logger.info(f"Background task completed for job_search_id: {job_search_id}, status: {result.status}")
    except Exception as e:
        logger.error(f"Background job execution failed for job_search_id {job_search_id}: {e}", exc_info=True)
    finally:
        db.close()
        logger.info(f"Background task DB session closed for job_search_id: {job_search_id}")


@app.post("/api/trigger-job", response_model=TriggerJobResponse)
async def trigger_job(request: TriggerJobRequest, db: Session = Depends(get_db)):
    """Manually trigger a job search execution"""
    job_search = db.query(JobSearch).filter(
        JobSearch.id == request.job_search_id
    ).first()
    
    if not job_search:
        raise HTTPException(status_code=404, detail="Job search not found")
    
    # Run in background with its OWN database session
    asyncio.create_task(run_job_in_background(request.job_search_id))
    
    return {
        "message": f"Job search '{job_search.name}' triggered successfully",
        "job_run_id": 0,  # Will be created in background
        "status": "running"
    }


@app.get("/api/job-runs", response_model=List[JobRunResponse])
async def list_job_runs(
    job_search_id: int = None,
    skip: int = 0,
    limit: int = 50,
    db: Session = Depends(get_db)
):
    """List job run history"""
    query = db.query(JobRun).order_by(JobRun.started_at.desc())
    
    if job_search_id:
        query = query.filter(JobRun.job_search_id == job_search_id)
    
    job_runs = query.offset(skip).limit(limit).all()
    return job_runs


@app.get("/api/job-runs/{job_run_id}", response_model=JobRunResponse)
async def get_job_run(job_run_id: int, db: Session = Depends(get_db)):
    """Get details of a specific job run"""
    job_run = db.query(JobRun).filter(JobRun.id == job_run_id).first()
    
    if not job_run:
        raise HTTPException(status_code=404, detail="Job run not found")
    
    return job_run


@app.get("/api/scraped-jobs", response_model=List[ScrapedJobResponse])
async def list_scraped_jobs(
    job_run_id: int = None,
    skip: int = 0,
    limit: int = 100,
    db: Session = Depends(get_db)
):
    """List scraped jobs"""
    query = db.query(ScrapedJob).order_by(ScrapedJob.created_at.desc())
    
    if job_run_id:
        query = query.filter(ScrapedJob.job_run_id == job_run_id)
    
    jobs = query.offset(skip).limit(limit).all()
    return jobs


# ============================================================================
# SCHEDULER API
# ============================================================================

@app.get("/api/scheduler/status")
async def get_scheduler_status():
    """Get scheduler status and list of scheduled jobs"""
    return {
        "running": scheduler.scheduler.running,
        "jobs": scheduler.get_scheduled_jobs()
    }


@app.get("/api/debug/job-runs")
async def debug_job_runs(db: Session = Depends(get_db)):
    """View all job runs for debugging"""
    runs = db.query(JobRun).order_by(JobRun.id.desc()).limit(20).all()
    return [{
        "id": r.id,
        "job_search_id": r.job_search_id,
        "status": r.status,
        "started_at": r.started_at,
        "completed_at": r.completed_at,
        "jobs_found": r.jobs_found,
        "jobs_filtered": r.jobs_filtered,
        "jobs_sent": r.jobs_sent,
        "error_message": r.error_message,
        "apify_run_id": r.apify_run_id
    } for r in runs]


# ============================================================================
# HEALTH CHECK
# ============================================================================

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "scheduler_running": scheduler.scheduler.running
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host=settings.HOST,
        port=settings.PORT,
        reload=settings.DEBUG
    )
