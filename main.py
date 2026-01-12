"""
Job Scraper API - Config-driven, scalable job scraping platform.

Adding a new scraper = Adding config to actor_registry.py. That's it.
"""

import os
import logging
import httpx
from typing import List, Optional
from datetime import datetime

from fastapi import FastAPI, Depends, HTTPException, Request, BackgroundTasks
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse, JSONResponse
from sqlalchemy.orm import Session
from pydantic import BaseModel, Field

from database import init_db, get_db
from models import JobSearch, JobRun, ScrapedJob, ActorConfig
from actor_registry import ActorRegistry, SEED_ACTORS
from scraper_service import ScraperService
from scheduler_service import scheduler, SchedulerService
from apify_service import ApifyService
from clay_service import ClayService
from schemas import (
    JobSearchCreate, JobSearchUpdate, JobSearchResponse, JobSearchListResponse,
    JobRunResponse, JobRunDetailResponse,
    ExecuteResponse, TestActorRequest, TestActorResponse,
    ActorConfigResponse, ActorListResponse,
    SuccessResponse, ErrorResponse
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize FastAPI
app = FastAPI(
    title="Job Scraper API",
    description="Config-driven job scraping platform. Add new scrapers via config, not code.",
    version="2.0.0"
)

# Templates
templates = Jinja2Templates(directory="templates")


# =============================================================================
# STARTUP & SHUTDOWN
# =============================================================================

@app.on_event("startup")
async def startup_event():
    """Initialize database, seed actor configs, and start scheduler"""
    logger.info("Starting Job Scraper API...")
    
    # Initialize database
    init_db()
    
    # Seed actor configurations
    from database import SessionLocal
    db = SessionLocal()
    try:
        registry = ActorRegistry(db)
        registry.seed_actors()
        logger.info(f"Seeded {len(SEED_ACTORS)} actor configurations")
    finally:
        db.close()
    
    # Start the scheduler
    scheduler.start()
    
    # Load all active job searches into scheduler
    scheduler.load_all_jobs()
    
    logger.info("Job Scraper API ready!")


@app.on_event("shutdown")
async def shutdown_event():
    """Shutdown scheduler"""
    logger.info("Shutting down Job Scraper API...")
    scheduler.shutdown()
    logger.info("Scheduler stopped")


# =============================================================================
# UI ROUTES
# =============================================================================

@app.get("/", response_class=HTMLResponse)
async def index(request: Request, db: Session = Depends(get_db)):
    """Main dashboard"""
    registry = ActorRegistry(db)
    actors = registry.list_actors()
    
    searches = db.query(JobSearch).order_by(JobSearch.created_at.desc()).all()
    
    # Enrich with actor display names and next run times
    actor_map = {a.actor_key: a.display_name for a in actors}
    for search in searches:
        search.actor_display_name = actor_map.get(search.actor_key, search.actor_key)
        # Get actual next run time from scheduler
        if search.is_active:
            next_run = scheduler.get_next_run(search.id)
            search.next_run_time = next_run
        else:
            search.next_run_time = None
    
    return templates.TemplateResponse("index.html", {
        "request": request,
        "actors": actors,
        "searches": searches
    })


# =============================================================================
# ACTOR CONFIG ENDPOINTS
# =============================================================================

@app.get("/api/actors", response_model=ActorListResponse)
async def list_actors(db: Session = Depends(get_db)):
    """List all available actors"""
    registry = ActorRegistry(db)
    actors = registry.list_actors()
    
    return ActorListResponse(
        actors=[ActorConfigResponse.model_validate(a) for a in actors]
    )


@app.get("/api/actors/{actor_key}", response_model=ActorConfigResponse)
async def get_actor(actor_key: str, db: Session = Depends(get_db)):
    """Get actor configuration"""
    registry = ActorRegistry(db)
    
    try:
        actor = registry.get_actor(actor_key)
        return ActorConfigResponse.model_validate(actor)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@app.get("/api/actors/{actor_key}/input-schema")
async def get_actor_input_schema(actor_key: str, db: Session = Depends(get_db)):
    """Get the input schema for an actor (for dynamic form generation)"""
    registry = ActorRegistry(db)
    
    try:
        actor = registry.get_actor(actor_key)
        return {
            "actor_key": actor.actor_key,
            "display_name": actor.display_name,
            "input_schema": actor.input_schema,
            "filter_config": actor.filter_config
        }
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


# =============================================================================
# JOB SEARCH ENDPOINTS
# =============================================================================

@app.get("/api/searches", response_model=JobSearchListResponse)
async def list_searches(
    skip: int = 0,
    limit: int = 100,
    active_only: bool = False,
    db: Session = Depends(get_db)
):
    """List all job searches"""
    query = db.query(JobSearch)
    
    if active_only:
        query = query.filter(JobSearch.is_active == True)
    
    total = query.count()
    searches = query.order_by(JobSearch.created_at.desc()).offset(skip).limit(limit).all()
    
    # Get actor display names
    registry = ActorRegistry(db)
    actors = {a.actor_key: a.display_name for a in registry.list_actors()}
    
    response_searches = []
    for search in searches:
        response = JobSearchResponse.model_validate(search)
        response.actor_display_name = actors.get(search.actor_key, search.actor_key)
        response_searches.append(response)
    
    return JobSearchListResponse(searches=response_searches, total=total)


@app.get("/api/searches/{search_id}", response_model=JobSearchResponse)
async def get_search(search_id: int, db: Session = Depends(get_db)):
    """Get a specific job search"""
    search = db.query(JobSearch).filter(JobSearch.id == search_id).first()
    
    if not search:
        raise HTTPException(status_code=404, detail="Job search not found")
    
    # Get actor display name
    registry = ActorRegistry(db)
    try:
        actor = registry.get_actor(search.actor_key)
        response = JobSearchResponse.model_validate(search)
        response.actor_display_name = actor.display_name
        return response
    except ValueError:
        return JobSearchResponse.model_validate(search)


@app.post("/api/searches", response_model=JobSearchResponse)
async def create_search(data: JobSearchCreate, db: Session = Depends(get_db)):
    """Create a new job search"""
    # Validate actor exists
    registry = ActorRegistry(db)
    try:
        actor = registry.get_actor(data.actor_key)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    
    # Create the search
    search = JobSearch(
        name=data.name,
        actor_key=data.actor_key,
        cron_schedule=data.cron_schedule,
        apify_token=data.apify_token,
        actor_inputs=data.actor_inputs,
        filter_rules=data.filter_rules,
        clay_webhook_url=data.clay_webhook_url,
        batch_size=data.batch_size,
        batch_interval_ms=data.batch_interval_ms,
        is_active=data.is_active
    )
    
    db.add(search)
    db.commit()
    db.refresh(search)
    
    # Add to scheduler if active
    if search.is_active:
        scheduler.add_job(search.id, search.cron_schedule, search.name)
    
    response = JobSearchResponse.model_validate(search)
    response.actor_display_name = actor.display_name
    
    return response


@app.put("/api/searches/{search_id}", response_model=JobSearchResponse)
async def update_search(search_id: int, data: JobSearchUpdate, db: Session = Depends(get_db)):
    """Update a job search"""
    search = db.query(JobSearch).filter(JobSearch.id == search_id).first()
    
    if not search:
        raise HTTPException(status_code=404, detail="Job search not found")
    
    # Update fields
    update_data = data.model_dump(exclude_unset=True)
    for field, value in update_data.items():
        setattr(search, field, value)
    
    search.updated_at = datetime.utcnow()
    db.commit()
    db.refresh(search)
    
    # Update scheduler
    scheduler.update_job(search.id, search.cron_schedule, search.name, search.is_active)
    
    return JobSearchResponse.model_validate(search)


@app.delete("/api/searches/{search_id}", response_model=SuccessResponse)
async def delete_search(search_id: int, db: Session = Depends(get_db)):
    """Delete a job search"""
    search = db.query(JobSearch).filter(JobSearch.id == search_id).first()
    
    if not search:
        raise HTTPException(status_code=404, detail="Job search not found")
    
    name = search.name
    
    # Remove from scheduler first
    scheduler.remove_job(search_id)
    
    db.delete(search)
    db.commit()
    
    return SuccessResponse(message=f"Deleted job search '{name}'")


@app.post("/api/searches/{search_id}/duplicate", response_model=JobSearchResponse)
async def duplicate_search(search_id: int, db: Session = Depends(get_db)):
    """Duplicate a job search"""
    original = db.query(JobSearch).filter(JobSearch.id == search_id).first()
    
    if not original:
        raise HTTPException(status_code=404, detail="Job search not found")
    
    # Create copy (starts inactive, so no scheduler needed)
    new_search = JobSearch(
        name=f"{original.name} (Copy)",
        actor_key=original.actor_key,
        cron_schedule=original.cron_schedule,
        apify_token=original.apify_token,
        actor_inputs=original.actor_inputs.copy() if original.actor_inputs else {},
        filter_rules=original.filter_rules.copy() if original.filter_rules else {},
        clay_webhook_url=original.clay_webhook_url,
        batch_size=original.batch_size,
        batch_interval_ms=original.batch_interval_ms,
        is_active=False  # Start inactive - user must activate to schedule
    )
    
    db.add(new_search)
    db.commit()
    db.refresh(new_search)
    
    # Note: Not adding to scheduler since is_active=False
    
    return JobSearchResponse.model_validate(new_search)


# =============================================================================
# EXECUTION ENDPOINTS
# =============================================================================

@app.post("/api/searches/{search_id}/execute", response_model=ExecuteResponse)
async def execute_search(
    search_id: int,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """Execute a job search immediately"""
    search = db.query(JobSearch).filter(JobSearch.id == search_id).first()
    
    if not search:
        raise HTTPException(status_code=404, detail="Job search not found")
    
    # Get Apify token
    apify_token = search.apify_token or os.getenv("APIFY_API_TOKEN")
    
    if not apify_token:
        raise HTTPException(status_code=400, detail="No Apify token configured")
    
    # Execute synchronously for now (could be background task for long-running)
    from database import SessionLocal
    
    async def run_search():
        db_session = SessionLocal()
        try:
            service = ScraperService(db_session, apify_token)
            result = await service.execute_search(search_id)
            return result
        finally:
            db_session.close()
    
    import asyncio
    result = await run_search()
    
    return ExecuteResponse(**result)


@app.post("/api/test-actor", response_model=TestActorResponse)
async def test_actor(data: TestActorRequest, db: Session = Depends(get_db)):
    """Test an actor configuration without saving"""
    apify_token = os.getenv("APIFY_API_TOKEN")
    
    if not apify_token:
        raise HTTPException(status_code=400, detail="No Apify token configured")
    
    service = ScraperService(db, apify_token)
    
    try:
        result = await service.test_actor(
            actor_key=data.actor_key,
            actor_inputs=data.actor_inputs,
            filter_rules=data.filter_rules,
            limit=data.limit
        )
        return TestActorResponse(**result)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# RUN HISTORY ENDPOINTS
# =============================================================================

@app.get("/api/searches/{search_id}/runs", response_model=list[JobRunResponse])
async def get_search_runs(
    search_id: int,
    limit: int = 10,
    db: Session = Depends(get_db)
):
    """Get run history for a job search"""
    try:
        runs = db.query(JobRun).filter(
            JobRun.job_search_id == search_id
        ).order_by(JobRun.started_at.desc()).limit(limit).all()
        
        return [JobRunResponse.model_validate(r) for r in runs]
    except Exception as e:
        if "execution_logs does not exist" in str(e) or "InFailedSqlTransaction" in str(e):
            # Rollback the failed transaction and create a new one
            db.rollback()
            db.close()
            
            # Create a fresh database session for the fallback query
            from database import SessionLocal
            from sqlalchemy import text
            
            fresh_db = SessionLocal()
            try:
                result = fresh_db.execute(text("""
                    SELECT id, job_search_id, 
                           COALESCE(started_at, NOW()) as started_at, 
                           completed_at, 
                           COALESCE(status, 'unknown') as status,
                           COALESCE(jobs_found, 0) as jobs_found, 
                           COALESCE(jobs_filtered, 0) as jobs_filtered, 
                           COALESCE(jobs_sent, 0) as jobs_sent, 
                           error_message,
                           apify_run_id, 
                           CASE WHEN EXISTS (
                               SELECT 1 FROM information_schema.columns 
                               WHERE table_name = 'job_runs' AND column_name = 'apify_dataset_id'
                           ) THEN apify_dataset_id ELSE NULL END as apify_dataset_id,
                           NULL as execution_logs
                    FROM job_runs 
                    WHERE job_search_id = :search_id 
                    ORDER BY COALESCE(started_at, NOW()) DESC 
                    LIMIT :limit
                """), {"search_id": search_id, "limit": limit})
                
                runs = []
                for row in result:
                    run_dict = {
                        "id": row[0],
                        "job_search_id": row[1], 
                        "started_at": row[2],
                        "completed_at": row[3],
                        "status": row[4] or "unknown",
                        "jobs_found": row[5] or 0,
                        "jobs_filtered": row[6] or 0, 
                        "jobs_sent": row[7] or 0,
                        "error_message": row[8],
                        "execution_logs": None,  # Will be available after migration
                        "apify_run_id": row[9]
                    }
                    runs.append(JobRunResponse.model_validate(run_dict))
                
                return runs
            finally:
                fresh_db.close()
        else:
            raise


@app.get("/api/runs/{run_id}", response_model=JobRunDetailResponse)
async def get_run_details(run_id: int, db: Session = Depends(get_db)):
    """Get detailed info about a specific run"""
    run = db.query(JobRun).filter(JobRun.id == run_id).first()
    
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")
    
    jobs = db.query(ScrapedJob).filter(ScrapedJob.job_run_id == run_id).all()
    
    passed = [j for j in jobs if not j.filtered_out]
    filtered = [j for j in jobs if j.filtered_out]
    
    return JobRunDetailResponse(
        run=JobRunResponse.model_validate(run),
        passed_count=len(passed),
        filtered_count=len(filtered),
        sample_jobs=[j.extracted_data for j in passed[:10]],
        filter_reasons=[j.filter_reason for j in filtered[:20] if j.filter_reason]
    )


@app.post("/api/runs/{run_id}/fetch-dataset")
async def fetch_and_process_dataset(
    run_id: int,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """
    Fallback endpoint: Manually fetch data from Apify dataset and process it.
    Useful when a run times out but dataset contains partial data.
    """
    run = db.query(JobRun).filter(JobRun.id == run_id).first()
    
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")
    
    if not run.apify_dataset_id:
        raise HTTPException(status_code=400, detail="No dataset ID associated with this run")
    
    job_search = db.query(JobSearch).filter(JobSearch.id == run.job_search_id).first()
    if not job_search:
        raise HTTPException(status_code=404, detail="Job search not found")
    
    # Get Apify token
    apify_token = job_search.apify_token or os.getenv("APIFY_API_TOKEN")
    
    if not apify_token:
        raise HTTPException(status_code=400, detail="No Apify token configured")
    
    # Process dataset in background
    background_tasks.add_task(
        process_dataset_fallback,
        run_id,
        run.apify_dataset_id,
        apify_token,
        db
    )
    
    return {
        "success": True,
        "message": f"Started processing dataset {run.apify_dataset_id} for run {run_id}",
        "dataset_id": run.apify_dataset_id,
        "dataset_url": f"https://console.apify.com/storage/datasets/{run.apify_dataset_id}"
    }


@app.post("/api/runs/{run_id}/push-to-clay")
async def push_run_to_clay(
    run_id: int,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """
    Manually push run data to Clay webhook in batches.
    Useful when Clay sending fails during normal execution.
    """
    run = db.query(JobRun).filter(JobRun.id == run_id).first()
    
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")
    
    job_search = db.query(JobSearch).filter(JobSearch.id == run.job_search_id).first()
    if not job_search:
        raise HTTPException(status_code=404, detail="Job search not found")
    
    if not job_search.clay_webhook_url:
        raise HTTPException(status_code=400, detail="No Clay webhook URL configured for this job search")
    
    # Count jobs to send
    jobs_to_send = db.query(ScrapedJob).filter(
        ScrapedJob.job_run_id == run_id,
        ScrapedJob.filtered_out == False
    ).count()
    
    if jobs_to_send == 0:
        return {
            "success": False,
            "message": "No jobs available to send (all filtered out or no jobs found)"
        }
    
    # Start background task to push to Clay
    background_tasks.add_task(
        push_to_clay_batch_task,
        run_id,
        job_search.clay_webhook_url,
        job_search.batch_size or 8,
        job_search.batch_interval_ms or 2000,
        db
    )
    
    return {
        "success": True,
        "message": f"Started pushing {jobs_to_send} jobs to Clay webhook",
        "jobs_to_send": jobs_to_send,
        "batch_size": job_search.batch_size or 8,
        "batch_interval": f"{(job_search.batch_interval_ms or 2000)/1000}s"
    }


# =============================================================================
# MANUAL PUSH ENDPOINT
# =============================================================================

class ManualPushRequest(BaseModel):
    dataset_id: str = Field(..., min_length=1)
    actor_key: str = Field(..., min_length=1) 
    clay_webhook_url: str = Field(..., min_length=1)
    batch_size: int = Field(default=8, ge=1, le=50)
    batch_interval_ms: int = Field(default=2000, ge=100, le=30000)

@app.post("/api/manual-push")
async def manual_push_to_clay(
    data: ManualPushRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """Manually push data from any Apify dataset to Clay webhook"""
    try:
        # Get actor config
        actor_registry = ActorRegistry(db)
        actor_config = actor_registry.get_actor(data.actor_key)
        
        if not actor_config:
            return {"success": False, "error": f"Actor '{data.actor_key}' not found"}
        
        # Get Apify token
        apify_token = os.getenv("APIFY_API_TOKEN")
        if not apify_token:
            return {"success": False, "error": "No Apify token configured"}
        
        # First, fetch the dataset to validate it exists and get job count
        logger.info(f"🔍 Validating dataset {data.dataset_id} for manual push")
        
        apify_service = ApifyService(apify_token)
        raw_jobs = await apify_service.get_dataset_items(data.dataset_id)
        
        if not raw_jobs:
            return {
                "success": False, 
                "error": f"No data found in dataset {data.dataset_id}. Dataset may be empty or doesn't exist."
            }
        
        jobs_count = len(raw_jobs)
        logger.info(f"📊 Found {jobs_count} jobs in dataset {data.dataset_id}")
        
        # Create temporary job search object for Clay service
        class TempJobSearch:
            def __init__(self):
                self.clay_webhook_url = data.clay_webhook_url
                self.batch_size = data.batch_size 
                self.batch_interval_ms = data.batch_interval_ms
        
        temp_job_search = TempJobSearch()
        
        # Add background task to process the push (pass the already-fetched jobs)
        background_tasks.add_task(
            process_manual_push,
            data.dataset_id,
            actor_config,
            temp_job_search,
            raw_jobs  # Pass the jobs we already fetched
        )
        
        estimated_time_minutes = (jobs_count * data.batch_interval_ms) / (data.batch_size * 60000)
        
        return {
            "success": True,
            "message": f"Manual push started for {jobs_count} jobs",
            "dataset_id": data.dataset_id,
            "actor_key": data.actor_key,
            "jobs_found": jobs_count,
            "batch_size": data.batch_size,
            "batch_interval_ms": data.batch_interval_ms,
            "estimated_time_minutes": round(estimated_time_minutes, 1)
        }
        
    except Exception as e:
        logger.error(f"Manual push setup failed: {e}")
        return {"success": False, "error": str(e)}

async def process_manual_push(dataset_id: str, actor_config, job_search, raw_jobs=None):
    """Background task to process manual push"""
    try:
        logger.info(f"🚀 Starting manual push - Dataset: {dataset_id}, Actor: {actor_config.actor_key}")
        
        # Use pre-fetched jobs if available, otherwise fetch them
        if raw_jobs is None:
            logger.info(f"📥 Fetching data from dataset {dataset_id}")
            apify_service = ApifyService()
            raw_jobs = await apify_service.get_dataset_items(dataset_id)
            
            if not raw_jobs:
                logger.warning(f"⚠️  No data found in dataset {dataset_id}")
                return
        
        logger.info(f"📊 Processing {len(raw_jobs)} jobs from dataset {dataset_id}")
        
        # Send to Clay using the same transformation logic as normal runs
        clay_service = ClayService()
        result = await clay_service.send_jobs(
            jobs=raw_jobs,
            actor_config=actor_config,
            job_search=job_search
        )
        
        logger.info(f"✅ Manual push completed - Sent: {result['sent']}, Failed: {result['failed']}")
        
        if result['errors']:
            for error in result['errors']:
                logger.error(f"❌ Manual push error: {error}")
        
    except Exception as e:
        logger.error(f"💥 Manual push failed: {e}")


# =============================================================================
# HEALTH CHECK & SCHEDULER STATUS
# =============================================================================

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "version": "2.0.0"}


@app.get("/api/scheduler/status")
async def scheduler_status():
    """Get scheduler status and all scheduled jobs"""
    jobs = scheduler.get_all_scheduled_jobs()
    return {
        "timezone": "America/New_York (EST)",
        "total_scheduled": len(jobs),
        "jobs": jobs
    }


# =============================================================================
# ANALYTICS ENDPOINTS
# =============================================================================

@app.get("/analytics")
async def analytics_page(request: Request, db: Session = Depends(get_db)):
    """Analytics dashboard page"""
    return templates.TemplateResponse("analytics.html", {"request": request})

@app.get("/api/analytics/overview")
async def get_analytics_overview(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    platform: Optional[str] = None,
    db: Session = Depends(get_db)
):
    """Get consolidated analytics data"""
    from sqlalchemy import func, and_
    from datetime import datetime, timedelta
    
    # Parse date filters
    if start_date:
        try:
            start_dt = datetime.fromisoformat(start_date)
            # Set to beginning of day
            start_dt = start_dt.replace(hour=0, minute=0, second=0, microsecond=0)
        except ValueError:
            start_dt = datetime.now() - timedelta(days=7)
    else:
        start_dt = datetime.now() - timedelta(days=7)
    
    if end_date:
        try:
            end_dt = datetime.fromisoformat(end_date)
            # Set to end of day
            end_dt = end_dt.replace(hour=23, minute=59, second=59, microsecond=999999)
        except ValueError:
            end_dt = datetime.now()
    else:
        end_dt = datetime.now()
    
    logger.info(f"Analytics date range: {start_dt} to {end_dt}")
    
    # Base query filters
    filters = [JobRun.started_at >= start_dt, JobRun.started_at <= end_dt]
    if platform:
        # Join with JobSearch to filter by actor_key/platform
        filters.append(JobSearch.actor_key == platform)
    
    # Overall totals
    if platform:
        total_query = db.query(
            func.sum(JobRun.jobs_found).label('total_found'),
            func.sum(JobRun.jobs_filtered).label('total_filtered'), 
            func.sum(JobRun.jobs_sent).label('total_sent'),
            func.count(JobRun.id).label('total_runs')
        ).join(JobSearch, JobRun.job_search_id == JobSearch.id).filter(and_(*filters))
    else:
        total_query = db.query(
            func.sum(JobRun.jobs_found).label('total_found'),
            func.sum(JobRun.jobs_filtered).label('total_filtered'),
            func.sum(JobRun.jobs_sent).label('total_sent'),
            func.count(JobRun.id).label('total_runs')
        ).filter(JobRun.started_at >= start_dt, JobRun.started_at <= end_dt)
    
    totals = total_query.first()
    logger.info(f"Raw totals query result: {totals}")
    
    # Handle case where no data exists
    if not totals or totals.total_found is None:
        totals_dict = {
            "total_found": 0,
            "total_filtered": 0,
            "total_sent": 0,
            "total_runs": 0
        }
    else:
        totals_dict = {
            "total_found": totals.total_found or 0,
            "total_filtered": totals.total_filtered or 0,
            "total_sent": totals.total_sent or 0,
            "total_runs": totals.total_runs or 0
        }
    
    # Daily breakdown
    daily_query = db.query(
        func.date(JobRun.started_at).label('date'),
        func.sum(JobRun.jobs_found).label('found'),
        func.sum(JobRun.jobs_filtered).label('filtered'),
        func.sum(JobRun.jobs_sent).label('sent'),
        func.count(JobRun.id).label('runs')
    )
    
    if platform:
        daily_query = daily_query.join(JobSearch, JobRun.job_search_id == JobSearch.id).filter(and_(*filters))
    else:
        daily_query = daily_query.filter(JobRun.started_at >= start_dt, JobRun.started_at <= end_dt)
    
    daily_data = daily_query.group_by(func.date(JobRun.started_at)).order_by(func.date(JobRun.started_at)).all()
    
    # Platform breakdown
    platform_query = db.query(
        JobSearch.actor_key.label('platform'),
        ActorConfig.display_name.label('display_name'),
        func.sum(JobRun.jobs_found).label('found'),
        func.sum(JobRun.jobs_filtered).label('filtered'),
        func.sum(JobRun.jobs_sent).label('sent'),
        func.count(JobRun.id).label('runs')
    ).join(JobSearch, JobRun.job_search_id == JobSearch.id)\
     .join(ActorConfig, JobSearch.actor_key == ActorConfig.actor_key).filter(
        JobRun.started_at >= start_dt, JobRun.started_at <= end_dt
    ).group_by(JobSearch.actor_key, ActorConfig.display_name).order_by(func.sum(JobRun.jobs_found).desc()).all()
    
    # Top searches
    search_query = db.query(
        JobSearch.name.label('search_name'),
        JobSearch.actor_key.label('platform'),
        func.sum(JobRun.jobs_found).label('found'),
        func.sum(JobRun.jobs_filtered).label('filtered'), 
        func.sum(JobRun.jobs_sent).label('sent'),
        func.count(JobRun.id).label('runs')
    ).join(JobRun, JobSearch.id == JobRun.job_search_id).filter(
        JobRun.started_at >= start_dt, JobRun.started_at <= end_dt
    ).group_by(JobSearch.id, JobSearch.name, JobSearch.actor_key).order_by(func.sum(JobRun.jobs_found).desc()).limit(10).all()
    
    return {
        "period": {
            "start": start_dt.isoformat(),
            "end": end_dt.isoformat(),
            "days": (end_dt - start_dt).days + 1
        },
        "totals": {
            "jobs_found": totals_dict["total_found"],
            "jobs_filtered": totals_dict["total_filtered"],
            "jobs_sent": totals_dict["total_sent"],
            "total_runs": totals_dict["total_runs"],
            "filter_rate": round(totals_dict["total_filtered"] / max(totals_dict["total_found"], 1) * 100, 1),
            "success_rate": round(totals_dict["total_sent"] / max(totals_dict["total_found"], 1) * 100, 1)
        },
        "daily": [
            {
                "date": str(row.date),
                "jobs_found": row.found or 0,
                "jobs_filtered": row.filtered or 0, 
                "jobs_sent": row.sent or 0,
                "runs": row.runs or 0
            } for row in daily_data
        ],
        "platforms": [
            {
                "platform": row.platform,
                "display_name": row.display_name or row.platform,
                "jobs_found": row.found or 0,
                "jobs_filtered": row.filtered or 0,
                "jobs_sent": row.sent or 0,
                "runs": row.runs or 0,
                "filter_rate": round((row.filtered or 0) / max(row.found or 1, 1) * 100, 1),
                "success_rate": round((row.sent or 0) / max(row.found or 1, 1) * 100, 1)
            } for row in platform_query
        ],
        "top_searches": [
            {
                "name": row.search_name,
                "platform": row.platform, 
                "jobs_found": row.found or 0,
                "jobs_filtered": row.filtered or 0,
                "jobs_sent": row.sent or 0,
                "runs": row.runs or 0
            } for row in search_query
        ]
    }

# =============================================================================
# BACKGROUND TASKS
# =============================================================================

async def process_dataset_fallback(run_id: int, dataset_id: str, apify_token: str, db_session: Session):
    """
    Background task to process dataset data from Apify and send to Clay.
    This is a fallback when the main run times out but data is available.
    """
    from database import SessionLocal
    from scraper_service import ScraperService
    from actor_registry import ActorRegistry
    
    db = SessionLocal()
    try:
        # Get run and job search info
        run = db.query(JobRun).filter(JobRun.id == run_id).first()
        if not run:
            logger.error(f"[FALLBACK] Run {run_id} not found")
            return
        
        job_search = db.query(JobSearch).filter(JobSearch.id == run.job_search_id).first()
        if not job_search:
            logger.error(f"[FALLBACK] Job search {run.job_search_id} not found")
            return
        
        logger.info(f"[FALLBACK] Processing dataset {dataset_id} for run {run_id}")
        
        # Get actor config
        registry = ActorRegistry(db)
        actor_config = registry.get_actor(job_search.actor_key)
        
        # Initialize services
        scraper_service = ScraperService(db, apify_token)
        apify_service = scraper_service.apify_service
        filter_service = scraper_service.filter_service
        clay_service = scraper_service.clay_service
        
        # Fetch data from dataset
        async with httpx.AsyncClient(timeout=60.0) as client:
            raw_jobs = await apify_service._get_dataset_items(client, dataset_id)
        
        logger.info(f"[FALLBACK] Fetched {len(raw_jobs)} jobs from dataset {dataset_id}")
        
        if not raw_jobs:
            logger.warning(f"[FALLBACK] No data found in dataset {dataset_id}")
            return
        
        # Filter jobs
        passed_jobs, filtered_jobs = filter_service.filter_jobs(
            jobs=raw_jobs,
            actor_config=actor_config,
            job_search=job_search
        )
        
        # Store jobs in database (clear existing first to avoid duplicates)
        db.query(ScrapedJob).filter(ScrapedJob.job_run_id == run_id).delete()
        
        from template_engine import extract_fields
        for job_data in passed_jobs:
            extracted = extract_fields(job_data, actor_config.output_mapping or {})
            
            scraped_job = ScrapedJob(
                job_run_id=run_id,
                job_id=extracted.get("job_id"),
                raw_data=job_data,
                extracted_data=extracted,
                sent_to_clay=False,
                filtered_out=False
            )
            db.add(scraped_job)
        
        for job_data, reason in filtered_jobs:
            extracted = extract_fields(job_data, actor_config.output_mapping or {})
            
            scraped_job = ScrapedJob(
                job_run_id=run_id,
                job_id=extracted.get("job_id"),
                raw_data=job_data,
                extracted_data=extracted,
                sent_to_clay=False,
                filtered_out=True,
                filter_reason=reason
            )
            db.add(scraped_job)
        
        # Send to Clay if webhook configured
        jobs_sent = 0
        if passed_jobs and job_search.clay_webhook_url:
            extra_fields = {
                "platform": actor_config.actor_key,
                "search_name": job_search.name
            }
            
            clay_result = await clay_service.send_jobs(
                jobs=passed_jobs,
                actor_config=actor_config,
                job_search=job_search,
                extra_fields=extra_fields
            )
            
            jobs_sent = clay_result["sent"]
            
            # Update sent status
            if jobs_sent > 0:
                db.query(ScrapedJob).filter(
                    ScrapedJob.job_run_id == run_id,
                    ScrapedJob.filtered_out == False
                ).update({"sent_to_clay": True})
        
        # Update run statistics
        run.jobs_found = len(raw_jobs)
        run.jobs_filtered = len(filtered_jobs)
        run.jobs_sent = jobs_sent
        run.status = "completed"
        run.completed_at = datetime.utcnow()
        
        db.commit()
        
        logger.info(f"[FALLBACK] Completed: Found {len(raw_jobs)}, Filtered {len(filtered_jobs)}, Sent {jobs_sent}")
        
    except Exception as e:
        logger.exception(f"[FALLBACK] Error processing dataset {dataset_id}: {e}")
        
        # Mark run as failed
        if 'run' in locals():
            run.status = "failed"
            run.error_message = f"Fallback processing failed: {str(e)}"
            run.completed_at = datetime.utcnow()
            db.commit()
    finally:
        db.close()


async def push_to_clay_batch_task(
    run_id: int,
    clay_webhook_url: str,
    batch_size: int,
    batch_interval_ms: int,
    db_session: Session
):
    """
    Background task to push scraped jobs to Clay webhook in batches.
    """
    import asyncio
    from database import SessionLocal
    from actor_registry import ActorRegistry
    from clay_service import ClayService
    
    db = SessionLocal()
    try:
        # Get run info
        run = db.query(JobRun).filter(JobRun.id == run_id).first()
        if not run:
            logger.error(f"[CLAY-PUSH] Run {run_id} not found")
            return
        
        job_search = db.query(JobSearch).filter(JobSearch.id == run.job_search_id).first()
        if not job_search:
            logger.error(f"[CLAY-PUSH] Job search {run.job_search_id} not found")
            return
        
        # Get actor config
        registry = ActorRegistry(db)
        actor_config = registry.get_actor(job_search.actor_key)
        
        # Get all jobs that passed filters and haven't been sent yet
        jobs_to_send = db.query(ScrapedJob).filter(
            ScrapedJob.job_run_id == run_id,
            ScrapedJob.filtered_out == False
        ).all()
        
        if not jobs_to_send:
            logger.warning(f"[CLAY-PUSH] No jobs to send for run {run_id}")
            return
        
        logger.info(f"[CLAY-PUSH] Starting manual Clay push for run {run_id}: {len(jobs_to_send)} jobs in batches of {batch_size}")
        
        # Initialize Clay service
        clay_service = ClayService()
        
        # Process in batches
        total_sent = 0
        total_failed = 0
        batch_interval_seconds = batch_interval_ms / 1000
        
        for i in range(0, len(jobs_to_send), batch_size):
            batch = jobs_to_send[i:i + batch_size]
            batch_num = (i // batch_size) + 1
            total_batches = (len(jobs_to_send) + batch_size - 1) // batch_size
            
            logger.info(f"[CLAY-PUSH] Processing batch {batch_num}/{total_batches} ({len(batch)} jobs)")
            
            # Prepare batch data - convert ScrapedJob objects to raw data
            batch_raw_data = []
            for job in batch:
                batch_raw_data.append(job.raw_data)
            
            try:
                # Build extra fields
                extra_fields = {
                    "platform": actor_config.actor_key,
                    "search_name": job_search.name,
                    "manual_push": True,
                    "batch_number": batch_num
                }
                
                # Send batch to Clay
                clay_result = await clay_service.send_jobs(
                    jobs=batch_raw_data,
                    actor_config=actor_config,
                    job_search=job_search,
                    extra_fields=extra_fields
                )
                
                batch_sent = clay_result.get("sent", 0)
                total_sent += batch_sent
                
                # Update database - mark these jobs as sent
                if batch_sent > 0:
                    job_ids = [job.id for job in batch[:batch_sent]]
                    db.query(ScrapedJob).filter(
                        ScrapedJob.id.in_(job_ids)
                    ).update({"sent_to_clay": True}, synchronize_session=False)
                    db.commit()
                
                logger.info(f"[CLAY-PUSH] Batch {batch_num}/{total_batches}: {batch_sent}/{len(batch)} jobs sent successfully")
                
                if batch_sent < len(batch):
                    total_failed += len(batch) - batch_sent
                    logger.warning(f"[CLAY-PUSH] Batch {batch_num}: {len(batch) - batch_sent} jobs failed to send")
                
            except Exception as e:
                logger.error(f"[CLAY-PUSH] Batch {batch_num} failed: {e}")
                total_failed += len(batch)
            
            # Wait before next batch (unless it's the last batch)
            if i + batch_size < len(jobs_to_send):
                logger.info(f"[CLAY-PUSH] Waiting {batch_interval_seconds}s before next batch...")
                await asyncio.sleep(batch_interval_seconds)
        
        # Update run statistics
        run.jobs_sent = total_sent
        db.commit()
        
        logger.info(f"[CLAY-PUSH] Completed: {total_sent} jobs sent successfully, {total_failed} failed")
        
    except Exception as e:
        logger.exception(f"[CLAY-PUSH] Error in batch push task: {e}")
    finally:
        db.close()


# =============================================================================
# ERROR HANDLING
# =============================================================================

@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """Global exception handler"""
    logger.exception(f"Unhandled exception: {exc}")
    return JSONResponse(
        status_code=500,
        content={"success": False, "error": str(exc)}
    )
