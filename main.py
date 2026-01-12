"""
Job Scraper API - Config-driven, scalable job scraping platform.

Adding a new scraper = Adding config to actor_registry.py. That's it.
"""

import os
import logging
from typing import List, Optional
from datetime import datetime

from fastapi import FastAPI, Depends, HTTPException, Request, BackgroundTasks
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse, JSONResponse
from sqlalchemy.orm import Session

from database import init_db, get_db
from models import JobSearch, JobRun, ScrapedJob, ActorConfig
from actor_registry import ActorRegistry, SEED_ACTORS
from scraper_service import ScraperService
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
# STARTUP
# =============================================================================

@app.on_event("startup")
async def startup_event():
    """Initialize database and seed actor configs"""
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
    
    logger.info("Job Scraper API ready!")


# =============================================================================
# UI ROUTES
# =============================================================================

@app.get("/", response_class=HTMLResponse)
async def index(request: Request, db: Session = Depends(get_db)):
    """Main dashboard"""
    registry = ActorRegistry(db)
    actors = registry.list_actors()
    
    searches = db.query(JobSearch).order_by(JobSearch.created_at.desc()).all()
    
    # Enrich with actor display names
    actor_map = {a.actor_key: a.display_name for a in actors}
    for search in searches:
        search.actor_display_name = actor_map.get(search.actor_key, search.actor_key)
    
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
    
    return JobSearchResponse.model_validate(search)


@app.delete("/api/searches/{search_id}", response_model=SuccessResponse)
async def delete_search(search_id: int, db: Session = Depends(get_db)):
    """Delete a job search"""
    search = db.query(JobSearch).filter(JobSearch.id == search_id).first()
    
    if not search:
        raise HTTPException(status_code=404, detail="Job search not found")
    
    db.delete(search)
    db.commit()
    
    return SuccessResponse(message=f"Deleted job search '{search.name}'")


@app.post("/api/searches/{search_id}/duplicate", response_model=JobSearchResponse)
async def duplicate_search(search_id: int, db: Session = Depends(get_db)):
    """Duplicate a job search"""
    original = db.query(JobSearch).filter(JobSearch.id == search_id).first()
    
    if not original:
        raise HTTPException(status_code=404, detail="Job search not found")
    
    # Create copy
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
        is_active=False  # Start inactive
    )
    
    db.add(new_search)
    db.commit()
    db.refresh(new_search)
    
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

@app.get("/api/searches/{search_id}/runs", response_model=List[JobRunResponse])
async def get_search_runs(
    search_id: int,
    limit: int = 10,
    db: Session = Depends(get_db)
):
    """Get run history for a job search"""
    runs = db.query(JobRun).filter(
        JobRun.job_search_id == search_id
    ).order_by(JobRun.started_at.desc()).limit(limit).all()
    
    return [JobRunResponse.model_validate(r) for r in runs]


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


# =============================================================================
# HEALTH CHECK
# =============================================================================

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "version": "2.0.0"}


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
