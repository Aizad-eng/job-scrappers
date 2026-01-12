from sqlalchemy import (
    Column, Integer, String, Boolean, DateTime, Text, 
    ForeignKey, JSON
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from datetime import datetime

Base = declarative_base()


class ActorConfig(Base):
    """
    Stores configuration for each Apify actor.
    Adding a new scraper = adding a row here. No code changes needed.
    """
    __tablename__ = "actor_configs"
    
    id = Column(Integer, primary_key=True, index=True)
    
    # Unique key for this actor (e.g., "linkedin", "indeed", "fantastic_jobs")
    actor_key = Column(String(50), unique=True, nullable=False, index=True)
    
    # Display name
    display_name = Column(String(100), nullable=False)
    
    # Apify actor ID
    actor_id = Column(String(255), nullable=False)
    
    # Actor settings
    default_timeout_minutes = Column(Integer, default=15)
    default_max_results = Column(Integer, default=100)
    
    # Input schema - defines what inputs this actor accepts
    # Example: [{"name": "urls", "type": "urls", "label": "Search URLs", "required": true}, ...]
    input_schema = Column(JSON, default=list)
    
    # Input mapping - how to transform our generic inputs to actor's expected format
    # Example: {"urls": "{{urls}}", "count": "{{max_results}}", "scrapeCompany": true}
    input_template = Column(JSON, default=dict)
    
    # Output mapping - how to extract standardized fields from actor's output
    # Example: {"job_id": "id", "title": "title", "company_name": "companyName", ...}
    output_mapping = Column(JSON, default=dict)
    
    # Filter config - which output fields can be filtered and how
    # Example: {"company_name": {"field": "companyName", "type": "exclude_contains"}, ...}
    filter_config = Column(JSON, default=dict)
    
    # Clay mapping - how to transform output for Clay webhook
    # Example: {"Job Title": "{{title}}", "Company name": "{{company_name}}", ...}
    clay_template = Column(JSON, default=dict)
    
    # Is this actor active/available
    is_active = Column(Boolean, default=True)
    
    # Schedule type: "simple" (dropdown) or "custom" (date/time picker)
    schedule_type = Column(String(20), default="simple")
    
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    job_searches = relationship("JobSearch", back_populates="actor_config")


class JobSearch(Base):
    """
    Job search configuration - now generic and config-driven.
    All actor-specific fields are stored in JSON.
    """
    __tablename__ = "job_searches"
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), nullable=False)
    
    # Reference to actor config
    actor_key = Column(String(50), ForeignKey("actor_configs.actor_key"), nullable=False)
    
    # Scheduling
    cron_schedule = Column(String(50), nullable=False)
    
    # Apify token (can override default)
    apify_token = Column(String(255))
    
    # Dynamic actor inputs - stored as JSON, validated against actor's input_schema
    # Example for LinkedIn: {"urls": ["https://linkedin.com/jobs/..."], "max_results": 100}
    # Example for Fantastic Jobs: {"title_search": ["Engineer"], "location_search": ["US"], "time_range": "7d"}
    actor_inputs = Column(JSON, default=dict)
    
    # Dynamic filter rules - stored as JSON
    # Example: {"company_name_excludes": ["staffing", "recruiting"], "max_employees": 500}
    filter_rules = Column(JSON, default=dict)
    
    # Output configuration
    clay_webhook_url = Column(Text, nullable=False)
    batch_size = Column(Integer, default=8)
    batch_interval_ms = Column(Integer, default=2000)
    
    # Status
    is_active = Column(Boolean, default=True)
    last_run_at = Column(DateTime)
    last_status = Column(String(50))
    
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    actor_config = relationship("ActorConfig", back_populates="job_searches")
    runs = relationship("JobRun", back_populates="job_search", cascade="all, delete-orphan")


class JobRun(Base):
    """Tracks each execution of a job search"""
    __tablename__ = "job_runs"
    
    id = Column(Integer, primary_key=True, index=True)
    job_search_id = Column(Integer, ForeignKey("job_searches.id"), nullable=False)
    
    started_at = Column(DateTime, default=datetime.utcnow)
    completed_at = Column(DateTime)
    status = Column(String(50), default="running")
    
    jobs_found = Column(Integer, default=0)
    jobs_filtered = Column(Integer, default=0)
    jobs_sent = Column(Integer, default=0)
    
    error_message = Column(Text)
    execution_logs = Column(Text)  # Store detailed execution logs
    apify_run_id = Column(String(255))
    apify_dataset_id = Column(String(255))
    
    # Relationships
    job_search = relationship("JobSearch", back_populates="runs")
    scraped_jobs = relationship("ScrapedJob", back_populates="job_run", cascade="all, delete-orphan")


class ScrapedJob(Base):
    """
    Stores scraped jobs with both raw data and extracted fields.
    Raw data allows re-processing if mappings change.
    """
    __tablename__ = "scraped_jobs"
    
    id = Column(Integer, primary_key=True, index=True)
    job_run_id = Column(Integer, ForeignKey("job_runs.id"), nullable=False)
    
    # Unique job identifier (extracted based on actor's output_mapping)
    job_id = Column(String(255), index=True)
    
    # Store the complete raw data from Apify
    raw_data = Column(JSON, default=dict)
    
    # Extracted standardized fields (populated by output_mapping)
    extracted_data = Column(JSON, default=dict)
    
    # Status
    sent_to_clay = Column(Boolean, default=False)
    filtered_out = Column(Boolean, default=False)
    filter_reason = Column(String(255))
    
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    job_run = relationship("JobRun", back_populates="scraped_jobs")
