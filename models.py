from sqlalchemy import (
    Column, Integer, String, Boolean, DateTime, Text, 
    ForeignKey, JSON, BigInteger
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from datetime import datetime

Base = declarative_base()


class JobSearch(Base):
    __tablename__ = "job_searches"
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), nullable=False)
    keyword = Column(String(500), nullable=False)
    platform = Column(String(50), default="linkedin")  # linkedin, indeed, etc
    search_url = Column(Text, nullable=False)
    cron_schedule = Column(String(50), nullable=False)  # e.g., "0 9 * * *"
    
    # Apify configuration
    apify_actor_id = Column(String(255), default="curious_coder~linkedin-jobs-scraper")
    apify_token = Column(String(255))
    max_results = Column(Integer, default=1000)
    scrape_company = Column(Boolean, default=True)
    
    # Filtering configuration (stored as JSON)
    company_name_excludes = Column(JSON, default=list)  # ["staff", "recruit", ...]
    industries_excludes = Column(JSON, default=list)
    max_employee_count = Column(Integer)
    min_employee_count = Column(Integer)
    
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
    runs = relationship("JobRun", back_populates="job_search", cascade="all, delete-orphan")


class JobRun(Base):
    __tablename__ = "job_runs"
    
    id = Column(Integer, primary_key=True, index=True)
    job_search_id = Column(Integer, ForeignKey("job_searches.id"), nullable=False)
    
    started_at = Column(DateTime, default=datetime.utcnow)
    completed_at = Column(DateTime)
    status = Column(String(50), default="running")  # running, success, failed, timeout
    
    jobs_found = Column(Integer, default=0)
    jobs_filtered = Column(Integer, default=0)
    jobs_sent = Column(Integer, default=0)
    
    error_message = Column(Text)
    apify_run_id = Column(String(255))
    apify_dataset_id = Column(String(255))
    
    # Relationships
    job_search = relationship("JobSearch", back_populates="runs")
    scraped_jobs = relationship("ScrapedJob", back_populates="job_run", cascade="all, delete-orphan")


class ScrapedJob(Base):
    __tablename__ = "scraped_jobs"
    
    id = Column(Integer, primary_key=True, index=True)
    job_run_id = Column(Integer, ForeignKey("job_runs.id"), nullable=False)
    
    # Job identifiers
    job_id = Column(String(255), unique=True, index=True)  # LinkedIn job ID
    title = Column(String(500))
    
    # Company info
    company_name = Column(String(255))
    company_url = Column(Text)
    company_linkedin_url = Column(Text)
    company_description = Column(Text)
    company_employees_count = Column(Integer)
    industries = Column(String(500))
    
    # Job details
    location = Column(String(255))
    posted_at = Column(String(100))
    description_text = Column(Text)
    apply_url = Column(Text)
    job_url = Column(Text)
    
    # Status
    sent_to_clay = Column(Boolean, default=False)
    filtered_out = Column(Boolean, default=False)
    filter_reason = Column(String(255))
    
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    job_run = relationship("JobRun", back_populates="scraped_jobs")
