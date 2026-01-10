from pydantic import BaseModel, HttpUrl, Field
from typing import Optional, List
from datetime import datetime


class JobSearchBase(BaseModel):
    name: str = Field(..., min_length=1, max_length=255)
    keyword: str = Field(..., min_length=1, max_length=500)
    platform: str = Field(default="linkedin", max_length=50)
    search_url: str = Field(..., min_length=1)
    cron_schedule: str = Field(..., description="Cron expression, e.g., '0 9 * * *'")
    
    # Apify config
    apify_actor_id: str = Field(default="curious_coder~linkedin-jobs-scraper")
    apify_token: Optional[str] = None
    max_results: int = Field(default=1000, ge=1, le=10000)
    scrape_company: bool = True
    
    # Filtering
    company_name_excludes: List[str] = Field(
        default_factory=lambda: ["staff", "recruit", "talent", "hire", "job", "search", "recruitment"]
    )
    industries_excludes: List[str] = Field(
        default_factory=lambda: ["staffing", "staffing and recruiting", "human", "non-profit"]
    )
    max_employee_count: Optional[int] = Field(default=300, ge=0)
    min_employee_count: Optional[int] = Field(default=0, ge=0)
    
    # Output
    clay_webhook_url: str = Field(..., min_length=1)
    batch_size: int = Field(default=8, ge=1, le=100)
    batch_interval_ms: int = Field(default=2000, ge=100)
    
    # Status
    is_active: bool = True


class JobSearchCreate(JobSearchBase):
    pass


class JobSearchUpdate(BaseModel):
    name: Optional[str] = None
    keyword: Optional[str] = None
    platform: Optional[str] = None
    search_url: Optional[str] = None
    cron_schedule: Optional[str] = None
    apify_actor_id: Optional[str] = None
    apify_token: Optional[str] = None
    max_results: Optional[int] = None
    scrape_company: Optional[bool] = None
    company_name_excludes: Optional[List[str]] = None
    industries_excludes: Optional[List[str]] = None
    max_employee_count: Optional[int] = None
    min_employee_count: Optional[int] = None
    clay_webhook_url: Optional[str] = None
    batch_size: Optional[int] = None
    batch_interval_ms: Optional[int] = None
    is_active: Optional[bool] = None


class JobSearchResponse(JobSearchBase):
    id: int
    last_run_at: Optional[datetime] = None
    last_status: Optional[str] = None
    created_at: datetime
    updated_at: datetime
    
    class Config:
        from_attributes = True


class JobRunResponse(BaseModel):
    id: int
    job_search_id: int
    started_at: datetime
    completed_at: Optional[datetime] = None
    status: str
    jobs_found: int
    jobs_filtered: int
    jobs_sent: int
    error_message: Optional[str] = None
    apify_run_id: Optional[str] = None
    
    class Config:
        from_attributes = True


class ScrapedJobResponse(BaseModel):
    id: int
    job_id: str
    title: Optional[str] = None
    company_name: Optional[str] = None
    location: Optional[str] = None
    sent_to_clay: bool
    filtered_out: bool
    filter_reason: Optional[str] = None
    created_at: datetime
    
    class Config:
        from_attributes = True


class TriggerJobRequest(BaseModel):
    job_search_id: int


class TriggerJobResponse(BaseModel):
    message: str
    job_run_id: int
    status: str
