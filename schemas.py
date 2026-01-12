from pydantic import BaseModel, HttpUrl, Field
from typing import Optional, List
from datetime import datetime


class JobSearchBase(BaseModel):
    name: str = Field(..., min_length=1, max_length=255)
    keyword: str = Field(..., min_length=1, max_length=500)
    platform: str = Field(default="linkedin", max_length=50)
    search_url: str = Field(default="", description="Optional for fantastic_jobs platform")
    cron_schedule: str = Field(..., description="Cron expression, e.g., '0 9 * * *'")
    
    # Apify config
    apify_actor_id: str = Field(default="curious_coder~linkedin-jobs-scraper")
    apify_token: Optional[str] = None
    max_results: int = Field(default=1000, ge=1, le=10000)
    scrape_company: bool = True
    
    # Indeed specific
    use_browser: bool = False
    use_apify_proxy: bool = True
    
    # ============================================================
    # Fantastic Jobs specific settings
    # ============================================================
    title_search: List[str] = Field(
        default_factory=list,
        description="Job titles to search (for fantastic_jobs platform)"
    )
    location_search: List[str] = Field(
        default_factory=lambda: ["united states"],
        description="Locations to search (for fantastic_jobs platform)"
    )
    employer_search: List[str] = Field(
        default_factory=list,
        description="Employer/company names to filter (for fantastic_jobs platform)"
    )
    time_range: str = Field(
        default="7d",
        description="Time range: 1h, 24h, 7d, 6m (for fantastic_jobs platform)"
    )
    include_ai: bool = Field(default=True, description="Include AI-enriched data")
    include_linkedin: bool = Field(default=True, description="Include LinkedIn company data")
    
    # ============================================================
    # Filtering
    # ============================================================
    company_name_excludes: List[str] = Field(
        default_factory=lambda: ["staff", "recruit", "talent", "hire", "job", "search", "recruitment"]
    )
    industries_excludes: List[str] = Field(
        default_factory=lambda: ["staffing", "staffing and recruiting", "human", "non-profit"]
    )
    max_employee_count: Optional[int] = Field(default=None, ge=0)
    min_employee_count: Optional[int] = Field(default=None, ge=0)
    job_description_filters: Optional[str] = Field(default=None, description="Regex pattern for filtering")
    
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
    
    # Indeed specific
    use_browser: Optional[bool] = None
    use_apify_proxy: Optional[bool] = None
    
    # Fantastic Jobs specific
    title_search: Optional[List[str]] = None
    location_search: Optional[List[str]] = None
    employer_search: Optional[List[str]] = None
    time_range: Optional[str] = None
    include_ai: Optional[bool] = None
    include_linkedin: Optional[bool] = None
    
    # Filtering
    company_name_excludes: Optional[List[str]] = None
    industries_excludes: Optional[List[str]] = None
    max_employee_count: Optional[int] = None
    min_employee_count: Optional[int] = None
    job_description_filters: Optional[str] = None
    
    # Output
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
