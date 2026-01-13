"""
Pydantic Schemas - Generic validation that works with any actor config.

No actor-specific schemas needed - validation is dynamic based on actor's input_schema.
"""

from pydantic import BaseModel, Field, validator
from typing import Dict, List, Any, Optional
from datetime import datetime


# =============================================================================
# ACTOR CONFIG SCHEMAS
# =============================================================================

class ActorInputField(BaseModel):
    """Schema for a single input field definition"""
    name: str
    type: str  # text, textarea, url, number, boolean, select
    label: str
    required: bool = False
    default: Any = None
    placeholder: Optional[str] = None
    help: Optional[str] = None
    options: Optional[List[Dict[str, str]]] = None  # For select type


class ActorConfigResponse(BaseModel):
    """Response schema for actor configuration"""
    actor_key: str
    display_name: str
    actor_id: str
    default_timeout_minutes: int
    default_max_results: int
    input_schema: List[ActorInputField]
    is_active: bool
    schedule_type: str = "simple"
    
    class Config:
        from_attributes = True


class ActorListResponse(BaseModel):
    """Response schema for list of actors"""
    actors: List[ActorConfigResponse]


# =============================================================================
# JOB SEARCH SCHEMAS
# =============================================================================

class JobSearchCreate(BaseModel):
    """Schema for creating a new job search"""
    name: str = Field(..., min_length=1, max_length=255)
    actor_key: str = Field(..., min_length=1)
    cron_schedule: str = Field(default="0 */6 * * *")
    apify_token: Optional[str] = None
    
    # Dynamic inputs - validated against actor's input_schema at runtime
    actor_inputs: Dict[str, Any] = Field(default_factory=dict)
    
    # Filter rules - validated against actor's filter_config at runtime
    filter_rules: Dict[str, Any] = Field(default_factory=dict)
    
    # Output config
    clay_webhook_url: str = Field(..., min_length=1)
    batch_size: int = Field(default=8, ge=1, le=50)
    batch_interval_ms: int = Field(default=2000, ge=100, le=30000)
    
    is_active: bool = True


class JobSearchUpdate(BaseModel):
    """Schema for updating a job search"""
    name: Optional[str] = Field(None, min_length=1, max_length=255)
    cron_schedule: Optional[str] = None
    apify_token: Optional[str] = None
    actor_inputs: Optional[Dict[str, Any]] = None
    filter_rules: Optional[Dict[str, Any]] = None
    clay_webhook_url: Optional[str] = None
    batch_size: Optional[int] = Field(None, ge=1, le=50)
    batch_interval_ms: Optional[int] = Field(None, ge=100, le=30000)
    is_active: Optional[bool] = None


class JobSearchResponse(BaseModel):
    """Response schema for a job search"""
    id: int
    name: str
    actor_key: str
    cron_schedule: str
    actor_inputs: Dict[str, Any]
    filter_rules: Dict[str, Any]
    clay_webhook_url: str
    batch_size: int
    batch_interval_ms: int
    is_active: bool
    last_run_at: Optional[datetime]
    last_status: Optional[str]
    created_at: datetime
    updated_at: datetime
    
    # Include actor display name
    actor_display_name: Optional[str] = None
    
    class Config:
        from_attributes = True


class JobSearchListResponse(BaseModel):
    """Response schema for list of job searches"""
    searches: List[JobSearchResponse]
    total: int


# =============================================================================
# JOB RUN SCHEMAS
# =============================================================================

class JobRunResponse(BaseModel):
    """Response schema for a job run"""
    id: int
    job_search_id: int
    started_at: datetime
    completed_at: Optional[datetime]
    status: str
    jobs_found: int
    jobs_filtered: int
    jobs_sent: int
    error_message: Optional[str]
    execution_logs: Optional[str]
    apify_run_id: Optional[str]
    apify_dataset_id: Optional[str]
    
    class Config:
        from_attributes = True


class JobRunDetailResponse(BaseModel):
    """Detailed response for a job run including jobs"""
    run: JobRunResponse
    passed_count: int
    filtered_count: int
    sample_jobs: List[Dict[str, Any]]
    filter_reasons: List[str]


# =============================================================================
# EXECUTION SCHEMAS
# =============================================================================

class ExecuteRequest(BaseModel):
    """Request to execute a job search"""
    job_search_id: int


class ExecuteResponse(BaseModel):
    """Response from executing a job search"""
    success: bool
    run_id: Optional[int]
    jobs_found: Optional[int]
    jobs_filtered: Optional[int]
    jobs_sent: Optional[int]
    error: Optional[str]


class TestActorRequest(BaseModel):
    """Request to test an actor configuration"""
    actor_key: str
    actor_inputs: Dict[str, Any]
    filter_rules: Optional[Dict[str, Any]] = None
    limit: int = Field(default=5, ge=1, le=20)


class TestActorResponse(BaseModel):
    """Response from testing an actor"""
    total_found: int
    passed_filter: int
    filtered_out: int
    sample_jobs: List[Dict[str, Any]]
    clay_previews: List[Dict[str, Any]]
    filter_reasons: List[str]


# =============================================================================
# SCRAPED JOB SCHEMAS
# =============================================================================

class ScrapedJobResponse(BaseModel):
    """Response schema for a scraped job"""
    id: int
    job_id: Optional[str]
    extracted_data: Dict[str, Any]
    sent_to_clay: bool
    filtered_out: bool
    filter_reason: Optional[str]
    created_at: datetime
    
    class Config:
        from_attributes = True


# =============================================================================
# GENERIC RESPONSES
# =============================================================================

class SuccessResponse(BaseModel):
    """Generic success response"""
    success: bool = True
    message: str


class ErrorResponse(BaseModel):
    """Generic error response"""
    success: bool = False
    error: str
    detail: Optional[str] = None
