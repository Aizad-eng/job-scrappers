# Project Structure

```
linkedin-scraper-render/
│
├── main.py                 # FastAPI application with all routes
├── config.py              # Configuration and environment variables
├── database.py            # Database connection and session management
├── models.py              # SQLAlchemy database models
├── schemas.py             # Pydantic schemas for API validation
│
├── apify_service.py       # Apify API integration (start, poll, fetch)
├── job_filter.py          # Job filtering logic
├── clay_service.py        # Clay webhook integration
├── scraper_service.py     # Main orchestration service
├── scheduler.py           # APScheduler for cron jobs
│
├── templates/
│   └── index.html         # Dashboard UI
│
├── requirements.txt       # Python dependencies
├── Dockerfile            # Container configuration
├── render.yaml           # Render deployment config
├── .env.example          # Environment variables template
├── .gitignore           # Git ignore rules
│
├── README.md            # Main documentation
├── DEPLOYMENT.md        # Deployment guide
└── migrate.py          # Migration script from Google Sheets
```

## File Descriptions

### Core Application Files

**main.py**
- FastAPI application setup
- Web UI routes (dashboard)
- REST API endpoints for CRUD operations
- Job execution endpoints
- Scheduler management
- Startup/shutdown event handlers

**config.py**
- Centralized configuration management
- Environment variable loading
- Application settings (Pydantic)

**database.py**
- SQLAlchemy engine setup
- Database session management
- Database initialization

**models.py**
- Database table definitions:
  - `JobSearch`: Job search configurations
  - `JobRun`: Execution history
  - `ScrapedJob`: Individual job records

**schemas.py**
- Pydantic models for request/response validation
- API input/output schemas
- Type safety and validation

### Service Layer

**apify_service.py**
- Start Apify actor runs
- Poll actor status with configurable intervals
- Wait for completion with timeout
- Fetch dataset items
- Retry logic with exponential backoff

**job_filter.py**
- Filter jobs by company name patterns
- Filter by industry exclusions
- Filter by employee count ranges
- Return filtered vs. passed jobs with reasons

**clay_service.py**
- Transform job data to Clay format
- Send jobs in configurable batches
- Handle rate limiting
- Retry failed webhook calls
- Clean URLs (remove query parameters)

**scraper_service.py**
- Main workflow orchestration
- Coordinates all services
- Database persistence
- Error handling and status updates
- Comprehensive logging

**scheduler.py**
- APScheduler integration
- Cron-based job scheduling
- Dynamic job management (add/remove)
- Load jobs from database on startup

### Frontend

**templates/index.html**
- Simple dashboard UI
- View all job searches
- Job statistics
- Manual trigger buttons
- Status indicators

### Configuration & Deployment

**requirements.txt**
- Python package dependencies
- Pinned versions for reproducibility

**Dockerfile**
- Container image definition
- Python 3.11 slim base
- PostgreSQL client tools

**render.yaml**
- Infrastructure as Code
- Automatic service provisioning
- Database and web service config
- Environment variable setup

**.env.example**
- Template for environment variables
- Configuration documentation

**.gitignore**
- Exclude sensitive files
- Python cache files
- Virtual environments

### Documentation

**README.md**
- Project overview
- Features and architecture
- Quick start guide
- API documentation
- Configuration examples

**DEPLOYMENT.md**
- Step-by-step deployment instructions
- Troubleshooting guide
- Monitoring and maintenance
- Scaling strategies
- Cost estimation

**migrate.py**
- Import existing Google Sheets data
- CSV to API conversion
- Sample data creation
- Batch migration support

## Key Features by File

### Data Flow

1. **Scheduler (scheduler.py)** triggers job based on cron
2. **Scraper Service (scraper_service.py)** orchestrates workflow
3. **Apify Service (apify_service.py)** runs LinkedIn scraper
4. **Job Filter (job_filter.py)** applies filtering rules
5. **Clay Service (clay_service.py)** sends to webhook
6. **Database (models.py)** persists all results

### API Endpoints (main.py)

**Job Search Management:**
- POST   /api/job-searches
- GET    /api/job-searches
- GET    /api/job-searches/{id}
- PATCH  /api/job-searches/{id}
- DELETE /api/job-searches/{id}

**Job Execution:**
- POST /api/trigger-job
- GET  /api/job-runs
- GET  /api/job-runs/{id}
- GET  /api/scraped-jobs

**Scheduler:**
- GET /api/scheduler/status

**Health:**
- GET /health

### Configuration (config.py)

Environment variables:
- DATABASE_URL
- APIFY tokens and polling config
- Secret keys
- Debug settings

### Models (models.py)

**JobSearch:**
- Search configuration
- Filtering rules
- Clay webhook URL
- Cron schedule
- Active status

**JobRun:**
- Execution tracking
- Status and timestamps
- Result counts
- Error messages

**ScrapedJob:**
- Individual job details
- Filter status and reason
- Clay delivery status

## Extension Points

### Adding New Platforms

1. Create new service in `platforms/` directory
2. Implement scraping interface
3. Add platform type to `schemas.py`
4. Update `scraper_service.py` to support platform

### Custom Filters

1. Extend `JobFilter` class in `job_filter.py`
2. Add new filter fields to `JobSearch` model
3. Update API schemas

### Additional Integrations

1. Create new service file (e.g., `slack_service.py`)
2. Add webhook/API configuration to models
3. Integrate in `scraper_service.py`

### Authentication

1. Add auth middleware to `main.py`
2. Implement user model and JWT
3. Protect endpoints with dependencies

## Best Practices

- **Logging:** All services use Python logging
- **Error Handling:** Try-catch with proper cleanup
- **Retries:** Tenacity library for network calls
- **Type Safety:** Pydantic for all data validation
- **Database:** SQLAlchemy ORM with relationships
- **Async:** Uses `asyncio` for concurrent operations
- **Testing:** Structure supports easy unit testing

## Development Workflow

1. Clone repository
2. Set up virtual environment
3. Install dependencies
4. Configure `.env`
5. Initialize database
6. Run locally: `python main.py`
7. Access at `http://localhost:8000`

## Production Deployment

1. Push to GitHub
2. Connect to Render
3. Automatic deployment via `render.yaml`
4. Database auto-provisioned
5. Environment variables configured
6. Continuous deployment on push

## Monitoring

- Health check: `/health`
- Scheduler status: `/api/scheduler/status`
- Job runs: `/api/job-runs`
- Render dashboard logs
- Database metrics in Render

## Maintenance

- Clean old job runs periodically
- Monitor database size
- Review error logs
- Update dependencies
- Rotate API tokens
- Backup database
