# LinkedIn Job Scraper for Render

A production-ready job scraping system that automates LinkedIn job searches, filters results, and sends qualified jobs to Clay webhooks.

## Features

- ✅ **Automated Scraping**: Schedule job searches with flexible cron expressions
- ✅ **Smart Filtering**: Configurable filters for company size, industry, and keywords
- ✅ **Apify Integration**: Polls Apify actors until completion (no blind waiting)
- ✅ **Clay Integration**: Batched webhook delivery with retry logic
- ✅ **PostgreSQL Storage**: Persistent storage replacing Google Sheets
- ✅ **Web Dashboard**: Simple UI to monitor jobs and trigger manual runs
- ✅ **REST API**: Full CRUD operations for job configurations
- ✅ **Error Handling**: Comprehensive logging and retry mechanisms

## Architecture

```
┌─────────────────┐
│   Scheduler     │ ──→ Triggers jobs based on cron schedules
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Scraper Service │ ──→ Orchestrates the workflow
└────────┬────────┘
         │
         ├──→ Apify Service ──→ Start actor, poll until complete
         │
         ├──→ Job Filter ──→ Apply configurable filters
         │
         └──→ Clay Service ──→ Send batched data to webhook
```

## Quick Start on Render

### 1. Fork/Clone this Repository

```bash
git clone <your-repo-url>
cd linkedin-scraper-render
```

### 2. Deploy to Render

**Option A: Using render.yaml (Recommended)**

1. Push this code to your GitHub repository
2. Go to [Render Dashboard](https://dashboard.render.com/)
3. Click "New" → "Blueprint"
4. Connect your repository
5. Render will automatically:
   - Create a PostgreSQL database
   - Deploy the web service
   - Set up environment variables

**Option B: Manual Setup**

1. Create a PostgreSQL database:
   - Go to Render Dashboard → "New" → "PostgreSQL"
   - Name: `linkedin-scraper-db`
   - Plan: Starter (or Free)
   - Copy the "Internal Database URL"

2. Create a Web Service:
   - Go to Render Dashboard → "New" → "Web Service"
   - Connect your repository
   - Name: `linkedin-scraper-api`
   - Environment: `Python 3`
   - Build Command: `pip install -r requirements.txt`
   - Start Command: `python -m uvicorn main:app --host 0.0.0.0 --port $PORT`

3. Set Environment Variables:
   - `DATABASE_URL`: (paste Internal Database URL from step 1)
   - `SECRET_KEY`: (generate random string)
   - `DEFAULT_APIFY_TOKEN`: (your Apify API token)
   - `APP_ENV`: `production`
   - `DEBUG`: `false`

### 3. Access Your Application

Once deployed, Render will provide you with a URL like:
```
https://linkedin-scraper-api.onrender.com
```

- Dashboard: `https://your-app.onrender.com/`
- API Docs: `https://your-app.onrender.com/docs`
- Health Check: `https://your-app.onrender.com/health`

## Configuration

### Creating a Job Search

Use the API to create a new job search configuration:

```bash
curl -X POST "https://your-app.onrender.com/api/job-searches" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "DevOps Engineer - US",
    "keyword": "DevOps Engineer",
    "platform": "linkedin",
    "search_url": "https://www.linkedin.com/jobs/search/?keywords=DevOps%20Engineer&location=United%20States",
    "cron_schedule": "0 9 * * *",
    "apify_actor_id": "curious_coder~linkedin-jobs-scraper",
    "apify_token": "apify_api_YOUR_TOKEN_HERE",
    "max_results": 1000,
    "company_name_excludes": ["staff", "recruit", "talent"],
    "industries_excludes": ["staffing", "non-profit"],
    "max_employee_count": 300,
    "clay_webhook_url": "https://api.clay.com/v3/sources/webhook/...",
    "batch_size": 8,
    "batch_interval_ms": 2000,
    "is_active": true
  }'
```

### Cron Schedule Examples

- `0 9 * * *` - Every day at 9 AM
- `0 */6 * * *` - Every 6 hours
- `0 9 * * 1-5` - Weekdays at 9 AM
- `*/30 * * * *` - Every 30 minutes

## API Endpoints

### Job Searches

- `POST /api/job-searches` - Create new job search
- `GET /api/job-searches` - List all job searches
- `GET /api/job-searches/{id}` - Get specific job search
- `PATCH /api/job-searches/{id}` - Update job search
- `DELETE /api/job-searches/{id}` - Delete job search

### Job Execution

- `POST /api/trigger-job` - Manually trigger a job
- `GET /api/job-runs` - List job execution history
- `GET /api/job-runs/{id}` - Get specific job run details
- `GET /api/scraped-jobs` - List scraped jobs

### Scheduler

- `GET /api/scheduler/status` - Get scheduler status

## Database Schema

The application automatically creates these tables:

### job_searches
Stores job search configurations with filtering rules and Clay webhook URLs.

### job_runs
Tracks each execution with status, results count, and error messages.

### scraped_jobs
Stores individual job details with filtering status.

## How It Works

1. **Scheduler** runs based on cron expressions
2. **Apify Service** starts the LinkedIn scraper actor
3. **Polling** checks actor status every 30 seconds (configurable)
4. When complete, **fetches** all scraped jobs
5. **Filter** applies company/industry/size rules
6. **Clay Service** sends passing jobs in batches to webhook
7. **Database** stores all results for tracking

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `DATABASE_URL` | PostgreSQL connection string | (required) |
| `SECRET_KEY` | App secret key | (required) |
| `DEFAULT_APIFY_TOKEN` | Default Apify API token | (optional) |
| `APIFY_POLL_INTERVAL_SECONDS` | Polling interval | 30 |
| `APIFY_MAX_WAIT_MINUTES` | Max wait time for actor | 15 |
| `APP_ENV` | Environment | production |
| `DEBUG` | Debug mode | false |

## Monitoring

- Check `/health` endpoint for application status
- View logs in Render Dashboard
- Monitor job runs in the database or via API

## Troubleshooting

### Database Connection Issues
Ensure `DATABASE_URL` is set correctly. Render provides this automatically when using render.yaml.

### Jobs Not Running
1. Check scheduler status: `GET /api/scheduler/status`
2. Verify cron schedule syntax
3. Ensure `is_active` is `true`
4. Check application logs in Render

### Apify Timeouts
Increase `APIFY_MAX_WAIT_MINUTES` if scraping large result sets.

### Clay Webhook Failures
- Verify webhook URL is correct
- Check Clay webhook is active
- Review job run logs for specific errors

## Local Development

```bash
# Clone repository
git clone <your-repo-url>
cd linkedin-scraper-render

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Set up environment variables
cp .env.example .env
# Edit .env with your values

# Run database migrations (creates tables)
python -c "from database import init_db; init_db()"

# Start application
python main.py
```

Access at: `http://localhost:8000`

## Migration from n8n

Your existing Google Sheets data can be migrated using this script:

```python
import requests
import pandas as pd

# Read your Google Sheet
df = pd.read_csv('your_sheet.csv')

# Create job searches via API
for _, row in df.iterrows():
    payload = {
        "name": row['Keyword'],
        "keyword": row['Keyword'],
        "search_url": row['URL'],
        "cron_schedule": "0 9 * * *",  # Adjust as needed
        "clay_webhook_url": row['webhook_url'],
        "apify_token": row['api_token'],
        # Add other fields...
    }
    
    response = requests.post(
        "https://your-app.onrender.com/api/job-searches",
        json=payload
    )
    print(f"Created: {response.json()}")
```

## Support

For issues or questions:
1. Check Render logs
2. Review API documentation at `/docs`
3. Verify environment variables
4. Test with manual trigger first

## License

MIT License - See LICENSE file for details
