# Deployment Guide for Render

## Step-by-Step Deployment

### Prerequisites
- GitHub account
- Render account (free tier works)
- Apify account with API token
- Clay workspace with webhook URLs

---

## Option 1: Automatic Deployment (Recommended)

### 1. Prepare Your Repository

```bash
# Push all files to GitHub
git init
git add .
git commit -m "Initial commit"
git remote add origin <your-github-repo-url>
git push -u origin main
```

### 2. Deploy on Render

1. Go to https://dashboard.render.com/
2. Click **"New"** → **"Blueprint"**
3. Connect your GitHub repository
4. Select the repository with this code
5. Render will detect `render.yaml` and show you:
   - Web Service: `linkedin-scraper-api`
   - Database: `linkedin-scraper-db`
6. Click **"Apply"**

### 3. Configure Environment Variables

After deployment starts, go to the Web Service settings:

1. Click on **"Environment"** tab
2. Add/Update these variables:
   - `DEFAULT_APIFY_TOKEN`: Your Apify API token
   - Verify `DATABASE_URL` is auto-populated
   - Verify `SECRET_KEY` was auto-generated
3. Click **"Save Changes"**

### 4. Wait for Deployment

- Database creation: ~2-3 minutes
- Web service deployment: ~5-7 minutes
- Watch the logs for any errors

### 5. Verify Deployment

Once deployed, your app will be at:
```
https://linkedin-scraper-api.onrender.com
```

Test these endpoints:
- Dashboard: `https://your-app.onrender.com/`
- Health: `https://your-app.onrender.com/health`
- API Docs: `https://your-app.onrender.com/docs`

---

## Option 2: Manual Deployment

### Step 1: Create PostgreSQL Database

1. In Render Dashboard, click **"New"** → **"PostgreSQL"**
2. Configure:
   - Name: `linkedin-scraper-db`
   - Database: `linkedin_scraper`
   - User: `linkedin_scraper_user`
   - Region: Choose closest to you
   - Plan: **Starter** ($7/month) or **Free** (expires in 90 days)
3. Click **"Create Database"**
4. **Copy the "Internal Database URL"** - you'll need this

### Step 2: Create Web Service

1. Click **"New"** → **"Web Service"**
2. Connect your GitHub repository
3. Configure:
   - Name: `linkedin-scraper-api`
   - Region: Same as database
   - Branch: `main`
   - Runtime: **Python 3**
   - Build Command: `pip install -r requirements.txt`
   - Start Command: `python -m uvicorn main:app --host 0.0.0.0 --port $PORT`
4. **Environment Variables** - Add these:

```
DATABASE_URL=<paste-internal-database-url-from-step-1>
APP_ENV=production
DEBUG=false
SECRET_KEY=<generate-random-string-32-chars>
DEFAULT_APIFY_TOKEN=<your-apify-token>
APIFY_POLL_INTERVAL_SECONDS=30
APIFY_MAX_WAIT_MINUTES=15
```

5. Instance Type: **Starter** ($7/month) or **Free** (limited)
6. Click **"Create Web Service"**

### Step 3: Monitor Deployment

Watch the deployment logs:
- Database initialization
- Package installation
- Application startup
- Look for: `"Application started successfully"`

---

## Post-Deployment Setup

### 1. Create Your First Job Search

Using the API (replace with your deployed URL):

```bash
curl -X POST "https://your-app.onrender.com/api/job-searches" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Test Job Search",
    "keyword": "Software Engineer",
    "platform": "linkedin",
    "search_url": "https://www.linkedin.com/jobs/search/?keywords=Software%20Engineer",
    "cron_schedule": "0 10 * * *",
    "apify_token": "YOUR_APIFY_TOKEN",
    "max_results": 100,
    "clay_webhook_url": "YOUR_CLAY_WEBHOOK_URL",
    "is_active": false
  }'
```

### 2. Test Manual Execution

1. Get the job search ID from the response
2. Trigger it manually:

```bash
curl -X POST "https://your-app.onrender.com/api/trigger-job" \
  -H "Content-Type: application/json" \
  -d '{"job_search_id": 1}'
```

3. Monitor in the dashboard or check logs

### 3. Activate Scheduler

Once tested, update the job to active:

```bash
curl -X PATCH "https://your-app.onrender.com/api/job-searches/1" \
  -H "Content-Type: application/json" \
  -d '{"is_active": true}'
```

---

## Migrating Existing Data

### Export Google Sheets to CSV

1. Open your Google Sheet
2. File → Download → CSV (.csv)
3. Save as `jobs_export.csv`

### Run Migration Script

```bash
# Install requests if needed
pip install requests

# Run migration
python migrate.py csv jobs_export.csv https://your-app.onrender.com
```

---

## Monitoring & Maintenance

### Check Logs

1. Go to Render Dashboard
2. Click on your Web Service
3. Click **"Logs"** tab
4. Look for:
   - Job execution logs
   - Error messages
   - Scheduler status

### Monitor Database

1. Click on your PostgreSQL database
2. View **"Metrics"** for:
   - Storage usage
   - Connection count
   - Query performance

### View Job History

Use the API:

```bash
# List all job runs
curl "https://your-app.onrender.com/api/job-runs?limit=10"

# Get specific run details
curl "https://your-app.onrender.com/api/job-runs/1"

# View scraped jobs
curl "https://your-app.onrender.com/api/scraped-jobs?job_run_id=1"
```

---

## Troubleshooting

### Service Won't Start

**Check logs for:**
- Database connection errors → Verify `DATABASE_URL`
- Import errors → Check all files are committed
- Port binding errors → Ensure using `$PORT` variable

**Solution:**
```bash
# In Render logs, look for specific error
# Update environment variables
# Trigger manual deploy
```

### Database Connection Failed

**Error:** `could not connect to server`

**Solutions:**
1. Verify `DATABASE_URL` format: `postgresql://user:pass@host:5432/db`
2. Check database is running
3. Verify both services in same region
4. Use **Internal Database URL**, not external

### Jobs Not Running

**Check:**
1. Scheduler status: Visit `/api/scheduler/status`
2. Job is active: `"is_active": true`
3. Cron schedule is valid
4. Check logs during expected run time

**Debug:**
```bash
# Manually trigger to test
curl -X POST "https://your-app.onrender.com/api/trigger-job" \
  -H "Content-Type: application/json" \
  -d '{"job_search_id": 1}'
```

### Apify Timeout

**Error:** `Actor run timed out`

**Solutions:**
1. Increase timeout: `APIFY_MAX_WAIT_MINUTES=20`
2. Reduce `max_results` in job config
3. Check Apify actor status directly

### Out of Memory

**Error:** `Process out of memory`

**Solutions:**
1. Upgrade to larger instance type
2. Reduce `batch_size` in job configs
3. Limit concurrent job runs

---

## Scaling

### Increase Performance

1. **Upgrade Instance Type:**
   - Starter → Standard ($25/mo)
   - More CPU & RAM for faster processing

2. **Optimize Database:**
   - Upgrade database plan
   - Add indexes for frequent queries
   - Clean old job runs periodically

3. **Batch Processing:**
   - Adjust batch sizes
   - Tune polling intervals
   - Stagger job schedules

### Multiple Regions

Deploy to multiple regions for redundancy:
1. Create new services in different regions
2. Share same database (or replicate)
3. Use different schedules to avoid conflicts

---

## Security Best Practices

1. **Never commit secrets to Git:**
   - Use Render's environment variables
   - Rotate API tokens regularly

2. **Database Security:**
   - Use internal connection string
   - Don't expose database publicly
   - Regular backups

3. **API Security:**
   - Add authentication (not included in basic version)
   - Use HTTPS only (automatic on Render)
   - Rate limiting for public endpoints

---

## Backup Strategy

### Automatic Backups (Render Pro)
Render provides automatic daily backups on paid plans.

### Manual Backup

```bash
# Export database
pg_dump $DATABASE_URL > backup.sql

# Or use Render's backup feature
# Dashboard → Database → Backups tab
```

### Restore from Backup

```bash
# Restore database
psql $DATABASE_URL < backup.sql
```

---

## Cost Estimation

### Free Tier
- Web Service: Free (limited hours)
- Database: Free (90 days, then $7/mo)
- **Total: $0-7/month**

### Starter Plan
- Web Service: $7/month
- Database: $7/month
- **Total: $14/month**

### Production Plan
- Web Service: $25/month (Standard)
- Database: $20/month (Standard)
- **Total: $45/month**

---

## Support Resources

- Render Docs: https://render.com/docs
- Render Status: https://status.render.com
- Apify Docs: https://docs.apify.com
- FastAPI Docs: https://fastapi.tiangolo.com

---

## Next Steps

1. ✅ Deploy to Render
2. ✅ Test with sample job
3. ✅ Migrate existing data
4. ✅ Monitor first scheduled run
5. ✅ Set up alerting (email/Slack)
6. ✅ Document your custom filters
7. ✅ Scale as needed

**You're ready to go! 🚀**
