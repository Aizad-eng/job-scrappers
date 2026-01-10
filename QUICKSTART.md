# 🚀 Quick Start - LinkedIn Job Scraper on Render

## What You Got

A complete, production-ready job scraping system that:
- ✅ Replaces your n8n workflow + Google Sheets
- ✅ Runs on Render with PostgreSQL database
- ✅ Polls Apify until scraping completes (smart waiting)
- ✅ Filters jobs by company/industry/size
- ✅ Sends to Clay webhooks in batches
- ✅ Fully configurable schedules per job
- ✅ Web dashboard to monitor everything

## 3-Minute Deployment

### 1. Push to GitHub
```bash
cd linkedin-scraper-render
git init
git add .
git commit -m "Initial commit"
git remote add origin <your-repo-url>
git push -u origin main
```

### 2. Deploy on Render
1. Go to https://dashboard.render.com/
2. Click "New" → "Blueprint"
3. Connect your GitHub repo
4. Click "Apply"
5. Wait ~5 minutes for deployment

### 3. Configure
In Render dashboard → Web Service → Environment:
- Add `DEFAULT_APIFY_TOKEN` = your Apify token
- Verify other variables are set

### 4. Access Your App
Visit: `https://your-app-name.onrender.com`

## Create Your First Job

### Option 1: Via Dashboard
1. Open `https://your-app.onrender.com/docs`
2. Use the interactive API docs to POST to `/api/job-searches`

### Option 2: Via cURL
```bash
curl -X POST "https://your-app.onrender.com/api/job-searches" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "DevOps Jobs - US",
    "keyword": "DevOps Engineer",
    "search_url": "https://www.linkedin.com/jobs/search/?keywords=DevOps",
    "cron_schedule": "0 9 * * *",
    "apify_token": "your_token",
    "clay_webhook_url": "https://api.clay.com/v3/sources/webhook/...",
    "max_employee_count": 300,
    "is_active": true
  }'
```

### Option 3: Migrate Existing Data
```bash
# Export Google Sheet to CSV first
python migrate.py csv your_sheet.csv https://your-app.onrender.com
```

## Test It

Trigger manually:
```bash
curl -X POST "https://your-app.onrender.com/api/trigger-job" \
  -H "Content-Type: application/json" \
  -d '{"job_search_id": 1}'
```

Watch logs in Render dashboard!

## Key Differences from n8n

| n8n Workflow | New System |
|--------------|------------|
| Google Sheets for config | PostgreSQL database |
| Fixed 10-minute wait | Smart polling (30s intervals) |
| Hard to modify filters | Configurable per job |
| Manual schedule changes | Edit via API anytime |
| Limited history | Full run history in DB |
| Complex workflow | Simple, maintainable code |

## Configuration Examples

### Schedule Patterns
```
"0 9 * * *"     - Daily at 9 AM
"0 */6 * * *"   - Every 6 hours
"0 9 * * 1-5"   - Weekdays at 9 AM
"*/30 * * * *"  - Every 30 minutes
```

### Filtering
```json
{
  "company_name_excludes": ["recruit", "staffing"],
  "industries_excludes": ["staffing and recruiting"],
  "max_employee_count": 300,
  "min_employee_count": 10
}
```

### Batching
```json
{
  "batch_size": 8,           // Jobs per batch
  "batch_interval_ms": 2000  // 2 seconds between batches
}
```

## Common Operations

**List all jobs:**
```bash
curl https://your-app.onrender.com/api/job-searches
```

**Update a job:**
```bash
curl -X PATCH "https://your-app.onrender.com/api/job-searches/1" \
  -H "Content-Type: application/json" \
  -d '{"is_active": false}'
```

**View run history:**
```bash
curl https://your-app.onrender.com/api/job-runs?job_search_id=1
```

**Check scheduler:**
```bash
curl https://your-app.onrender.com/api/scheduler/status
```

## Troubleshooting

**Jobs not running?**
- Check `/api/scheduler/status`
- Verify `is_active: true`
- Check Render logs

**Apify timeout?**
- Increase `APIFY_MAX_WAIT_MINUTES` in environment
- Check Apify dashboard for actor status

**Clay webhook failing?**
- Verify webhook URL in job config
- Check Clay webhook is active
- Look at job run error message

## Monitoring

- Dashboard: `https://your-app.onrender.com/`
- API Docs: `https://your-app.onrender.com/docs`
- Health: `https://your-app.onrender.com/health`
- Logs: Render Dashboard → Your Service → Logs

## Files Overview

```
main.py              - FastAPI app (routes, startup)
scraper_service.py   - Main workflow orchestration
apify_service.py     - Polls Apify, fetches data
job_filter.py        - Filtering logic
clay_service.py      - Webhook batching
scheduler.py         - Cron job management
models.py            - Database tables
config.py            - Settings
```

## Support

1. Read `README.md` for detailed docs
2. Check `DEPLOYMENT.md` for deployment help
3. View `PROJECT_STRUCTURE.md` for architecture
4. Logs in Render dashboard
5. Test manually via `/docs` interface

## Cost

- **Free Tier**: Limited hours, good for testing
- **Starter**: $14/month (web + database)
- **Production**: $45/month (better performance)

## Next Steps

1. ✅ Deploy to Render
2. ✅ Create test job (set `is_active: false`)
3. ✅ Trigger manually
4. ✅ Verify Clay receives data
5. ✅ Activate scheduling
6. ✅ Migrate all your jobs
7. ✅ Monitor first scheduled run

**You're done! No more n8n complexity. 🎉**

---

## Questions?

- Environment variables issues → Check `.env.example`
- Database errors → Verify `DATABASE_URL`
- Apify problems → Check token and actor ID
- Clay issues → Verify webhook URL format
- Filtering not working → Review job config

Good luck! 🚀
