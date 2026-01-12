# 🚀 Fantastic Jobs Career Site Scraper - Update Guide

## What's New

Added **Fantastic Jobs** (`fantastic-jobs~advanced-linkedin-job-search-api`) as a third platform alongside LinkedIn and Indeed.

### New Features

1. **Title-based search** - Search by job titles instead of URLs
2. **Location filtering** - Filter by location (e.g., "united states", "california")  
3. **Employer filtering** - Optionally filter to specific employers
4. **Time range selection** - 1h, 24h, 7d, 6m options
5. **Employee count filters** - Min/max sent to Apify for pre-filtering
6. **AI-enriched data** - Salary, skills, experience level, work arrangement
7. **LinkedIn company data** - Employee count, industry, HQ, specialties

---

## Files to Update (7 files)

Upload these files to your GitHub repo to replace existing ones:

| File | Changes |
|------|---------|
| `models.py` | Added `title_search`, `location_search`, `employer_search`, `time_range`, `include_ai`, `include_linkedin` columns |
| `schemas.py` | Added Fantastic Jobs fields to Pydantic schemas |
| `apify_service.py` | Added `fantastic_jobs` platform config and payload builder |
| `job_filter.py` | Added `_should_filter_fantastic_jobs()` method |
| `clay_service.py` | Added `_prepare_fantastic_jobs_payload()` with all 30+ fields |
| `scraper_service.py` | Added Fantastic Jobs field extraction and workflow |
| `main.py` | Added Fantastic Jobs fields to duplicate endpoint |
| `templates/index.html` | Added UI for Fantastic Jobs with conditional fields |

---

## Upload Steps

### 1. Replace Each File on GitHub

For each file:
1. Go to: `https://github.com/Aizad-eng/job-scrappers/blob/main/<filename>`
2. Click **pencil icon** ✏️ (Edit)
3. Select All (Ctrl+A) → Delete
4. Copy content from downloaded file
5. Paste into GitHub
6. Click **Commit changes**

### 2. Run Database Migration

After deploying, run this SQL to add new columns (Render will do this automatically if using SQLAlchemy's create_all):

```sql
-- If tables already exist, add new columns:
ALTER TABLE job_searches ADD COLUMN IF NOT EXISTS title_search JSON DEFAULT '[]';
ALTER TABLE job_searches ADD COLUMN IF NOT EXISTS location_search JSON DEFAULT '[]';
ALTER TABLE job_searches ADD COLUMN IF NOT EXISTS employer_search JSON DEFAULT '[]';
ALTER TABLE job_searches ADD COLUMN IF NOT EXISTS time_range VARCHAR(10) DEFAULT '7d';
ALTER TABLE job_searches ADD COLUMN IF NOT EXISTS include_ai BOOLEAN DEFAULT true;
ALTER TABLE job_searches ADD COLUMN IF NOT EXISTS include_linkedin BOOLEAN DEFAULT true;

ALTER TABLE scraped_jobs ADD COLUMN IF NOT EXISTS experience_level VARCHAR(50);
ALTER TABLE scraped_jobs ADD COLUMN IF NOT EXISTS work_arrangement VARCHAR(50);
ALTER TABLE scraped_jobs ADD COLUMN IF NOT EXISTS key_skills JSON;
ALTER TABLE scraped_jobs ADD COLUMN IF NOT EXISTS source_domain VARCHAR(255);
```

### 3. Wait for Render Deploy

~3-5 minutes for auto-deploy after pushing to GitHub.

---

## Creating a Fantastic Jobs Search

### Via Dashboard

1. Click **Create New Job Search**
2. Select Platform: **Fantastic Jobs (Career Sites)**
3. Enter job titles (one per line):
   ```
   Test Engineer
   Regulatory Affairs Engineer
   EMC Engineer
   Certification Engineer
   ```
4. Set Location: `united states`
5. Set Time Range: `7d`
6. Configure employee filters if needed
7. Add your Apify token and Clay webhook
8. Save!

### Via API

```bash
curl -X POST "https://your-app.onrender.com/api/job-searches" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "FCC Compliance Test Engineers",
    "keyword": "Test Engineer",
    "platform": "fantastic_jobs",
    "search_url": "",
    "cron_schedule": "0 9 * * *",
    "apify_token": "YOUR_APIFY_TOKEN",
    "clay_webhook_url": "https://api.clay.com/...",
    "title_search": [
      "Test Engineer",
      "Regulatory Affairs Engineer",
      "EMC Engineer",
      "Certification Engineer",
      "Quality Engineer"
    ],
    "location_search": ["united states"],
    "employer_search": [],
    "time_range": "7d",
    "min_employee_count": 500,
    "max_employee_count": 10000,
    "max_results": 100,
    "is_active": true
  }'
```

---

## Clay Webhook Output Fields

Fantastic Jobs sends these fields to Clay:

**Core Job Info:**
- ID, Job Title, JOB URL, Apply URL, Job Description, Posted at

**Company Info:**
- Company name, Company website, Company domain, Linkedin URL
- Company description, Industry, Employee Count, Company Size
- Company Type, Company Founded, Company HQ, Company Specialties

**Location:**
- Location, Remote (boolean), Work Arrangement

**AI-Enriched:**
- Experience Level, Employment Type, Salary, Currency
- Key Skills, Requirements Summary, Core Responsibilities
- Education Requirements, Visa Sponsorship

**Source:**
- Source, Source Type, Source Domain, keyword, platform

---

## Apify Input Parameters

The system sends these to Fantastic Jobs actor:

```json
{
  "titleSearch": ["Test Engineer", "EMC Engineer"],
  "locationSearch": ["united states"],
  "employerSearch": ["optional", "filter"],
  "limit": 100,
  "timeRange": "7d",
  "liOrganizationEmployeesGte": 500,
  "liOrganizationEmployeesLte": 10000,
  "includeAi": true,
  "includeLinkedIn": true,
  "aiHasSalary": false,
  "aiVisaSponsorshipFilter": false,
  "descriptionType": "text"
}
```

---

## Checklist

- [ ] Upload `models.py`
- [ ] Upload `schemas.py`
- [ ] Upload `apify_service.py`
- [ ] Upload `job_filter.py`
- [ ] Upload `clay_service.py`
- [ ] Upload `scraper_service.py`
- [ ] Upload `main.py`
- [ ] Upload `templates/index.html`
- [ ] Wait for Render deploy
- [ ] Test creating Fantastic Jobs search
- [ ] Trigger job manually
- [ ] Verify Clay receives data

---

## Troubleshooting

**Database column errors?**
- Run the ALTER TABLE statements above, or delete the database and let SQLAlchemy recreate it

**Actor not found?**
- Verify actor ID: `fantastic-jobs~advanced-linkedin-job-search-api`
- Check your Apify token has access

**No results?**
- Try broader job titles
- Extend time range to `6m`
- Remove employer filter

---

Good luck! 🎉
