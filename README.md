# Job Scraper v2.0 - Config-Driven Architecture

**Adding a new scraper = Adding JSON config. Zero code changes. No migrations.**

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                    ACTOR REGISTRY (actor_registry.py)           │
│  Define actors with:                                            │
│  - Input schema (what the UI form shows)                        │
│  - Input template (how to build Apify payload)                  │
│  - Output mapping (how to extract standardized fields)          │
│  - Filter config (what can be filtered)                         │
│  - Clay template (what gets sent to webhook)                    │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    GENERIC SERVICES                             │
│  - ApifyService: Builds payload from template, runs actor       │
│  - FilterService: Applies filters based on config               │
│  - ClayService: Transforms output using template                │
│  - ScraperService: Orchestrates the workflow                    │
└─────────────────────────────────────────────────────────────────┘
```

## 🚀 Adding a New Actor (5 minutes)

### Step 1: Open `actor_registry.py`

### Step 2: Add your actor to `SEED_ACTORS`:

```python
SEED_ACTORS = {
    # ... existing actors ...
    
    "my_new_actor": {
        "display_name": "My New Job Scraper",
        "actor_id": "username~actor-name",  # From Apify
        "default_timeout_minutes": 15,
        "default_max_results": 100,
        
        # What inputs does the UI show?
        "input_schema": [
            {"name": "search_query", "type": "text", "label": "Search Query", "required": True},
            {"name": "location", "type": "text", "label": "Location", "default": "USA"},
            {"name": "max_results", "type": "number", "label": "Max Results", "default": 100},
        ],
        
        # How to build the Apify input payload
        "input_template": {
            "query": "{{search_query}}",
            "location": "{{location}}",
            "maxItems": "{{max_results}}"
        },
        
        # How to extract standardized fields from output
        "output_mapping": {
            "job_id": "id",
            "title": "jobTitle",
            "company_name": "company.name",
            "location": "location",
            "description": "description",
            "job_url": "url"
        },
        
        # What can be filtered?
        "filter_config": {
            "company_name_excludes": {"field": "company.name", "type": "exclude_contains"},
            "min_employees": {"field": "company.size", "type": "min_value"},
            "max_employees": {"field": "company.size", "type": "max_value"}
        },
        
        # What gets sent to Clay?
        "clay_template": {
            "ID": "{{job_id}}",
            "Job Title": "{{title}}",
            "Company": "{{company_name}}",
            "Location": "{{location}}",
            "Description": "{{description}}",
            "URL": "{{job_url}}"
        }
    }
}
```

### Step 3: Restart the app

That's it! The new actor will:
- Appear in the dropdown
- Generate dynamic form fields
- Work with filtering
- Send to Clay webhook

## Configuration Reference

### Input Schema Field Types

| Type | Description | Example |
|------|-------------|---------|
| `text` | Single line text | Search query |
| `textarea` | Multi-line text | URLs (one per line) |
| `url` | URL validation | Search URL |
| `number` | Numeric input | Max results |
| `boolean` | Checkbox | Enable feature |
| `select` | Dropdown | Time range |

### Input Schema Field Properties

```python
{
    "name": "field_name",        # Internal name
    "type": "text",              # Field type
    "label": "Display Label",    # UI label
    "required": True,            # Required?
    "default": "value",          # Default value
    "placeholder": "hint",       # Placeholder text
    "help": "Help text",         # Help text below field
    "options": [                 # For select type only
        {"value": "a", "label": "Option A"},
        {"value": "b", "label": "Option B"}
    ]
}
```

### Template Variables

In `input_template` and `clay_template`, use `{{variable_name}}` to reference:
- Actor inputs from the form
- Extracted fields from output mapping

**Special suffix `_array`**: Converts newline/comma text to array
```python
"titleSearch": "{{title_search_array}}"  # "a\nb\nc" -> ["a", "b", "c"]
```

### Output Mapping - Nested Fields

Use dot notation for nested fields:
```python
"output_mapping": {
    "company_name": "company.name",           # data["company"]["name"]
    "first_location": "locations[0]",         # data["locations"][0]
    "deep_field": "a.b[0].c.d"               # Complex nesting
}
```

### Filter Types

| Type | Description | Example Use |
|------|-------------|-------------|
| `exclude_contains` | Exclude if field contains any value | Company name blacklist |
| `include_contains` | Include only if field contains value | Industry whitelist |
| `min_value` | Exclude if field < value | Min employees |
| `max_value` | Exclude if field > value | Max employees |
| `equals` | Exclude if field != value | Exact match |
| `not_equals` | Exclude if field == value | Exclude exact value |

## File Structure

```
job-scraper-v2/
├── main.py              # FastAPI app & endpoints
├── models.py            # SQLAlchemy models
├── schemas.py           # Pydantic schemas
├── database.py          # Database config
├── actor_registry.py    # 👈 ADD NEW ACTORS HERE
├── template_engine.py   # Variable substitution
├── apify_service.py     # Generic Apify client
├── filter_service.py    # Generic filtering
├── clay_service.py      # Generic Clay webhook
├── scraper_service.py   # Orchestrator
├── templates/
│   └── index.html       # Dynamic UI
└── requirements.txt
```

## Deployment

### Environment Variables

```bash
DATABASE_URL=postgresql://user:pass@host:5432/dbname
APIFY_API_TOKEN=your_token
```

### Render Deployment

1. Push to GitHub
2. Create new Web Service on Render
3. Set environment variables
4. Deploy!

Database tables are created automatically on startup.

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/actors` | List all actors |
| GET | `/api/actors/{key}/input-schema` | Get actor's form schema |
| GET | `/api/searches` | List job searches |
| POST | `/api/searches` | Create job search |
| PUT | `/api/searches/{id}` | Update job search |
| DELETE | `/api/searches/{id}` | Delete job search |
| POST | `/api/searches/{id}/execute` | Run immediately |
| POST | `/api/searches/{id}/duplicate` | Duplicate search |
| GET | `/api/searches/{id}/runs` | Get run history |
| POST | `/api/test-actor` | Test actor config |

## Migration from v1

If you have existing data from v1, you'll need to:

1. Export your job search configurations
2. Deploy v2 (creates new schema)
3. Recreate searches via UI or API

The new schema is fundamentally different (dynamic JSON vs fixed columns), so direct migration isn't possible.

## Troubleshooting

### Actor not showing up?
- Check the actor_key is unique
- Restart the app (actors are seeded on startup)

### Fields not extracting correctly?
- Check output_mapping paths match the actual Apify output
- Use the test endpoint to preview raw data

### Filters not working?
- Check filter_config field paths are correct
- Ensure filter_rules in job search match filter_config keys
