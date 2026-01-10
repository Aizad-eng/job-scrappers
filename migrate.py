"""
Migration script to import existing Google Sheets data into the new system
"""
import csv
import requests
import sys
from datetime import datetime


def migrate_from_csv(csv_file_path: str, api_base_url: str):
    """
    Migrate data from CSV export of Google Sheets
    
    Args:
        csv_file_path: Path to exported CSV file
        api_base_url: Base URL of deployed API (e.g., https://your-app.onrender.com)
    """
    
    created_count = 0
    failed_count = 0
    
    with open(csv_file_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        
        for row in reader:
            try:
                # Map CSV columns to API schema
                payload = {
                    "name": row.get('Keyword', 'Imported Job Search'),
                    "keyword": row.get('Keyword', ''),
                    "platform": "linkedin",
                    "search_url": row.get('URL', ''),
                    "cron_schedule": "0 9 * * *",  # Default to 9 AM daily
                    
                    # Apify config
                    "apify_actor_id": "curious_coder~linkedin-jobs-scraper",
                    "apify_token": row.get('api_token', ''),
                    "max_results": int(row.get('Max_result', 1000)) if row.get('Max_result') else 1000,
                    
                    # Filtering (use defaults if not specified)
                    "company_name_excludes": ["staff", "recruit", "talent", "hire", "job", "search", "recruitment"],
                    "industries_excludes": ["staffing", "staffing and recruiting", "human", "non-profit"],
                    "max_employee_count": 300,
                    
                    # Output
                    "clay_webhook_url": row.get('webhook_url', ''),
                    "batch_size": 8,
                    "batch_interval_ms": 2000,
                    
                    # Status - check if was previously successful
                    "is_active": "Successful" in row.get('Daily Status', '')
                }
                
                # Validate required fields
                if not payload['search_url'] or not payload['clay_webhook_url']:
                    print(f"⚠️  Skipping row with missing URL or webhook: {payload['keyword']}")
                    failed_count += 1
                    continue
                
                # Create via API
                response = requests.post(
                    f"{api_base_url}/api/job-searches",
                    json=payload,
                    timeout=10
                )
                
                if response.status_code == 201:
                    result = response.json()
                    print(f"✅ Created: {result['name']} (ID: {result['id']})")
                    created_count += 1
                else:
                    print(f"❌ Failed to create '{payload['name']}': {response.status_code} - {response.text}")
                    failed_count += 1
            
            except Exception as e:
                print(f"❌ Error processing row: {e}")
                failed_count += 1
    
    print(f"\n{'='*50}")
    print(f"Migration complete!")
    print(f"✅ Successfully created: {created_count}")
    print(f"❌ Failed: {failed_count}")
    print(f"{'='*50}")


def migrate_sample_data(api_base_url: str):
    """Create sample job search for testing"""
    
    sample_job = {
        "name": "DevOps Engineer - US (Sample)",
        "keyword": "DevOps Engineer",
        "platform": "linkedin",
        "search_url": "https://www.linkedin.com/jobs/search/?keywords=DevOps%20Engineer&location=United%20States",
        "cron_schedule": "0 9 * * *",
        "apify_actor_id": "curious_coder~linkedin-jobs-scraper",
        "apify_token": "YOUR_APIFY_TOKEN_HERE",  # Replace with actual token
        "max_results": 100,
        "company_name_excludes": ["staff", "recruit", "talent"],
        "industries_excludes": ["staffing", "non-profit"],
        "max_employee_count": 300,
        "clay_webhook_url": "https://api.clay.com/v3/sources/webhook/YOUR_WEBHOOK_ID",  # Replace
        "batch_size": 8,
        "batch_interval_ms": 2000,
        "is_active": False  # Set to False for testing
    }
    
    try:
        response = requests.post(
            f"{api_base_url}/api/job-searches",
            json=sample_job,
            timeout=10
        )
        
        if response.status_code == 201:
            result = response.json()
            print(f"✅ Sample job created successfully!")
            print(f"ID: {result['id']}")
            print(f"Name: {result['name']}")
            print(f"\nUpdate the apify_token and clay_webhook_url, then set is_active to true")
        else:
            print(f"❌ Failed to create sample: {response.status_code} - {response.text}")
    
    except Exception as e:
        print(f"❌ Error: {e}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage:")
        print("  Create sample: python migrate.py sample <api-base-url>")
        print("  Migrate CSV:   python migrate.py csv <csv-file-path> <api-base-url>")
        print("\nExample:")
        print("  python migrate.py sample https://your-app.onrender.com")
        print("  python migrate.py csv data.csv https://your-app.onrender.com")
        sys.exit(1)
    
    mode = sys.argv[1]
    
    if mode == "sample":
        if len(sys.argv) < 3:
            print("Error: Please provide API base URL")
            sys.exit(1)
        
        api_url = sys.argv[2].rstrip('/')
        print(f"Creating sample job search at {api_url}")
        migrate_sample_data(api_url)
    
    elif mode == "csv":
        if len(sys.argv) < 4:
            print("Error: Please provide CSV file path and API base URL")
            sys.exit(1)
        
        csv_path = sys.argv[2]
        api_url = sys.argv[3].rstrip('/')
        
        print(f"Migrating from {csv_path} to {api_url}")
        migrate_from_csv(csv_path, api_url)
    
    else:
        print(f"Unknown mode: {mode}")
        print("Use 'sample' or 'csv'")
        sys.exit(1)
