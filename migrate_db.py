#!/usr/bin/env python3
"""
Database Migration Script
Add missing columns to production PostgreSQL database
"""

import os
import sys
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError

def migrate_database():
    """Add missing columns to production database"""
    
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        print("ERROR: DATABASE_URL environment variable not set")
        sys.exit(1)
    
    # Handle Render's postgres:// vs postgresql://
    if database_url.startswith("postgres://"):
        database_url = database_url.replace("postgres://", "postgresql://", 1)
    
    engine = create_engine(database_url)
    
    migrations = [
        # Add execution_logs column if it doesn't exist
        """
        ALTER TABLE job_runs 
        ADD COLUMN IF NOT EXISTS execution_logs TEXT;
        """,
        
        # Add apify_dataset_id column if it doesn't exist
        """
        ALTER TABLE job_runs 
        ADD COLUMN IF NOT EXISTS apify_dataset_id VARCHAR(255);
        """,
    ]
    
    try:
        with engine.connect() as conn:
            for i, migration in enumerate(migrations, 1):
                try:
                    print(f"Running migration {i}...")
                    conn.execute(text(migration))
                    conn.commit()
                    print(f"✅ Migration {i} completed successfully")
                except OperationalError as e:
                    if "already exists" in str(e) or "duplicate column" in str(e).lower():
                        print(f"⚠️  Migration {i} skipped (column already exists)")
                    else:
                        print(f"❌ Migration {i} failed: {e}")
                        raise
                except Exception as e:
                    print(f"❌ Migration {i} failed: {e}")
                    raise
        
        print("🎉 All migrations completed successfully!")
        
    except Exception as e:
        print(f"💥 Migration failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    migrate_database()