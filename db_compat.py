"""
Database Compatibility Layer
Handles differences between local development and production schemas
"""

from sqlalchemy import text
from sqlalchemy.exc import OperationalError, ProgrammingError, DatabaseError
from models import JobRun
from database import SessionLocal
import logging

# Try to import psycopg2 errors for PostgreSQL
try:
    import psycopg2.errors
except ImportError:
    psycopg2 = None

logger = logging.getLogger(__name__)

def create_job_run_safe(db, **kwargs):
    """
    Create a JobRun object safely, handling missing columns in production.
    
    Falls back to raw SQL INSERT if SQLAlchemy fails due to missing columns.
    """
    try:
        # Try normal SQLAlchemy creation first
        job_run = JobRun(**kwargs)
        db.add(job_run)
        db.commit()
        db.refresh(job_run)
        return job_run
        
    except Exception as e:
        # Check if this is a column-related error
        error_str = str(e).lower()
        if any(term in error_str for term in ["execution_logs", "does not exist", "undefinedcolumn", "no such column"]):
            logger.warning(f"Falling back to safe JobRun creation: {e}")
            
            # Rollback the failed transaction
            db.rollback()
            
            # Check which columns actually exist in the table
            check_columns_sql = text("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'job_runs' 
                AND column_name IN ('execution_logs', 'apify_dataset_id')
            """)
            
            existing_columns = db.execute(check_columns_sql).fetchall()
            existing_column_names = [row[0] for row in existing_columns]
            
            # Build INSERT statement with only existing columns
            base_columns = ['job_search_id', 'started_at', 'completed_at', 'status', 
                           'jobs_found', 'jobs_filtered', 'jobs_sent', 'error_message', 'apify_run_id']
            
            # Add optional columns only if they exist
            all_columns = base_columns.copy()
            if 'apify_dataset_id' in existing_column_names:
                all_columns.append('apify_dataset_id')
            
            columns_str = ', '.join(all_columns)
            placeholders_str = ', '.join([f':{col}' for col in all_columns])
            
            insert_sql = text(f"""
                INSERT INTO job_runs ({columns_str})
                VALUES ({placeholders_str})
                RETURNING id
            """)
            
            # Prepare parameters for only the columns that exist
            from datetime import datetime
            
            safe_params = {}
            for col in all_columns:
                if col in kwargs and kwargs[col] is not None:
                    safe_params[col] = kwargs[col]
                elif col == 'status':
                    safe_params[col] = kwargs.get('status', 'running')
                elif col == 'started_at':
                    safe_params[col] = kwargs.get('started_at', datetime.utcnow())
                elif col in ['jobs_found', 'jobs_filtered', 'jobs_sent']:
                    safe_params[col] = kwargs.get(col, 0)
                else:
                    safe_params[col] = kwargs.get(col)
            
            result = db.execute(insert_sql, safe_params)
            run_id = result.fetchone()[0]
            db.commit()
            
            # Return a minimal JobRun-like object
            class JobRunCompat:
                def __init__(self, run_id, **params):
                    self.id = run_id
                    for key, value in params.items():
                        setattr(self, key, value)
            
            return JobRunCompat(run_id, **safe_params)
        else:
            raise
    
    except Exception as e:
        logger.error(f"Failed to create JobRun: {e}")
        raise


def update_job_run_safe(db, job_run_id, **updates):
    """
    Update a JobRun object safely, handling missing columns in production.
    """
    try:
        # Try normal SQLAlchemy update first
        job_run = db.query(JobRun).filter(JobRun.id == job_run_id).first()
        if job_run:
            for key, value in updates.items():
                if hasattr(job_run, key):
                    setattr(job_run, key, value)
            db.commit()
            return job_run
        
    except Exception as e:
        # Check if this is a column-related error
        error_str = str(e).lower()
        if any(term in error_str for term in ["execution_logs", "does not exist", "undefinedcolumn", "no such column"]):
            logger.warning(f"Falling back to safe JobRun update: {e}")
            
            # Rollback the failed transaction
            db.rollback()
            
            # Build safe UPDATE query excluding problematic columns
            safe_updates = {k: v for k, v in updates.items() 
                           if k not in ['execution_logs', 'apify_dataset_id']}
            
            if safe_updates:
                set_clause = ", ".join([f"{k} = :{k}" for k in safe_updates.keys()])
                update_sql = text(f"""
                    UPDATE job_runs 
                    SET {set_clause}
                    WHERE id = :job_run_id
                """)
                
                params = {**safe_updates, 'job_run_id': job_run_id}
                db.execute(update_sql, params)
                db.commit()
            
            return None  # Can't return updated object in fallback mode
        else:
            raise
    
    except Exception as e:
        logger.error(f"Failed to update JobRun {job_run_id}: {e}")
        raise