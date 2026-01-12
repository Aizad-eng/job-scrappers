"""
Database Compatibility Layer
Handles differences between local development and production schemas
"""

from sqlalchemy import text
from sqlalchemy.exc import OperationalError
from models import JobRun
from database import SessionLocal
import logging

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
        
    except OperationalError as e:
        if "execution_logs" in str(e) or "does not exist" in str(e):
            logger.warning(f"Falling back to safe JobRun creation: {e}")
            
            # Rollback the failed transaction
            db.rollback()
            
            # Use raw SQL insert without problematic columns
            insert_sql = text("""
                INSERT INTO job_runs 
                (job_search_id, started_at, completed_at, status, 
                 jobs_found, jobs_filtered, jobs_sent, error_message, apify_run_id)
                VALUES 
                (:job_search_id, :started_at, :completed_at, :status,
                 :jobs_found, :jobs_filtered, :jobs_sent, :error_message, :apify_run_id)
                RETURNING id
            """)
            
            # Prepare safe parameters (exclude problematic fields)
            safe_params = {
                'job_search_id': kwargs.get('job_search_id'),
                'started_at': kwargs.get('started_at'),
                'completed_at': kwargs.get('completed_at'),
                'status': kwargs.get('status', 'running'),
                'jobs_found': kwargs.get('jobs_found', 0),
                'jobs_filtered': kwargs.get('jobs_filtered', 0),
                'jobs_sent': kwargs.get('jobs_sent', 0),
                'error_message': kwargs.get('error_message'),
                'apify_run_id': kwargs.get('apify_run_id')
            }
            
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
        
    except OperationalError as e:
        if "execution_logs" in str(e) or "does not exist" in str(e):
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