"""
Run Logger - Captures execution logs for individual job runs.
"""

import logging
import threading
from typing import Dict, List
from datetime import datetime


class RunLogHandler(logging.Handler):
    """Custom logging handler that captures logs for specific job runs"""
    
    def __init__(self):
        super().__init__()
        self._logs: Dict[int, List[str]] = {}
        self._lock = threading.Lock()
        
        # Format logs nicely
        self.setFormatter(logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%H:%M:%S'
        ))
    
    def start_run(self, run_id: int):
        """Start capturing logs for a specific run"""
        with self._lock:
            self._logs[run_id] = []
    
    def emit(self, record):
        """Capture log record if it's for a tracked run"""
        # Only capture logs from scraper_service and related modules
        if not record.name.startswith(('scraper_service', 'apify_service', 'clay_service', 'filter_service')):
            return
        
        # Get current run ID from thread local storage
        run_id = getattr(threading.current_thread(), 'run_id', None)
        if run_id is None:
            return
        
        with self._lock:
            if run_id in self._logs:
                formatted = self.format(record)
                self._logs[run_id].append(formatted)
    
    def get_logs(self, run_id: int) -> List[str]:
        """Get all logs for a specific run"""
        with self._lock:
            return self._logs.get(run_id, []).copy()
    
    def finish_run(self, run_id: int) -> str:
        """Finish capturing logs and return them as a string"""
        with self._lock:
            logs = self._logs.get(run_id, [])
            if run_id in self._logs:
                del self._logs[run_id]
            return '\n'.join(logs)


# Global instance
run_log_handler = RunLogHandler()

# Add to root logger
logging.getLogger().addHandler(run_log_handler)


def start_run_logging(run_id: int):
    """Start capturing logs for a job run"""
    run_log_handler.start_run(run_id)
    threading.current_thread().run_id = run_id


def finish_run_logging(run_id: int) -> str:
    """Finish capturing logs and return them"""
    # Clear thread local
    if hasattr(threading.current_thread(), 'run_id'):
        delattr(threading.current_thread(), 'run_id')
    
    return run_log_handler.finish_run(run_id)