import re
from typing import Dict, List, Optional
import logging

logger = logging.getLogger(__name__)


class JobFilter:
    """Filter jobs based on configured criteria"""
    
    def __init__(
        self,
        company_name_excludes: List[str],
        industries_excludes: List[str],
        max_employee_count: Optional[int] = None,
        min_employee_count: Optional[int] = None
    ):
        self.company_name_excludes = company_name_excludes or []
        self.industries_excludes = industries_excludes or []
        self.max_employee_count = max_employee_count
        self.min_employee_count = min_employee_count
    
    def _matches_exclusion_pattern(self, text: str, patterns: List[str]) -> bool:
        """Check if text matches any exclusion pattern (case-insensitive)"""
        if not text or not patterns:
            return False
        
        text_lower = text.lower()
        
        for pattern in patterns:
            if pattern.lower() in text_lower:
                return True
        
        return False
    
    def should_filter_out(self, job: Dict) -> tuple[bool, Optional[str]]:
        """
        Determine if a job should be filtered out
        
        Returns:
            (should_filter: bool, reason: str)
        """
        # Check company name exclusions
        company_name = job.get("companyName", "")
        if self._matches_exclusion_pattern(company_name, self.company_name_excludes):
            return True, f"Company name matches exclusion: {company_name}"
        
        # Check industry exclusions
        industries = job.get("industries", "")
        if self._matches_exclusion_pattern(industries, self.industries_excludes):
            return True, f"Industry matches exclusion: {industries}"
        
        # Check employee count range
        employee_count = job.get("companyEmployeesCount")
        if employee_count is not None:
            if self.min_employee_count is not None and employee_count < self.min_employee_count:
                return True, f"Employee count too low: {employee_count} < {self.min_employee_count}"
            
            if self.max_employee_count is not None and employee_count >= self.max_employee_count:
                return True, f"Employee count too high: {employee_count} >= {self.max_employee_count}"
        
        return False, None
    
    def filter_jobs(self, jobs: List[Dict]) -> tuple[List[Dict], List[Dict]]:
        """
        Filter a list of jobs
        
        Returns:
            (passed_jobs: List[Dict], filtered_jobs: List[Dict with filter_reason])
        """
        passed = []
        filtered = []
        
        for job in jobs:
            should_filter, reason = self.should_filter_out(job)
            
            if should_filter:
                job["filter_reason"] = reason
                filtered.append(job)
                logger.debug(f"Filtered job '{job.get('title')}': {reason}")
            else:
                passed.append(job)
        
        logger.info(f"Filtered {len(filtered)} jobs, {len(passed)} passed")
        return passed, filtered
