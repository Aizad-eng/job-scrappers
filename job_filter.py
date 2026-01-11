import re
from typing import Dict, List, Optional, Tuple
import logging

logger = logging.getLogger(__name__)


class JobFilter:
    """Filter jobs based on configured criteria - supports LinkedIn and Indeed"""
    
    def __init__(
        self,
        platform: str,
        company_name_excludes: List[str],
        industries_excludes: List[str],
        max_employee_count: Optional[int] = None,
        min_employee_count: Optional[int] = None,
        job_description_filters: Optional[str] = None
    ):
        self.platform = platform
        self.company_name_excludes = company_name_excludes or []
        self.industries_excludes = industries_excludes or []
        self.max_employee_count = max_employee_count
        self.min_employee_count = min_employee_count
        self.job_description_filters = job_description_filters
    
    def _matches_exclusion_pattern(self, text: str, patterns: List[str]) -> bool:
        """Check if text matches any exclusion pattern (case-insensitive)"""
        if not text or not patterns:
            return False
        
        text_lower = text.lower()
        
        for pattern in patterns:
            if pattern.lower() in text_lower:
                return True
        
        return False
    
    def _matches_regex(self, text: str, pattern: str) -> bool:
        """Check if text matches regex pattern"""
        if not text or not pattern:
            return False
        
        try:
            return bool(re.search(pattern, text, re.IGNORECASE))
        except re.error:
            logger.error(f"Invalid regex pattern: {pattern}")
            return False
    
    def _clean_html(self, text: str) -> str:
        """Clean HTML tags from text"""
        if not text:
            return ""
        
        cleaned = re.sub(r'<[^>]+>', '', text)
        cleaned = cleaned.replace('&amp;', '&')
        cleaned = cleaned.replace('&lt;', '<')
        cleaned = cleaned.replace('&gt;', '>')
        cleaned = cleaned.replace('&quot;', '"')
        cleaned = cleaned.replace('&#039;', "'")
        cleaned = re.sub(r'\s+', ' ', cleaned).strip()
        
        return cleaned
    
    def _should_filter_linkedin(self, job: Dict) -> Tuple[bool, Optional[str]]:
        """Filter logic for LinkedIn jobs"""
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
    
    def _should_filter_indeed(self, job: Dict) -> Tuple[bool, Optional[str]]:
        """Filter logic for Indeed jobs"""
        # Check company name exclusions
        company_name = job.get("company", "")
        if self._matches_exclusion_pattern(company_name, self.company_name_excludes):
            return True, f"Company name matches exclusion: {company_name}"
        
        # Check industry exclusions (in nested structure)
        company_details = job.get("companyDetails", {})
        about_section = company_details.get("aboutSectionViewModel", {})
        about_company = about_section.get("aboutCompany", {})
        industry = about_company.get("industry", "")
        
        if self._matches_exclusion_pattern(industry, self.industries_excludes):
            return True, f"Industry matches exclusion: {industry}"
        
        # Check job description filters (Indeed-specific regex matching)
        if self.job_description_filters:
            # Clean and check job description
            job_desc = self._clean_html(job.get("jobDescription", ""))
            if self._matches_regex(job_desc, self.job_description_filters):
                # If description matches, also check company description
                company_desc = about_company.get("description", "")
                if self._matches_regex(company_desc, self.job_description_filters):
                    # Both match - keep the job (pass filter)
                    return False, None
                else:
                    # Only job desc matches - could go either way
                    # For now, keep it if job desc matches
                    return False, None
            else:
                # Description doesn't match the required pattern
                return True, "Job description doesn't match required filters"
        
        return False, None
    
    def should_filter_out(self, job: Dict) -> Tuple[bool, Optional[str]]:
        """
        Determine if a job should be filtered out
        
        Returns:
            (should_filter: bool, reason: str)
        """
        if self.platform == "indeed":
            return self._should_filter_indeed(job)
        else:  # linkedin (default)
            return self._should_filter_linkedin(job)
    
    def filter_jobs(self, jobs: List[Dict]) -> Tuple[List[Dict], List[Dict]]:
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
                logger.debug(f"Filtered {self.platform} job '{job.get('title') or job.get('displayTitle')}': {reason}")
            else:
                passed.append(job)
        
        logger.info(f"Filtered {len(filtered)} {self.platform} jobs, {len(passed)} passed")
        return passed, filtered
