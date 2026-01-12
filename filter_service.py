"""
Generic Filter Service - Applies filters based on actor config and user rules.

Filter types supported:
- exclude_contains: Exclude if field contains any of the values
- include_contains: Include only if field contains any of the values
- min_value: Exclude if field < value
- max_value: Exclude if field > value
- equals: Exclude if field doesn't equal value
- not_equals: Exclude if field equals value
"""

import logging
from typing import Dict, List, Any, Tuple, Optional

from models import ActorConfig, JobSearch
from template_engine import get_nested_value

logger = logging.getLogger(__name__)


class FilterService:
    """Generic filter service that works with any actor based on config"""
    
    def filter_jobs(
        self,
        jobs: List[Dict],
        actor_config: ActorConfig,
        job_search: JobSearch
    ) -> Tuple[List[Dict], List[Tuple[Dict, str]]]:
        """
        Filter jobs based on actor's filter_config and user's filter_rules.
        
        Returns: (passed_jobs, [(filtered_job, reason), ...])
        """
        filter_config = actor_config.filter_config or {}
        filter_rules = job_search.filter_rules or {}
        
        passed = []
        filtered = []
        
        for job in jobs:
            should_filter, reason = self._should_filter(job, filter_config, filter_rules)
            
            if should_filter:
                filtered.append((job, reason))
            else:
                passed.append(job)
        
        logger.info(f"Filtered {len(filtered)} jobs, {len(passed)} passed")
        return passed, filtered
    
    def _should_filter(
        self,
        job: Dict,
        filter_config: Dict,
        filter_rules: Dict
    ) -> Tuple[bool, Optional[str]]:
        """
        Check if a job should be filtered out.
        
        filter_config: Defines available filters and their field mappings
        filter_rules: User's specific filter values
        """
        
        for rule_name, rule_value in filter_rules.items():
            # Skip empty rules
            if not rule_value:
                continue
            
            # Get the filter configuration for this rule
            config = filter_config.get(rule_name)
            
            if not config:
                # Rule not defined in config, skip
                continue
            
            field_path = config.get("field")
            filter_type = config.get("type")
            
            if not field_path or not filter_type:
                continue
            
            # Get the actual value from the job
            job_value = get_nested_value(job, field_path)
            
            # Apply the filter
            should_filter, reason = self._apply_filter(
                filter_type, job_value, rule_value, rule_name, field_path
            )
            
            if should_filter:
                return True, reason
        
        return False, None
    
    def _apply_filter(
        self,
        filter_type: str,
        job_value: Any,
        rule_value: Any,
        rule_name: str,
        field_path: str
    ) -> Tuple[bool, Optional[str]]:
        """Apply a specific filter type"""
        
        # Handle exclude_contains (most common for company name, industry)
        if filter_type == "exclude_contains":
            return self._filter_exclude_contains(job_value, rule_value, rule_name)
        
        # Handle include_contains
        if filter_type == "include_contains":
            return self._filter_include_contains(job_value, rule_value, rule_name)
        
        # Handle min_value
        if filter_type == "min_value":
            return self._filter_min_value(job_value, rule_value, rule_name)
        
        # Handle max_value
        if filter_type == "max_value":
            return self._filter_max_value(job_value, rule_value, rule_name)
        
        # Handle equals
        if filter_type == "equals":
            return self._filter_equals(job_value, rule_value, rule_name)
        
        # Handle not_equals
        if filter_type == "not_equals":
            return self._filter_not_equals(job_value, rule_value, rule_name)
        
        return False, None
    
    def _filter_exclude_contains(
        self,
        job_value: Any,
        exclude_list: Any,
        rule_name: str
    ) -> Tuple[bool, Optional[str]]:
        """Filter if job_value contains any of the exclude terms"""
        
        if not job_value:
            return False, None
        
        # Normalize to list
        if isinstance(exclude_list, str):
            exclude_list = [t.strip().lower() for t in exclude_list.split(',') if t.strip()]
        elif isinstance(exclude_list, list):
            exclude_list = [str(t).strip().lower() for t in exclude_list if t]
        else:
            return False, None
        
        # Normalize job value to string for comparison
        job_str = str(job_value).lower()
        
        for term in exclude_list:
            if term in job_str:
                return True, f"{rule_name}: contains '{term}'"
        
        return False, None
    
    def _filter_include_contains(
        self,
        job_value: Any,
        include_list: Any,
        rule_name: str
    ) -> Tuple[bool, Optional[str]]:
        """Filter if job_value does NOT contain any of the include terms"""
        
        if not job_value:
            return True, f"{rule_name}: empty value"
        
        # Normalize to list
        if isinstance(include_list, str):
            include_list = [t.strip().lower() for t in include_list.split(',') if t.strip()]
        elif isinstance(include_list, list):
            include_list = [str(t).strip().lower() for t in include_list if t]
        else:
            return False, None
        
        job_str = str(job_value).lower()
        
        for term in include_list:
            if term in job_str:
                return False, None  # Found a match, don't filter
        
        return True, f"{rule_name}: doesn't contain required terms"
    
    def _filter_min_value(
        self,
        job_value: Any,
        min_val: Any,
        rule_name: str
    ) -> Tuple[bool, Optional[str]]:
        """Filter if job_value < min_val"""
        
        try:
            job_num = self._to_number(job_value)
            min_num = self._to_number(min_val)
            
            if job_num is None:
                return False, None  # Can't compare, don't filter
            
            if job_num < min_num:
                return True, f"{rule_name}: {job_num} < {min_num}"
        except (ValueError, TypeError):
            pass
        
        return False, None
    
    def _filter_max_value(
        self,
        job_value: Any,
        max_val: Any,
        rule_name: str
    ) -> Tuple[bool, Optional[str]]:
        """Filter if job_value > max_val"""
        
        try:
            job_num = self._to_number(job_value)
            max_num = self._to_number(max_val)
            
            if job_num is None:
                return False, None  # Can't compare, don't filter
            
            if job_num > max_num:
                return True, f"{rule_name}: {job_num} > {max_num}"
        except (ValueError, TypeError):
            pass
        
        return False, None
    
    def _filter_equals(
        self,
        job_value: Any,
        expected: Any,
        rule_name: str
    ) -> Tuple[bool, Optional[str]]:
        """Filter if job_value != expected"""
        
        if str(job_value).lower().strip() != str(expected).lower().strip():
            return True, f"{rule_name}: '{job_value}' != '{expected}'"
        
        return False, None
    
    def _filter_not_equals(
        self,
        job_value: Any,
        not_expected: Any,
        rule_name: str
    ) -> Tuple[bool, Optional[str]]:
        """Filter if job_value == not_expected"""
        
        if str(job_value).lower().strip() == str(not_expected).lower().strip():
            return True, f"{rule_name}: equals '{not_expected}'"
        
        return False, None
    
    def _to_number(self, value: Any) -> Optional[float]:
        """Convert value to number, handling various formats"""
        
        if value is None:
            return None
        
        if isinstance(value, (int, float)):
            return float(value)
        
        if isinstance(value, str):
            # Remove common formatting
            cleaned = value.replace(',', '').replace(' ', '').strip()
            
            # Handle ranges like "100-500" - take the first number
            if '-' in cleaned:
                cleaned = cleaned.split('-')[0]
            
            # Handle "1000+" format
            cleaned = cleaned.rstrip('+')
            
            try:
                return float(cleaned)
            except ValueError:
                return None
        
        return None
