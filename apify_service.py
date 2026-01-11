import httpx
import asyncio
import re
from typing import Dict, List, Optional, Union
from datetime import datetime, timedelta
from config import settings
from tenacity import retry, stop_after_attempt, wait_exponential
import logging

logger = logging.getLogger(__name__)


# Platform-specific configurations - NO MANUAL CONFIG NEEDED
PLATFORM_CONFIG = {
    "linkedin": {
        "actor_id": "curious_coder~linkedin-jobs-scraper",
        "default_count": 1000,
        "min_count": 100,
        "use_browser": False,
        "use_apify_proxy": True,
        "max_wait_minutes": 15,
    },
    "indeed": {
        "actor_id": "curious_coder~indeed-scraper",
        "default_count": 200,
        "min_count": 1,
        "use_browser": True,
        "use_apify_proxy": True,
        "max_wait_minutes": 35,  # Indeed takes longer
    }
}


class ApifyService:
    BASE_URL = "https://api.apify.com/v2"
    
    def __init__(self, api_token: str):
        self.api_token = api_token
        self.client = httpx.AsyncClient(timeout=60.0)
    
    def get_platform_config(self, platform: str) -> Dict:
        """Get platform-specific configuration"""
        return PLATFORM_CONFIG.get(platform, PLATFORM_CONFIG["linkedin"])
    
    async def start_actor_run(
        self,
        platform: str,
        urls: Union[str, List[str]],
        count: int = None,
        scrape_company: bool = True,
        actor_id: str = None,  # Optional override
        use_browser: bool = None,  # Optional override
        use_apify_proxy: bool = None,  # Optional override
    ) -> Dict:
        """
        Start an Apify actor run - auto-configures based on platform
        
        Args:
            platform: 'linkedin' or 'indeed' - determines actor and settings
            urls: Single URL string, comma-separated URL string, or list of URLs
            count: Max results (uses platform default if not specified)
            scrape_company: Whether to scrape company details
            actor_id: Optional override for actor ID
            use_browser: Optional override for browser mode
            use_apify_proxy: Optional override for proxy setting
        
        Returns:
            Dict with run information including 'id' and 'defaultDatasetId'
        """
        # Get platform config
        config = self.get_platform_config(platform)
        
        # Use platform defaults unless overridden
        actor_id = actor_id or config["actor_id"]
        count = count or config["default_count"]
        use_browser = use_browser if use_browser is not None else config["use_browser"]
        use_apify_proxy = use_apify_proxy if use_apify_proxy is not None else config["use_apify_proxy"]
        
        # Validate count
        if count < config["min_count"]:
            logger.warning(f"Count {count} is too low for {platform}, using minimum {config['min_count']}")
            count = config["min_count"]
        
        # Process URLs - handle all input formats
        urls_list = self._normalize_urls(urls)
        
        logger.info(f"Starting {platform} scrape with actor {actor_id}")
        logger.info(f"Processing {len(urls_list)} URL(s), count={count}, browser={use_browser}")
        
        # Build API URL
        api_url = f"{self.BASE_URL}/acts/{actor_id}/runs"
        
        # Build payload based on platform
        if platform == "indeed":
            payload = {
                "count": count,
                "proxy": {
                    "useApifyProxy": use_apify_proxy,
                    "apifyProxyGroups": []
                },
                "scrapeJobs.scrapeCompany": scrape_company,
                "scrapeJobs.searchUrl": urls_list[0] if urls_list else "",
                "startPage": 1,
                "useBrowser": use_browser
            }
        else:  # linkedin (default)
            payload = {
                "count": count,
                "scrapeCompany": scrape_company,
                "urls": urls_list
            }
        
        params = {"token": self.api_token}
        
        try:
            logger.info(f"Sending {platform} payload to Apify: {payload}")
            response = await self.client.post(api_url, params=params, json=payload)
            
            if response.status_code != 201:
                logger.error(f"Apify error response: {response.text}")
            
            response.raise_for_status()
            result = response.json()
            
            logger.info(f"Started {platform} Apify actor run: {result['data']['id']}")
            return result["data"]
        except httpx.HTTPError as e:
            logger.error(f"Failed to start Apify actor: {e}")
            logger.error(f"Response: {e.response.text if hasattr(e, 'response') else 'No response'}")
            raise
    
    def _normalize_urls(self, urls: Union[str, List[str]]) -> List[str]:
        """
        Normalize URLs from various input formats to a clean list.
        
        Handles:
        - Single URL string
        - Comma-separated URL string
        - List of URLs
        - List containing comma-separated URL strings (common bug case)
        
        Returns:
            List of individual URL strings, properly trimmed
        """
        urls_list = []
        
        if isinstance(urls, str):
            # Single string - might be comma-separated
            urls_list = self._split_comma_urls(urls)
        elif isinstance(urls, list):
            # List - check each element for comma-separated URLs
            for item in urls:
                if isinstance(item, str):
                    urls_list.extend(self._split_comma_urls(item))
                else:
                    urls_list.append(item)
        else:
            urls_list = [urls] if urls else []
        
        # Final cleanup - remove empty strings and whitespace
        urls_list = [url.strip() for url in urls_list if url and url.strip()]
        
        logger.debug(f"Normalized {len(urls_list)} URLs from input")
        return urls_list
    
    def _split_comma_urls(self, url_string: str) -> List[str]:
        """
        Split a string that may contain comma-separated URLs.
        
        Handles the tricky case where URLs contain commas in query params.
        We split on ', http' or ',http' patterns to be safe.
        """
        if not url_string or not url_string.strip():
            return []
        
        url_string = url_string.strip()
        
        # Split on comma followed by optional space followed by http(s)://
        # This pattern preserves the http(s):// in the result
        parts = re.split(r',\s*(?=https?://)', url_string)
        
        # Clean up each URL
        result = [part.strip() for part in parts if part.strip()]
        
        return result
    
    async def get_run_status(self, run_id: str) -> Dict:
        """
        Get the status of an Apify actor run
        
        Returns:
            Dict with status information
        """
        url = f"{self.BASE_URL}/actor-runs/{run_id}"
        params = {"token": self.api_token}
        
        try:
            response = await self.client.get(url, params=params)
            response.raise_for_status()
            result = response.json()
            return result["data"]
        except httpx.HTTPError as e:
            logger.error(f"Failed to get run status: {e}")
            raise
    
    async def wait_for_completion(
        self,
        run_id: str,
        platform: str = "linkedin",
        poll_interval: int = None,
        max_wait_minutes: int = None
    ) -> Dict:
        """
        Poll the actor run until it completes or times out
        
        Args:
            run_id: The Apify run ID
            platform: Platform to get default timeout
            poll_interval: Seconds between polls
            max_wait_minutes: Max wait time (uses platform default if not specified)
        
        Returns:
            Final run status
        """
        config = self.get_platform_config(platform)
        
        poll_interval = poll_interval or settings.APIFY_POLL_INTERVAL_SECONDS
        max_wait_minutes = max_wait_minutes or config["max_wait_minutes"]
        
        start_time = datetime.utcnow()
        timeout = timedelta(minutes=max_wait_minutes)
        
        logger.info(f"Waiting for {platform} Apify run {run_id} to complete (timeout: {max_wait_minutes}m)")
        
        while True:
            # Check timeout
            if datetime.utcnow() - start_time > timeout:
                logger.error(f"Apify run {run_id} timed out after {max_wait_minutes} minutes")
                raise TimeoutError(f"Actor run timed out after {max_wait_minutes} minutes")
            
            # Get status
            run_status = await self.get_run_status(run_id)
            status = run_status.get("status")
            
            logger.info(f"Apify run {run_id} status: {status}")
            
            # Check if completed
            if status in ["SUCCEEDED", "FAILED", "ABORTED", "TIMED-OUT"]:
                return run_status
            
            # Wait before next poll
            await asyncio.sleep(poll_interval)
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    async def get_dataset_items(self, dataset_id: str) -> List[Dict]:
        """
        Fetch items from an Apify dataset
        
        Returns:
            List of scraped job items
        """
        url = f"{self.BASE_URL}/datasets/{dataset_id}/items"
        params = {"token": self.api_token}
        
        try:
            response = await self.client.get(url, params=params)
            response.raise_for_status()
            items = response.json()
            
            logger.info(f"Fetched {len(items)} items from dataset {dataset_id}")
            return items
        except httpx.HTTPError as e:
            logger.error(f"Failed to fetch dataset items: {e}")
            raise
    
    async def close(self):
        """Close the HTTP client"""
        await self.client.aclose()
