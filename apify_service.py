import httpx
import asyncio
from typing import Dict, List, Optional
from datetime import datetime, timedelta
from config import settings
from tenacity import retry, stop_after_attempt, wait_exponential
import logging

logger = logging.getLogger(__name__)


class ApifyService:
    BASE_URL = "https://api.apify.com/v2"
    
    def __init__(self, api_token: str):
        self.api_token = api_token
        self.client = httpx.AsyncClient(timeout=60.0)
    
    async def start_actor_run(
        self,
        actor_id: str,
        platform: str,
        urls: List[str],
        count: int = 1000,
        scrape_company: bool = True,
        use_browser: bool = False,
        use_apify_proxy: bool = True
    ) -> Dict:
        """
        Start an Apify actor run for LinkedIn or Indeed
        
        Args:
            actor_id: The Apify actor ID
            platform: 'linkedin' or 'indeed'
            urls: List of search URLs
            count: Max results
            scrape_company: Whether to scrape company details
            use_browser: For Indeed - use browser mode
            use_apify_proxy: For Indeed - use Apify proxy
        
        Returns:
            Dict with run information including 'id' and 'defaultDatasetId'
        """
        url = f"{self.BASE_URL}/acts/{actor_id}/runs"
        
        # Build payload based on platform
        if platform == "indeed":
            payload = {
                "count": count,
                "proxy": {
                    "useApifyProxy": use_apify_proxy,
                    "apifyProxyGroups": []
                },
                "scrapeJobs.scrapeCompany": scrape_company,
                "scrapeJobs.searchUrl": urls[0] if urls else "",
                "startPage": 1,
                "useBrowser": use_browser
            }
        else:  # linkedin (default)
            payload = {
                "count": count,
                "scrapeCompany": scrape_company,
                "urls": urls
            }
        
        params = {"token": self.api_token}
        
        try:
            response = await self.client.post(url, params=params, json=payload)
            response.raise_for_status()
            result = response.json()
            
            logger.info(f"Started {platform} Apify actor run: {result['data']['id']}")
            return result["data"]
        except httpx.HTTPError as e:
            logger.error(f"Failed to start Apify actor: {e}")
            raise
    
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
        poll_interval: int = None,
        max_wait_minutes: int = None
    ) -> Dict:
        """
        Poll the actor run until it completes or times out
        
        Returns:
            Final run status
        """
        poll_interval = poll_interval or settings.APIFY_POLL_INTERVAL_SECONDS
        max_wait_minutes = max_wait_minutes or settings.APIFY_MAX_WAIT_MINUTES
        
        start_time = datetime.utcnow()
        timeout = timedelta(minutes=max_wait_minutes)
        
        logger.info(f"Waiting for Apify run {run_id} to complete (timeout: {max_wait_minutes}m)")
        
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
