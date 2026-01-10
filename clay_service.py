import httpx
import asyncio
from typing import Dict, List
from tenacity import retry, stop_after_attempt, wait_exponential
import logging

logger = logging.getLogger(__name__)


class ClayWebhookService:
    """Service to send job data to Clay webhook in batches"""
    
    def __init__(
        self,
        webhook_url: str,
        batch_size: int = 8,
        batch_interval_ms: int = 2000
    ):
        self.webhook_url = webhook_url
        self.batch_size = batch_size
        self.batch_interval_seconds = batch_interval_ms / 1000.0
        self.client = httpx.AsyncClient(timeout=30.0)
    
    def _prepare_job_payload(self, job: Dict, keyword: str, url_scraped: str) -> Dict:
        """Transform job data to Clay webhook format"""
        # Clean URLs (remove query parameters)
        linkedin_url = job.get("companyLinkedinUrl", "")
        if linkedin_url and "?" in linkedin_url:
            linkedin_url = linkedin_url.split("?")[0]
        
        job_url = job.get("link", "")
        if job_url and "?" in job_url:
            job_url = job_url.split("?")[0]
        
        payload = {
            "Job Title": job.get("title", ""),
            "ID": job.get("id", ""),
            "Company name": job.get("companyName", ""),
            "Company website": job.get("companyWebsite", ""),
            "Linkedin URL": linkedin_url,
            "Industry": job.get("industries", ""),
            "Location": job.get("location", ""),
            "Employee Count": job.get("companyEmployeesCount", ""),
            "Apply URL": job.get("applyUrl", ""),
            "JOB URL": job_url,
            "Company description": job.get("companyDescription", ""),
            "Posted at": job.get("postedAt", ""),
            "Job Description": job.get("descriptionText", ""),
            "keyword": keyword,
            "url_scrapped": url_scraped
        }
        
        return payload
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    async def _send_batch(self, batch: List[Dict]) -> bool:
        """Send a batch of jobs to Clay webhook"""
        try:
            response = await self.client.post(self.webhook_url, json=batch)
            response.raise_for_status()
            
            logger.info(f"Successfully sent batch of {len(batch)} jobs to Clay")
            return True
        except httpx.HTTPError as e:
            logger.error(f"Failed to send batch to Clay: {e}")
            raise
    
    async def send_jobs(
        self,
        jobs: List[Dict],
        keyword: str,
        url_scraped: str
    ) -> int:
        """
        Send jobs to Clay webhook in batches
        
        Returns:
            Number of jobs successfully sent
        """
        if not jobs:
            logger.info("No jobs to send to Clay")
            return 0
        
        # Prepare all payloads
        payloads = [
            self._prepare_job_payload(job, keyword, url_scraped)
            for job in jobs
        ]
        
        # Split into batches
        batches = [
            payloads[i:i + self.batch_size]
            for i in range(0, len(payloads), self.batch_size)
        ]
        
        logger.info(f"Sending {len(payloads)} jobs in {len(batches)} batches to Clay")
        
        sent_count = 0
        
        for i, batch in enumerate(batches):
            try:
                await self._send_batch(batch)
                sent_count += len(batch)
                
                # Wait between batches (except for the last one)
                if i < len(batches) - 1:
                    await asyncio.sleep(self.batch_interval_seconds)
            
            except Exception as e:
                logger.error(f"Failed to send batch {i + 1}/{len(batches)}: {e}")
                # Continue with next batch even if one fails
        
        logger.info(f"Sent {sent_count}/{len(payloads)} jobs to Clay")
        return sent_count
    
    async def close(self):
        """Close the HTTP client"""
        await self.client.aclose()
