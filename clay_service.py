import httpx
import asyncio
import re
from typing import Dict, List
from tenacity import retry, stop_after_attempt, wait_exponential
import logging

logger = logging.getLogger(__name__)


class ClayWebhookService:
    """Service to send job data to Clay webhook - ONE JOB AT A TIME"""
    
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
    
    def _clean_html(self, text: str, max_length: int = 8000) -> str:
        """Clean HTML and truncate text"""
        if not text:
            return ""
        
        # Remove HTML tags and decode entities
        cleaned = re.sub(r'<[^>]+>', '', text)
        cleaned = cleaned.replace('&amp;', '&')
        cleaned = cleaned.replace('&lt;', '<')
        cleaned = cleaned.replace('&gt;', '>')
        cleaned = cleaned.replace('&quot;', '"')
        cleaned = cleaned.replace('&#039;', "'")
        cleaned = re.sub(r'\s+', ' ', cleaned).strip()
        
        # Truncate if needed
        if len(cleaned) > max_length:
            cleaned = cleaned[:max_length].rsplit(' ', 1)[0] + '...'
        
        return cleaned
    
    def _prepare_linkedin_payload(self, job: Dict, keyword: str, url_scraped: str) -> Dict:
        """Transform LinkedIn job data to Clay webhook format"""
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
            "url_scrapped": url_scraped,
            "platform": "linkedin"
        }
        
        return payload
    
    def _prepare_indeed_payload(self, job: Dict, keyword: str, url_scraped: str) -> Dict:
        """Transform Indeed job data to Clay webhook format"""
        # Extract company details
        company_details = job.get("companyDetails", {})
        about_section = company_details.get("aboutSectionViewModel", {})
        about_company = about_section.get("aboutCompany", {})
        
        # Get location
        location = job.get("formattedLocation", "")
        
        # Get company address
        hq_location = about_company.get("headquartersLocation", {})
        company_address = hq_location.get("address", "")
        
        # Get website URL
        website_info = about_company.get("websiteUrl", {})
        company_website = website_info.get("url", "") if isinstance(website_info, dict) else ""
        
        # Get and clean job description
        job_description = self._clean_html(job.get("jobDescription", ""))
        
        # Get salary info
        salary_snippet = job.get("salarySnippet", {})
        salary_text = salary_snippet.get("text", "") if salary_snippet else ""
        currency = salary_snippet.get("currency", "") if salary_snippet else ""
        
        # Format pub date
        pub_date = job.get("pubDate", "")
        if pub_date:
            try:
                # Handle both timestamp formats
                if isinstance(pub_date, (int, float)):
                    if len(str(int(pub_date))) == 10:
                        pub_date = pub_date * 1000
                    from datetime import datetime
                    pub_date = datetime.fromtimestamp(pub_date / 1000).strftime('%Y-%m-%d')
            except:
                pub_date = str(pub_date)
        
        payload = {
            "Company name": job.get("company", ""),
            "Job Title": job.get("displayTitle", ""),
            "Company website": company_website,
            "Apply URL": job.get("thirdPartyApplyUrl", ""),
            "Industry": about_company.get("industry", ""),
            "Job Description": job_description,
            "Location": location,
            "Employee Range": about_company.get("employeeRange", ""),
            "Company description": about_company.get("description", ""),
            "Company Address": company_address,
            "Posted at": pub_date,
            "Date Scrapped": "",  # Will be filled by current date
            "Salary": salary_text,
            "Currency": currency,
            "Input Search URL": url_scraped,
            "NEW JOB": job.get("newJob", False),
            "keyword": keyword,
            "url_scrapped": url_scraped,
            "platform": "indeed"
        }
        
        return payload
    
    def _prepare_job_payload(self, job: Dict, keyword: str, url_scraped: str, platform: str) -> Dict:
        """Transform job data based on platform"""
        if platform == "indeed":
            return self._prepare_indeed_payload(job, keyword, url_scraped)
        else:  # linkedin (default)
            return self._prepare_linkedin_payload(job, keyword, url_scraped)
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    async def _send_single_job(self, job_data: Dict) -> bool:
        """Send a SINGLE job to Clay webhook"""
        try:
            # Send as a single object, NOT an array
            response = await self.client.post(self.webhook_url, json=job_data)
            response.raise_for_status()
            
            logger.debug(f"Successfully sent job to Clay: {job_data.get('Job Title', 'Unknown')}")
            return True
        except httpx.HTTPError as e:
            logger.error(f"Failed to send job to Clay: {e}")
            raise
    
    async def send_jobs(
        self,
        jobs: List[Dict],
        keyword: str,
        url_scraped: str,
        platform: str = "linkedin"
    ) -> int:
        """
        Send jobs to Clay webhook ONE BY ONE in batches
        
        Args:
            jobs: List of job dictionaries
            keyword: Search keyword
            url_scraped: Original search URL
            platform: 'linkedin' or 'indeed'
        
        Returns:
            Number of jobs successfully sent
        """
        if not jobs:
            logger.info("No jobs to send to Clay")
            return 0
        
        # Prepare all payloads
        payloads = [
            self._prepare_job_payload(job, keyword, url_scraped, platform)
            for job in jobs
        ]
        
        # Split into batches
        batches = [
            payloads[i:i + self.batch_size]
            for i in range(0, len(payloads), self.batch_size)
        ]
        
        logger.info(f"Sending {len(payloads)} {platform} jobs in {len(batches)} batches to Clay")
        
        sent_count = 0
        
        for batch_num, batch in enumerate(batches, 1):
            logger.info(f"Processing batch {batch_num}/{len(batches)} ({len(batch)} jobs)")
            
            # Send each job in the batch individually
            for job_payload in batch:
                try:
                    await self._send_single_job(job_payload)
                    sent_count += 1
                    
                    # Small delay between individual jobs within a batch (100ms)
                    await asyncio.sleep(0.1)
                
                except Exception as e:
                    logger.error(f"Failed to send job '{job_payload.get('Job Title')}': {e}")
                    # Continue with next job even if one fails
            
            # Wait between batches (except for the last one)
            if batch_num < len(batches):
                logger.info(f"Waiting {self.batch_interval_seconds}s before next batch...")
                await asyncio.sleep(self.batch_interval_seconds)
        
        logger.info(f"Sent {sent_count}/{len(payloads)} {platform} jobs to Clay")
        return sent_count
    
    async def close(self):
        """Close the HTTP client"""
        await self.client.aclose()
