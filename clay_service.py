"""
Generic Clay Service - Sends jobs to Clay webhook using config-driven transformation.

Handles batching, rate limiting, and error recovery.
"""

import httpx
import asyncio
import logging
from typing import Dict, List, Any, Optional

from models import ActorConfig, JobSearch
from template_engine import extract_fields, build_clay_payload

logger = logging.getLogger(__name__)


class ClayService:
    """Generic Clay webhook service that transforms data based on actor config"""
    
    def __init__(self):
        self.default_batch_size = 8
        self.default_batch_interval_ms = 2000
    
    async def send_jobs(
        self,
        jobs: List[Dict],
        actor_config: ActorConfig,
        job_search: JobSearch,
        extra_fields: Optional[Dict] = None
    ) -> Dict[str, Any]:
        """
        Send jobs to Clay webhook with batching.
        
        Returns: {"sent": int, "failed": int, "errors": list}
        """
        webhook_url = job_search.clay_webhook_url
        batch_size = job_search.batch_size or self.default_batch_size
        batch_interval = job_search.batch_interval_ms or self.default_batch_interval_ms
        
        # Extract and transform all jobs
        payloads = []
        for job in jobs:
            payload = self._transform_job(job, actor_config, extra_fields)
            payloads.append(payload)
        
        logger.info(f"Sending {len(payloads)} jobs to Clay in batches of {batch_size}")
        
        # Send in batches
        sent = 0
        failed = 0
        errors = []
        
        for i in range(0, len(payloads), batch_size):
            batch = payloads[i:i + batch_size]
            
            try:
                await self._send_batch(webhook_url, batch)
                sent += len(batch)
                logger.debug(f"Sent batch {i // batch_size + 1}: {len(batch)} jobs")
            except Exception as e:
                failed += len(batch)
                errors.append(f"Batch {i // batch_size + 1}: {str(e)}")
                logger.error(f"Failed to send batch: {e}")
            
            # Rate limit between batches
            if i + batch_size < len(payloads):
                await asyncio.sleep(batch_interval / 1000)
        
        return {
            "sent": sent,
            "failed": failed,
            "errors": errors
        }
    
    def _transform_job(
        self,
        job: Dict,
        actor_config: ActorConfig,
        extra_fields: Optional[Dict] = None
    ) -> Dict:
        """
        Transform a job using actor's output_mapping and clay_template.
        
        1. Extract standardized fields using output_mapping
        2. Build Clay payload using clay_template
        """
        # Step 1: Extract fields from raw job data
        output_mapping = actor_config.output_mapping or {}
        extracted = extract_fields(job, output_mapping)
        
        # Step 2: Build Clay payload
        clay_template = actor_config.clay_template or {}
        payload = build_clay_payload(extracted, clay_template, extra_fields)
        
        return payload
    
    async def _send_batch(self, webhook_url: str, payloads: List[Dict]) -> None:
        """Send a batch of payloads to Clay webhook"""
        
        async with httpx.AsyncClient(timeout=30.0) as client:
            tasks = []
            
            for payload in payloads:
                task = self._send_single(client, webhook_url, payload)
                tasks.append(task)
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Check for failures
            failures = [r for r in results if isinstance(r, Exception)]
            if failures:
                raise Exception(f"{len(failures)} requests failed: {failures[0]}")
    
    async def _send_single(
        self,
        client: httpx.AsyncClient,
        webhook_url: str,
        payload: Dict
    ) -> Dict:
        """Send a single payload to Clay webhook"""
        
        response = await client.post(
            webhook_url,
            json=payload,
            headers={"Content-Type": "application/json"}
        )
        
        # Clay returns 200 or 202 on success
        if response.status_code not in [200, 201, 202]:
            raise Exception(f"Clay returned {response.status_code}: {response.text}")
        
        return {"status": "sent"}
    
    def preview_payload(
        self,
        job: Dict,
        actor_config: ActorConfig,
        extra_fields: Optional[Dict] = None
    ) -> Dict:
        """
        Preview what the Clay payload would look like for a job.
        Useful for debugging and testing.
        """
        return self._transform_job(job, actor_config, extra_fields)
    
    def get_output_fields(self, actor_config: ActorConfig) -> List[str]:
        """Get list of fields that will be sent to Clay"""
        clay_template = actor_config.clay_template or {}
        return list(clay_template.keys())
