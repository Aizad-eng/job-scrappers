"""
Generic Apify Service - Works with ANY actor using config-driven payloads.

No actor-specific code here. All logic comes from actor_registry configs.
"""

import httpx
import asyncio
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime

from models import ActorConfig, JobSearch
from template_engine import render_template, clean_payload

logger = logging.getLogger(__name__)


class ApifyService:
    """Generic Apify service that works with any actor based on config"""
    
    BASE_URL = "https://api.apify.com/v2"
    
    def __init__(self, api_token: str):
        self.api_token = api_token
    
    async def run_actor(
        self,
        actor_config: ActorConfig,
        job_search: JobSearch,
        timeout_minutes: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Run an Apify actor using config-driven payload building.
        
        Returns: {"run_id": str, "dataset_id": str, "status": str, "items": list}
        """
        timeout = timeout_minutes or actor_config.default_timeout_minutes
        
        # Build the input payload from template
        payload = self._build_payload(actor_config, job_search)
        
        logger.info(f"Running actor {actor_config.actor_id} with payload: {payload}")
        
        async with httpx.AsyncClient(timeout=60.0) as client:
            # Start the actor run
            run_url = f"{self.BASE_URL}/acts/{actor_config.actor_id}/runs"
            
            response = await client.post(
                run_url,
                params={"token": self.api_token},
                json=payload
            )
            response.raise_for_status()
            
            run_data = response.json()["data"]
            run_id = run_data["id"]
            
            logger.info(f"Started actor run: {run_id}")
            
            # Poll for completion
            status = await self._wait_for_completion(client, run_id, timeout)
            
            if status not in ["SUCCEEDED", "FINISHED"]:
                raise Exception(f"Actor run failed with status: {status}")
            
            # Get dataset items
            dataset_id = run_data.get("defaultDatasetId")
            items = await self._get_dataset_items(client, dataset_id)
            
            return {
                "run_id": run_id,
                "dataset_id": dataset_id,
                "status": status,
                "items": items
            }
    
    def _build_payload(self, actor_config: ActorConfig, job_search: JobSearch) -> Dict:
        """
        Build Apify input payload from actor's input_template and job_search inputs.
        
        The magic: actor_config.input_template defines the structure,
        job_search.actor_inputs provides the values.
        """
        # Get the user's inputs for this search
        user_inputs = job_search.actor_inputs or {}
        
        # Add defaults
        variables = {
            "max_results": user_inputs.get("max_results", actor_config.default_max_results),
            **user_inputs
        }
        
        # Handle filter rules that go into the payload (like employee count filters)
        filter_rules = job_search.filter_rules or {}
        if filter_rules.get("min_employees"):
            variables["min_employees"] = filter_rules["min_employees"]
        if filter_rules.get("max_employees"):
            variables["max_employees"] = filter_rules["max_employees"]
        
        # Render the template
        payload = render_template(actor_config.input_template, variables)
        
        # Clean up None values and empty fields
        payload = clean_payload(payload)
        
        return payload
    
    async def _wait_for_completion(
        self,
        client: httpx.AsyncClient,
        run_id: str,
        timeout_minutes: int
    ) -> str:
        """Poll for actor run completion"""
        
        status_url = f"{self.BASE_URL}/actor-runs/{run_id}"
        max_attempts = timeout_minutes * 6  # Check every 10 seconds
        
        for attempt in range(max_attempts):
            response = await client.get(
                status_url,
                params={"token": self.api_token}
            )
            response.raise_for_status()
            
            status = response.json()["data"]["status"]
            
            if status in ["SUCCEEDED", "FINISHED", "FAILED", "ABORTED", "TIMED-OUT"]:
                return status
            
            logger.debug(f"Run {run_id} status: {status} (attempt {attempt + 1}/{max_attempts})")
            await asyncio.sleep(10)
        
        raise TimeoutError(f"Actor run {run_id} timed out after {timeout_minutes} minutes")
    
    async def _get_dataset_items(
        self,
        client: httpx.AsyncClient,
        dataset_id: str,
        limit: int = 10000
    ) -> List[Dict]:
        """Fetch all items from a dataset"""
        
        if not dataset_id:
            return []
        
        items_url = f"{self.BASE_URL}/datasets/{dataset_id}/items"
        
        response = await client.get(
            items_url,
            params={
                "token": self.api_token,
                "limit": limit,
                "format": "json"
            }
        )
        response.raise_for_status()
        
        return response.json()
    
    async def get_run_status(self, run_id: str) -> Dict:
        """Get current status of a run"""
        
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(
                f"{self.BASE_URL}/actor-runs/{run_id}",
                params={"token": self.api_token}
            )
            response.raise_for_status()
            return response.json()["data"]
