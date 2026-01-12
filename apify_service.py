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
        timeout_minutes: Optional[int] = None,
        progress_callback = None
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
            
            # Get dataset ID early for progressive fetching
            dataset_id = run_data.get("defaultDatasetId")
            
            # Poll for completion with progressive data fetching
            status = await self._wait_for_completion(client, run_id, timeout, dataset_id, progress_callback)
            
            if status not in ["SUCCEEDED", "FINISHED"]:
                raise Exception(f"Actor run failed with status: {status}")
            
            # Get final dataset items (dataset_id already retrieved earlier)
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
        
        # Convert string inputs to appropriate types based on input schema
        converted_inputs = self._convert_input_types(user_inputs, actor_config.input_schema)
        
        # Add defaults
        variables = {
            "max_results": converted_inputs.get("max_results", actor_config.default_max_results),
            **converted_inputs
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
    
    def _convert_input_types(self, user_inputs: Dict, input_schema: List[Dict]) -> Dict:
        """
        Convert form string inputs to appropriate types based on schema.
        
        Args:
            user_inputs: Raw form data (all strings)
            input_schema: Schema defining expected types
        
        Returns:
            Dict with properly typed values
        """
        converted = {}
        
        # Create a mapping of field names to their expected types
        type_map = {}
        for field in input_schema:
            type_map[field["name"]] = field.get("type", "text")
        
        for key, value in user_inputs.items():
            field_type = type_map.get(key, "text")
            
            if value is None or value == "":
                converted[key] = None
            elif field_type == "number":
                try:
                    # Try integer first, then float
                    converted[key] = int(value) if str(value).isdigit() else float(value)
                except (ValueError, TypeError):
                    converted[key] = value  # Keep as string if conversion fails
            elif field_type == "boolean":
                # Handle various boolean representations from forms
                if isinstance(value, bool):
                    converted[key] = value
                elif str(value).lower() in ('true', '1', 'on', 'yes'):
                    converted[key] = True
                elif str(value).lower() in ('false', '0', 'off', 'no', ''):
                    converted[key] = False
                else:
                    converted[key] = bool(value)
            else:
                # Keep as string for text, textarea, url, select, etc.
                converted[key] = value
        
        return converted
    
    async def _wait_for_completion(
        self,
        client: httpx.AsyncClient,
        run_id: str,
        timeout_minutes: int,
        dataset_id: str = None,
        progress_callback = None
    ) -> str:
        """Poll for actor run completion with progressive data fetching"""
        
        status_url = f"{self.BASE_URL}/actor-runs/{run_id}"
        start_time = datetime.now()
        attempt = 0
        last_progress_check = start_time
        last_item_count = 0
        
        while True:
            response = await client.get(
                status_url,
                params={"token": self.api_token}
            )
            response.raise_for_status()
            
            run_data = response.json()["data"]
            status = run_data["status"]
            attempt += 1
            elapsed_minutes = (datetime.now() - start_time).total_seconds() / 60
            
            # Get dataset ID if not provided
            if not dataset_id:
                dataset_id = run_data.get("defaultDatasetId")
            
            if status in ["SUCCEEDED", "FINISHED", "FAILED", "ABORTED", "TIMED-OUT"]:
                logger.info(f"[APIFY] Run {run_id} completed with status: {status} after {elapsed_minutes:.1f} minutes")
                return status
            
            # Progressive data fetching: check for new items every 3 minutes
            time_since_progress = (datetime.now() - last_progress_check).total_seconds() / 60
            if time_since_progress >= 3 and dataset_id:
                try:
                    # Get current item count
                    current_items = await self._get_dataset_item_count(client, dataset_id)
                    
                    if current_items > last_item_count:
                        logger.info(f"[APIFY] Progressive update: {current_items} items scraped so far (+{current_items - last_item_count} new)")
                        
                        # Call progress callback if provided (for processing partial data)
                        if progress_callback and current_items > 0:
                            try:
                                partial_items = await self._get_dataset_items(client, dataset_id, offset=last_item_count, limit=current_items - last_item_count)
                                await progress_callback(partial_items, current_items, False)  # False = not final
                            except Exception as e:
                                logger.warning(f"[APIFY] Progress callback failed: {e}")
                        
                        last_item_count = current_items
                    
                    last_progress_check = datetime.now()
                except Exception as e:
                    logger.warning(f"[APIFY] Could not fetch progress data: {e}")
            
            # Continue polling even beyond original timeout (but log warnings)
            if elapsed_minutes >= timeout_minutes:
                logger.warning(f"[APIFY] Run {run_id} exceeded timeout ({timeout_minutes}m) but still running - continuing to poll...")
            
            # Adaptive polling intervals:
            # First 5 minutes: check every 30 seconds (fast for quick jobs)
            # 5-15 minutes: check every 60 seconds  
            # 15+ minutes: check every 120 seconds (efficient for long jobs)
            if elapsed_minutes < 5:
                wait_seconds = 30
            elif elapsed_minutes < 15:
                wait_seconds = 60
            else:
                wait_seconds = 120
            
            timeout_status = f" [OVERTIME: +{elapsed_minutes - timeout_minutes:.1f}m]" if elapsed_minutes >= timeout_minutes else ""
            logger.info(f"[APIFY] Run {run_id} status: {status} (attempt {attempt}, {elapsed_minutes:.1f}m elapsed{timeout_status}) - waiting {wait_seconds}s...")
            await asyncio.sleep(wait_seconds)
    
    async def _get_dataset_items(
        self,
        client: httpx.AsyncClient,
        dataset_id: str,
        limit: int = 10000,
        offset: int = 0
    ) -> List[Dict]:
        """Fetch items from a dataset with pagination support"""
        
        if not dataset_id:
            return []
        
        items_url = f"{self.BASE_URL}/datasets/{dataset_id}/items"
        
        response = await client.get(
            items_url,
            params={
                "token": self.api_token,
                "limit": limit,
                "offset": offset,
                "format": "json"
            }
        )
        response.raise_for_status()
        
        return response.json()
    
    async def _get_dataset_item_count(
        self,
        client: httpx.AsyncClient,
        dataset_id: str
    ) -> int:
        """Get the current number of items in a dataset"""
        
        if not dataset_id:
            return 0
        
        dataset_url = f"{self.BASE_URL}/datasets/{dataset_id}"
        
        response = await client.get(
            dataset_url,
            params={"token": self.api_token}
        )
        response.raise_for_status()
        
        dataset_info = response.json()["data"]
        return dataset_info.get("itemCount", 0)
    
    async def get_dataset_items(self, dataset_id: str, limit: int = 10000, offset: int = 0) -> List[Dict]:
        """Public method to get dataset items"""
        
        async with httpx.AsyncClient(timeout=60.0) as client:
            return await self._get_dataset_items(client, dataset_id, limit, offset)
    
    async def get_all_dataset_items(self, dataset_id: str, chunk_size: int = 1000) -> List[Dict]:
        """Fetch all items from a dataset using pagination"""
        
        if not dataset_id:
            return []
        
        all_items = []
        offset = 0
        
        logger.info(f"📥 Starting paginated fetch for dataset {dataset_id} (chunk size: {chunk_size})")
        
        async with httpx.AsyncClient(timeout=120.0) as client:
            while True:
                logger.info(f"📄 Fetching chunk: offset {offset}, limit {chunk_size}")
                
                chunk = await self._get_dataset_items(client, dataset_id, chunk_size, offset)
                
                if not chunk:
                    logger.info(f"✅ Finished fetching dataset - no more data at offset {offset}")
                    break
                
                chunk_count = len(chunk)
                all_items.extend(chunk)
                
                logger.info(f"📊 Fetched {chunk_count} items (total so far: {len(all_items)})")
                
                # If we got fewer items than requested, we've reached the end
                if chunk_count < chunk_size:
                    logger.info(f"✅ Finished fetching dataset - reached end (got {chunk_count} < {chunk_size})")
                    break
                
                offset += chunk_size
        
        logger.info(f"🎉 Completed dataset fetch: {len(all_items)} total items from dataset {dataset_id}")
        return all_items
    
    async def get_run_status(self, run_id: str) -> Dict:
        """Get current status of a run"""
        
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(
                f"{self.BASE_URL}/actor-runs/{run_id}",
                params={"token": self.api_token}
            )
            response.raise_for_status()
            return response.json()["data"]
