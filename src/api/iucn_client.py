import httpx
import logging
import os
from typing import Optional, Dict, List, Any
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from src.cache.sqlite_cache import SQLiteCache
from src.api.synonyms import fetch_synonyms, synonym_candidates

logger = logging.getLogger("pipeline.iucn")

# Categories that mean "checked, not assessed" vs an actual assessment.
_NOT_ASSESSED = {"NE"}

class IUCNClient:
    def __init__(self, cache: SQLiteCache, base_url: str = "https://api.iucnredlist.org/api/v4", rate_limit_per_sec: float = 0.5, timeout: int = 60):
        self.cache = cache
        self.base_url = base_url
        self.rate_limit = rate_limit_per_sec
        self.timeout = timeout
        self.token = os.getenv("IUCN_REDLIST_API_TOKEN")
        
        if not self.token:
            logger.warning("IUCN_REDLIST_API_TOKEN not set. IUCN enrichment will be skipped.")
        
        self.client = httpx.AsyncClient(
            base_url=base_url,
            timeout=timeout,
            headers={
                "User-Agent": "KalimantanBioEnrichment/1.0",
                "Authorization": f"Bearer {self.token}" if self.token else ""
            }
        )
    
    async def close(self):
        await self.client.aclose()
    
    def _has_token(self) -> bool:
        return bool(self.token)
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=5, max=30),
        retry=retry_if_exception_type((httpx.TimeoutException, httpx.HTTPStatusError))
    )
    async def get_assessments(self, scientific_name: str) -> Optional[List[Dict]]:
        if not self._has_token():
            return None
        
        parts = scientific_name.split()
        if len(parts) < 2:
            return None
        params = {"genus_name": parts[0], "species_name": parts[1]}
        response = await self.client.get("/taxa/scientific_name", params=params)
        
        if response.status_code == 404:
            return None
        
        response.raise_for_status()
        data = response.json()
        
        assessments = data.get("assessments", []) or []
        
        return assessments
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=5, max=30),
        retry=retry_if_exception_type((httpx.TimeoutException, httpx.HTTPStatusError))
    )
    async def get_latest_assessment(self, scientific_name: str) -> Optional[Dict]:
        assessments = await self.get_assessments(scientific_name)
        if not assessments:
            return None
        
        def _year(a: Dict) -> int:
            try:
                return int(str(a.get("year_published", 0)))
            except (ValueError, TypeError):
                return 0
        
        latest = None
        for assessment in assessments:
            if assessment.get("latest"):
                latest = assessment
                break
        
        if latest is None:
            latest = max(assessments, key=_year)
        
        return {
            "category": latest.get("red_list_category_code", "NE"),
            "criteria": latest.get("criteria", ""),
            "year": _year(latest) or None,
            "assessment_id": latest.get("assessment_id"),
        }
    
    async def enrich(self, usage_key: int, scientific_name: str) -> Optional[Dict]:
        if not self._has_token():
            return None
        
        cached = self.cache.get_iucn(usage_key)
        if cached:
            return cached
        
        # IUCN keys many names by their basionym (e.g. Anthoshorea -> Shorea),
        # so also try GBIF synonyms before declaring "not assessed".
        synonyms = await fetch_synonyms(self.cache, self.client, usage_key, timeout=self.timeout)
        assessment = None
        candidate_names = synonym_candidates(scientific_name, synonyms)
        for name in candidate_names:
            if len(name.split()) < 2:
                continue
            assessment = await self.get_latest_assessment(name)
            if assessment:
                break
        
        if not assessment:
            # Checked but not assessed: cache explicit NE so reruns skip and
            # the export can show NE rather than an unexplained blank.
            result = {
                "category": "NE",
                "criteria": "",
                "year": None,
                "assessment_id": None,
            }
            self.cache.set_iucn(usage_key, result)
            return result
        
        result = {
            "category": assessment.get("category", "NE"),
            "criteria": assessment.get("criteria", ""),
            "year": assessment.get("year"),
            "assessment_id": assessment.get("assessment_id")
        }
        
        self.cache.set_iucn(usage_key, result)
        return result