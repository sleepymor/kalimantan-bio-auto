import httpx
import logging
import os
from typing import Optional, Dict, List, Any
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from src.cache.sqlite_cache import SQLiteCache
from src.api.synonyms import fetch_synonyms, synonym_candidates

logger = logging.getLogger("pipeline.cites")

class CITESClient:
    def __init__(self, cache: SQLiteCache, base_url: str = "https://api.speciesplus.net/api/v1", rate_limit_per_sec: float = 1, timeout: int = 60):
        self.cache = cache
        self.base_url = base_url
        self.rate_limit = rate_limit_per_sec
        self.timeout = timeout
        self.token = os.getenv("SPECIESPLUS_API_TOKEN")
        
        if not self.token:
            logger.warning("SPECIESPLUS_API_TOKEN not set. CITES enrichment will be skipped.")
        
        self.client = httpx.AsyncClient(
            base_url=base_url,
            timeout=timeout,
            headers={
                "User-Agent": "KalimantanBioEnrichment/1.0",
                "X-Authentication-Token": self.token if self.token else ""
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
    async def search_taxon_concept(self, name: str) -> Optional[Dict]:
        if not self._has_token():
            return None
        
        params = {"name": name, "per_page": 5}
        response = await self.client.get("/taxon_concepts", params=params)
        
        if response.status_code == 404:
            return None
        
        response.raise_for_status()
        data = response.json()
        
        results = data.get("taxon_concepts", [])
        if not results:
            return None
        
        for result in results:
            if result.get("rank", "").upper() in ["SPECIES", "SUBSPECIES", "VARIETY"]:
                return result
        
        return results[0]
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=5, max=30),
        retry=retry_if_exception_type((httpx.TimeoutException, httpx.HTTPStatusError))
    )
    async def get_cites_legislation(self, taxon_concept_id: int) -> Optional[Dict]:
        if not self._has_token():
            return None
        
        response = await self.client.get(f"/taxon_concepts/{taxon_concept_id}/cites_legislation")
        
        if response.status_code == 404:
            return None
        
        response.raise_for_status()
        return response.json()
    
    async def enrich(self, usage_key: int, scientific_name: str) -> Optional[Dict]:
        if not self._has_token():
            return None
        
        cached = self.cache.get_cites(usage_key)
        if cached:
            return cached
        
        # Try the canonical name first, then GBIF synonyms (some orchids and
        # timbers are listed under their basionym).
        synonyms = await fetch_synonyms(self.cache, self.client, usage_key, timeout=self.timeout)
        taxon = None
        for name in synonym_candidates(scientific_name, synonyms):
            taxon = await self.search_taxon_concept(name)
            if taxon:
                break
        
        if not taxon:
            # Checked against Species+ and not found: cache explicit unlisted.
            self.cache.set_cites(usage_key, {"appendix": "NOT_LISTED", "effective_date": ""})
            return {"appendix": "NOT_LISTED", "effective_date": "", "party": "", "annotation": ""}
        
        legislation = await self.get_cites_legislation(taxon["id"])
        if not legislation:
            self.cache.set_cites(usage_key, {"appendix": "NOT_LISTED", "effective_date": ""})
            return {"appendix": "NOT_LISTED", "effective_date": "", "party": "", "annotation": ""}
        
        current_listing = None
        for listing in legislation.get("cites_listings", []):
            if listing.get("is_current"):
                current_listing = listing
                break
        
        if not current_listing and legislation.get("cites_listings"):
            current_listing = legislation["cites_listings"][0]
        
        if not current_listing:
            self.cache.set_cites(usage_key, {"appendix": "NOT_LISTED", "effective_date": ""})
            return {"appendix": "NOT_LISTED", "effective_date": "", "party": "", "annotation": ""}
        
        appendix = current_listing.get("appendix", "")
        appendix_map = {"I": "I", "II": "II", "III": "III", "A": "I", "B": "II", "C": "III"}
        appendix = appendix_map.get(appendix, appendix)
        
        result = {
            "appendix": appendix if appendix else "NOT_LISTED",
            "effective_date": current_listing.get("effective_at", ""),
            "party": current_listing.get("party", ""),
            "annotation": current_listing.get("annotation", "")
        }
        
        self.cache.set_cites(usage_key, result)
        return result