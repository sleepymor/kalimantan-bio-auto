import httpx
import logging
from typing import Optional, Dict, List, Any
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from src.cache.sqlite_cache import SQLiteCache

logger = logging.getLogger("pipeline.gbif")

class GBIFClient:
    def __init__(self, cache: SQLiteCache, base_url: str = "https://api.gbif.org/v1", rate_limit_per_sec: float = 100, timeout: int = 30):
        self.cache = cache
        self.base_url = base_url
        self.rate_limit = rate_limit_per_sec
        self.timeout = timeout
        self.client = httpx.AsyncClient(
            base_url=base_url,
            timeout=timeout,
            headers={"User-Agent": "KalimantanBioEnrichment/1.0"}
        )
    
    async def close(self):
        await self.client.aclose()
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((httpx.TimeoutException, httpx.HTTPStatusError))
    )
    async def match_species(self, name: str, strict: bool = False) -> Optional[Dict]:
        cached = self.cache.get_gbif_match(name)
        if cached:
            logger.debug(f"Cache hit for GBIF match: {name}")
            return cached
        
        params = {
            "name": name,
            "strict": str(strict).lower(),
            "verbose": "true"
        }
        
        response = await self.client.get("/species/match", params=params)
        response.raise_for_status()
        data = response.json()
        
        if data.get("matchType") == "NONE" and not strict:
            return await self.match_species(name, strict=True)
        
        result = {
            "usage_key": data.get("usageKey"),
            "accepted_usage_key": data.get("acceptedUsageKey"),
            "canonical_name": data.get("canonicalName"),
            "rank": data.get("rank"),
            "status": data.get("status"),
            "confidence": data.get("confidence"),
            "match_type": data.get("matchType"),
            "taxonomy": {}
        }
        
        taxonomy_key = result["accepted_usage_key"] or result["usage_key"]
        if taxonomy_key:
            taxonomy = await self.get_taxonomy(taxonomy_key)
            result["taxonomy"] = taxonomy
        
        self.cache.set_gbif_match(name, result)
        return result
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((httpx.TimeoutException, httpx.HTTPStatusError))
    )
    async def get_taxonomy(self, usage_key: int) -> Dict[str, Any]:
        response = await self.client.get(f"/species/{usage_key}")
        response.raise_for_status()
        data = response.json()
        
        taxonomy = {}
        for rank in ["kingdom", "phylum", "class", "order", "family", "genus", "species"]:
            if rank in data and data[rank]:
                taxonomy[rank] = data[rank]
        
        return taxonomy
    
    async def get_species_detail(self, usage_key: int) -> Optional[Dict]:
        response = await self.client.get(f"/species/{usage_key}")
        if response.status_code == 404:
            return None
        response.raise_for_status()
        return response.json()
    
    async def get_vernacular_names(self, usage_key: int) -> List[Dict]:
        response = await self.client.get(f"/species/{usage_key}/vernacularNames")
        response.raise_for_status()
        data = response.json()
        return data.get("results", [])
    
    def split_vernacular_names(self, vernacular: List[Dict]) -> tuple[List[str], List[str]]:
        """Split GBIF vernacular names into Indonesian vs other languages."""
        names_id: List[str] = []
        names_other: List[str] = []
        seen_id, seen_other = set(), set()
        for v in vernacular or []:
            name = (v.get("vernacularName") or "").strip()
            if not name:
                continue
            lang = str(v.get("language") or "").lower()
            if lang in ("ind", "id", "indonesian", "bahasa indonesia"):
                if name.lower() not in seen_id:
                    seen_id.add(name.lower())
                    names_id.append(name)
            else:
                if name.lower() not in seen_other:
                    seen_other.add(name.lower())
                    names_other.append(name)
        return names_id, names_other
    
    async def get_indonesian_vernacular(self, usage_key: int) -> tuple[List[str], List[str]]:
        try:
            vernacular = await self.get_vernacular_names(usage_key)
        except Exception:
            return [], []
        return self.split_vernacular_names(vernacular)
    
    async def search_species(self, q: str, limit: int = 10) -> List[Dict]:
        params = {"q": q, "limit": limit}
        response = await self.client.get("/species/search", params=params)
        response.raise_for_status()
        data = response.json()
        return data.get("results", [])