import httpx
import logging
import re
from typing import Optional, Dict, List, Any
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from src.cache.sqlite_cache import SQLiteCache

logger = logging.getLogger("pipeline.powo")

class POWOClient:
    def __init__(self, cache: SQLiteCache, base_url: str = "https://powo.science.kew.org/api/2", rate_limit_per_sec: float = 2, timeout: int = 30):
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
    
    def _extract_lsid(self, uri: str) -> Optional[str]:
        match = re.search(r'urn:lsid:ipni\.org:names:(\d+-\d+)', uri)
        return match.group(1) if match else None
    
    async def search(self, name: str) -> Optional[Dict]:
        params = {"q": name, "f": "species_f:true", "perPage": 5}
        try:
            response = await self.client.get("/search", params=params)
        except httpx.TimeoutException:
            raise
        except httpx.HTTPError as e:
            logger.warning(f"POWO search failed for {name}: {e}")
            return None
        # POWO API blocks non-browser clients (403) - fail fast, no retry.
        if response.status_code == 403:
            logger.warning("POWO API returned 403 (blocked). Skipping POWO enrichment.")
            return None
        if response.status_code == 404:
            return None
        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as e:
            logger.warning(f"POWO search failed for {name}: {e}")
            return None
        data = response.json()
        
        results = data.get("results", [])
        if not results:
            return None
        
        for result in results:
            if result.get("accepted") and result.get("rank") in ["Species", "Subspecies", "Variety"]:
                return result
        
        return results[0] if results else None
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((httpx.TimeoutException, httpx.HTTPStatusError))
    )
    async def get_taxon(self, lsid: str) -> Optional[Dict]:
        response = await self.client.get(f"/taxon/urn:lsid:ipni.org:names:{lsid}")
        if response.status_code == 404:
            return None
        response.raise_for_status()
        return response.json()
    
    async def enrich(self, usage_key: int, scientific_name: str) -> Optional[Dict]:
        cached = self.cache.get_powo(usage_key)
        if cached:
            return cached
        
        search_result = await self.search(scientific_name)
        if not search_result:
            logger.warning(f"POWO: No search result for {scientific_name}")
            return None
        
        lsid = self._extract_lsid(search_result.get("uri", ""))
        if not lsid:
            logger.warning(f"POWO: Could not extract LSID from {search_result.get('uri')}")
            return None
        
        taxon = await self.get_taxon(lsid)
        if not taxon:
            return None
        
        result = {
            "description_en": taxon.get("description"),
            "distribution": taxon.get("distribution", []),
            "image_urls": self._extract_image_urls(taxon),
            "synonyms": self._extract_synonyms(taxon),
            "powo_id": lsid,
            "powo_uri": search_result.get("uri")
        }
        
        self.cache.set_powo(usage_key, result)
        return result
    
    def _extract_image_urls(self, taxon: Dict) -> List[str]:
        urls = []
        images = taxon.get("images", [])
        for img in images:
            for size in ["original", "large", "medium", "thumbnail"]:
                if size in img and img[size]:
                    urls.append(img[size])
                    break
        
        if not urls and "image" in taxon and taxon["image"]:
            urls.append(taxon["image"])
        
        return urls
    
    def _extract_synonyms(self, taxon: Dict) -> List[str]:
        synonyms = []
        for syn in taxon.get("synonyms", []):
            if syn.get("name"):
                synonyms.append(syn["name"])
        return synonyms