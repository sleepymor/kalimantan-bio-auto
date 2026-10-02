import httpx
import asyncio
import logging
import json
import re
import time
from typing import Optional, Dict, List, Any
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from src.cache.sqlite_cache import SQLiteCache

logger = logging.getLogger("pipeline.wikidata")

WIKIDATA_API_URL = "https://www.wikidata.org/w/api.php"

# Boilerplate descriptions that carry no real information about the taxon
# (e.g. every plant item defaults to "species of plant"). Rejected so
# Deskripsi/Catatan fall through to real prose (Wikipedia, translations).
_GENERIC_EN = re.compile(
    r"^(a\s+)?species of (plant|plants|flowering plant|flowering plants|"
    r"tree|shrub|herb|grass|orchid|fern|plant in the genus \w+)$"
    r"|^(plant species|species|taxon|plant|tree|shrub|herb)$"
    r"|^.{1,80}?\bis a species of (plant|plants|flowering plant|flowering plants)$"
)
_GENERIC_ID = re.compile(
    r"^spesies (tumbuhan|tanaman|tumbuhan berbunga|pohon|palem|anggrek|pakis)\b"
    r"|^(jenis|tumbuhan|tanaman|pohon) (tumbuhan|tanaman)\b"
    r"|^(tumbuhan|tanaman|pohon|palem|anggrek)$"
    r"|^berikut ini adalah daftar\b"
    r"|^.{1,80}?\b(adalah|ialah|merupakan)\s+(sejenis\s+)?(spesies|jenis)\s+"
    r"(tumbuhan|tanaman|tumbuhan berbunga|pohon|palem|anggrek|pakis)(\s+\w+){0,2}$"
)


def strip_generic_description(text: str) -> str:
    """Return '' if the description is generic boilerplate, else the text."""
    t = (text or "").strip().rstrip(".").strip()
    if not t:
        return ""
    low = t.lower()
    if _GENERIC_EN.match(low) or _GENERIC_ID.match(low):
        return ""
    return (text or "").strip()

class WikidataClient:
    def __init__(self, cache: SQLiteCache, sparql_endpoint: str = "https://query.wikidata.org/sparql", rate_limit_per_sec: float = 0.5, timeout: int = 60):
        self.cache = cache
        self.sparql_endpoint = sparql_endpoint
        self.rate_limit = rate_limit_per_sec
        self.timeout = timeout
        self._user_agent = "KalimantanBioEnrichment/1.0 (research; contact: biodiversity-pipeline)"
        self._last_request = 0.0
        self._min_interval = 1.0 / max(rate_limit_per_sec, 0.1)
        self._throttle_lock = asyncio.Lock()
        self._client: Optional[httpx.AsyncClient] = None
        self._consecutive_429 = 0
        self._circuit_open = False
    
    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(
                timeout=self.timeout,
                headers={"User-Agent": self._user_agent},
            )
        return self._client
    
    async def close(self):
        if self._client is not None:
            await self._client.aclose()
            self._client = None
    
    async def _search_entity(self, scientific_name: str) -> Optional[str]:
        """Find Wikidata entity ID whose taxon name (P225) equals the query."""
        client = await self._get_client()
        resp = await client.get(WIKIDATA_API_URL, params={
            "action": "wbsearchentities",
            "search": scientific_name,
            "language": "en",
            "format": "json",
            "limit": 10,
        })
        resp.raise_for_status()
        candidates = resp.json().get("search", [])
        if not candidates:
            return None
        
        # Shortcut: search `match.text` is the exact label/alias hit.
        # Accepting it directly saves the claims-verification call.
        wanted = scientific_name.lower()
        for cand in candidates:
            if (cand.get("match", {}).get("text") or "").lower() == wanted:
                return cand["id"]
        
        ids = "|".join(c["id"] for c in candidates)
        resp = await client.get(WIKIDATA_API_URL, params={
            "action": "wbgetentities",
            "ids": ids,
            "props": "claims",
            "format": "json",
        })
        resp.raise_for_status()
        entities = resp.json().get("entities", {})
        for cand in candidates:
            ent = entities.get(cand["id"], {})
            for stmt in ent.get("claims", {}).get("P225", []):
                try:
                    name = stmt["mainsnak"]["datavalue"]["value"]
                except (KeyError, TypeError):
                    continue
                if name.lower() == wanted:
                    return cand["id"]
        return None
    
    async def _get_entity_texts(self, entity_id: str) -> Dict[str, Any]:
        client = await self._get_client()
        resp = await client.get(WIKIDATA_API_URL, params={
            "action": "wbgetentities",
            "ids": entity_id,
            "props": "labels|descriptions",
            "languages": "id|en",
            "format": "json",
        })
        resp.raise_for_status()
        ent = resp.json().get("entities", {}).get(entity_id, {})
        labels = ent.get("labels", {})
        descriptions = ent.get("descriptions", {})
        desc_id = strip_generic_description(descriptions.get("id", {}).get("value", ""))
        desc_en = strip_generic_description(descriptions.get("en", {}).get("value", ""))
        return {
            "labels_id": [labels["id"]["value"]] if labels.get("id", {}).get("value") else [],
            "labels_other": [labels["en"]["value"]] if labels.get("en", {}).get("value") else [],
            "description_id": desc_id,
            "description_en": desc_en,
            "native_to": [],
            "endemic_to": [],
            "notes_id": desc_id,
        }
    
    async def query(self, scientific_name: str) -> Optional[Dict]:
        # Circuit breaker: sustained 429s mean the host is throttled -
        # fail fast for the rest of the run instead of stalling it.
        if self._circuit_open:
            return None
        # Serialize throttled API calls: concurrent tasks would otherwise
        # all pass the interval check at once and get throttled server-side.
        async with self._throttle_lock:
            now = asyncio.get_event_loop().time()
            wait = self._min_interval - (now - self._last_request)
            if wait > 0:
                await asyncio.sleep(wait)
            try:
                entity_id = await self._search_entity(scientific_name)
                if not entity_id:
                    return None
                result = await self._get_entity_texts(entity_id)
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 429:
                    retry_after = e.response.headers.get("Retry-After")
                    delay = float(retry_after) if retry_after and retry_after.isdigit() else 60.0
                    logger.warning(f"Wikidata 429 - backing off {delay:.0f}s")
                    await asyncio.sleep(min(delay, 120.0))
                    self._consecutive_429 += 1
                    if self._consecutive_429 >= 5:
                        logger.warning("Wikidata circuit breaker open - skipping remaining lookups")
                        self._circuit_open = True
                else:
                    logger.warning(f"Wikidata query failed for {scientific_name}: {e}")
                return None
            except Exception as e:
                logger.warning(f"Wikidata query failed for {scientific_name}: {e}")
                return None
            finally:
                self._last_request = asyncio.get_event_loop().time()
            self._consecutive_429 = 0
            return result
    
    async def enrich(self, usage_key: int, scientific_name: str) -> Optional[Dict]:
        cached = self.cache.get_wikidata(usage_key)
        if cached:
            return cached
        
        result = await self.query(scientific_name)
        if not result:
            return None
        
        self.cache.set_wikidata(usage_key, result)
        return result