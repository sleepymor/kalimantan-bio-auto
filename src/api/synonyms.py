import asyncio
import httpx
import logging
from typing import List, Optional

from src.cache.sqlite_cache import SQLiteCache

logger = logging.getLogger("pipeline.synonyms")

SYNONYMS_URL = "https://api.gbif.org/v1/species/{key}/synonyms"
SPECIES_URL = "https://api.gbif.org/v1/species/{key}"
_SEM = asyncio.Semaphore(5)


async def _get_accepted_key(client: httpx.AsyncClient, usage_key: int) -> int:
    try:
        r = await client.get(SPECIES_URL.format(key=usage_key))
        if r.status_code == 200:
            data = r.json()
            return int(data.get("acceptedKey") or data.get("usageKey") or usage_key)
    except Exception:
        pass
    return usage_key


async def fetch_synonyms(
    cache: SQLiteCache,
    client: httpx.AsyncClient,
    usage_key: int,
    timeout: int = 30,
) -> List[str]:
    """Return cached GBIF synonym names for a usage key (fetching once).

    Includes synonyms of the resolved accepted key so recently-split or
    recently-renamed taxa still surface their basionyms (e.g. Anthoshorea
    -> Shorea), which the IUCN/CITES databases are keyed by.
    """
    cached = cache.get_synonyms(usage_key)
    if cached is not None:
        return cached

    async with _SEM:
        names: List[str] = []
        try:
            accepted = await _get_accepted_key(client, usage_key)
            keys = {int(usage_key), accepted}
            for key in keys:
                r = await client.get(SYNONYMS_URL.format(key=key))
                if r.status_code != 200:
                    continue
                for s in r.json().get("results", []) or []:
                    n = (s.get("scientificName") or "").strip()
                    if not n:
                        continue
                    if len(n.split()) >= 2 and n.lower() not in [x.lower() for x in names]:
                        names.append(n)
        except Exception as e:
            logger.warning(f"GBIF synonym fetch failed for {usage_key}: {e}")
            names = []

    result = sorted(names, key=str.lower)
    cache.set_synonyms(usage_key, result)
    return result


def synonym_candidates(scientific_name: Optional[str], synonyms: List[str]) -> List[str]:
    """Ordered candidate names: the given name first, then cached synonyms."""
    out: List[str] = []
    seen = set()
    for name in [scientific_name] + (synonyms or []):
        n = (name or "").strip()
        if not n:
            continue
        key = n.lower()
        if key not in seen:
            seen.add(key)
            out.append(n)
    return out