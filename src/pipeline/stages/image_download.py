import pandas as pd
import logging
import httpx
import asyncio
import json
import math
import os
from pathlib import Path
from typing import Dict, Any, Optional, List
from tqdm.asyncio import tqdm_asyncio
from PIL import Image
from io import BytesIO

from src.cache.sqlite_cache import SQLiteCache
from src.utils.checkpoint import CheckpointManager
from src.pipeline.stages.enrichment_base import EnrichmentStage, _get_valid_usage_key, merge_results_on_valid_key

logger = logging.getLogger("pipeline.images")

class ImageDownloadStage(EnrichmentStage):
    def __init__(self, config: Dict, cache: SQLiteCache, checkpoint: CheckpointManager, run_mode: str):
        super().__init__(config, cache, checkpoint, "images")
        self.config = config
        self.run_mode = run_mode
        self.images_config = config["images"]
        self.output_dir = Path(self.images_config["output_dir"].format(run_mode=run_mode))
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        self.client = httpx.AsyncClient(
            timeout=self.images_config["timeout_sec"],
            headers={"User-Agent": "KalimantanBioEnrichment/1.0"}
        )
        self.semaphore = asyncio.Semaphore(self.images_config["concurrent_downloads"])
    
    async def _enrich_species(self, usage_key: int, scientific_name: str) -> Optional[Dict]:
        # This is handled in _process_batch
        return None
    
    async def _process_batch(self, batch: pd.DataFrame):
        tasks = []
        for _, row in batch.iterrows():
            usage_key = _get_valid_usage_key(row)
            if usage_key:
                tasks.append(self._download_and_convert(int(usage_key), row))
        
        if tasks:
            await tqdm_asyncio.gather(*tasks, desc="Downloading images")
    
    async def _download_and_convert(self, usage_key: int, row: pd.Series) -> Optional[Dict]:
        async with self.semaphore:
            # Check cache first
            cached = self.cache.get_image(usage_key)
            if cached:
                if cached.get("status") == "none":
                    # Persisted "checked, no image found" - do not re-hammer
                    # source APIs on every rerun.
                    return None
                if cached.get("local_path"):
                    local_path = Path(cached["local_path"])
                    if local_path.exists() and local_path.stat().st_size <= self.images_config["target_max_kb"] * 1024:
                        return cached
            
            # Get image URLs (priority: POWO > GBIF occurrence media > iNaturalist > web)
            image_urls = self._get_image_urls(row)
            if len(image_urls) < 3:
                image_urls += await self._get_gbif_media_urls(usage_key)
            if not image_urls:
                # Canonical (accepted) names may be too new for observation
                # platforms - retry iNaturalist with the original input name
                # and the genus.
                genus = str(row.get("Genus") or "")
                for candidate in dict.fromkeys([
                    str(row.get("canonical_name") or ""),
                    str(row.get("Taxon Name") or ""),
                    genus,
                ]):
                    if candidate and candidate.lower() not in ("nan", "none", ""):
                        image_urls += await self._get_inaturalist_urls(candidate)
                        if image_urls:
                            break
            if not image_urls:
                for candidate in dict.fromkeys([
                    str(row.get("canonical_name") or ""),
                    str(row.get("Taxon Name") or ""),
                ]):
                    if candidate and candidate.lower() not in ("nan", "none", ""):
                        image_urls += await self._get_web_image_urls(candidate)
                        if image_urls:
                            break
            if not image_urls:
                # Explicit negative cache entry.
                self.cache.set_image(usage_key, {"status": "none"})
                return None
            
            for url in image_urls:
                try:
                    result = await self._download_and_convert_url(usage_key, url)
                    if result:
                        return result
                except Exception as e:
                    logger.warning(f"Failed to download {url}: {e}")
                    continue
            
            # All candidates failed.
            self.cache.set_image(usage_key, {"status": "none"})
            return None
    
    def _get_image_urls(self, row: pd.Series) -> List[str]:
        urls = []
        
        # POWO images
        image_urls_json = row.get("image_urls_json")
        if image_urls_json:
            if isinstance(image_urls_json, str):
                try:
                    parsed = json.loads(image_urls_json)
                    if isinstance(parsed, list):
                        urls.extend(parsed)
                except Exception:
                    pass
            elif isinstance(image_urls_json, list):
                urls.extend(image_urls_json)
        
        return urls
    
    async def _get_gbif_media_urls(self, usage_key: int, limit: int = 5) -> List[str]:
        """Fetch StillImage media URLs from GBIF occurrences for a taxon.

        Includes Retry-After-aware backoff because GBIF frequently throttles
        bursty occurrence searches (429/503) on this stage.
        """
        for attempt in (1, 2, 3):
            try:
                response = await self.client.get(
                    "https://api.gbif.org/v1/occurrence/search",
                    params={"taxonKey": usage_key, "mediaType": "StillImage", "limit": 20},
                )
                if response.status_code == 200:
                    urls = []
                    for occ in response.json().get("results", []):
                        for media in occ.get("media", []) or []:
                            if media.get("type") == "StillImage" and media.get("identifier"):
                                urls.append(media["identifier"])
                                if len(urls) >= limit:
                                    return urls
                    return urls
                if response.status_code in (429, 503) and attempt < 3:
                    retry_after = response.headers.get("Retry-After", "")
                    try:
                        delay = min(float(retry_after), 90.0)
                    except (ValueError, TypeError):
                        delay = float(attempt * 20)
                    logger.warning(f"GBIF media {response.status_code} - backing off {delay:.0f}s")
                    await asyncio.sleep(delay)
                    continue
                logger.warning(f"GBIF media fetch returned {response.status_code} for {usage_key}")
                return []
            except Exception as e:
                logger.warning(f"GBIF media fetch failed for {usage_key}: {e}")
                if attempt < 3:
                    await asyncio.sleep(attempt * 10)
                    continue
                return []
        return []
    
    async def _get_inaturalist_urls(self, scientific_name: str, limit: int = 5) -> List[str]:
        """Fetch research-grade photo URLs from iNaturalist for a taxon.

        Genus names are queried without a rank filter so genus-level photo
        pools are reachable for species without direct observations.
        """
        for params_rank in ("species", None):
            taxa_params = {"q": scientific_name, "per_page": 5}
            if params_rank:
                taxa_params["rank"] = params_rank
            try:
                taxa_resp = await self.client.get(
                    "https://api.inaturalist.org/v1/taxa",
                    params=taxa_params,
                )
                if taxa_resp.status_code != 200:
                    continue
                results = taxa_resp.json().get("results", [])
                if not results:
                    continue
                taxon = next(
                    (t for t in results if t.get("name", "").lower() == scientific_name.lower()),
                    results[0],
                )
                obs_resp = await self.client.get(
                    "https://api.inaturalist.org/v1/observations",
                    params={
                        "taxon_id": taxon["id"],
                        "per_page": 10,
                        "quality_grade": "research",
                        "photos": "true",
                        "order": "desc",
                        "order_by": "votes",
                    },
                )
                if obs_resp.status_code != 200:
                    continue
                urls = []
                for obs in obs_resp.json().get("results", []):
                    for photo in obs.get("photos", []) or []:
                        url = (photo.get("url") or "").replace("square", "original")
                        if url:
                            urls.append(url)
                            if len(urls) >= limit:
                                return urls
                if urls:
                    return urls
            except Exception as e:
                logger.warning(f"iNaturalist fetch failed for {scientific_name}: {e}")
                continue
        return []
    
    async def _get_web_image_urls(self, scientific_name: str, limit: int = 5) -> List[str]:
        """DuckDuckGo image search fallback (no API key required).

        Used only as a last resort for rare taxa with no research-grade
        observations on iNaturalist or GBIF occurrences with photos.
        """
        try:
            from ddgs import DDGS
            query = f"{scientific_name} species plant photograph"
            ddgs = DDGS()
            try:
                found = ddgs.images(
                    query,
                    region="wt-wt",
                    safesearch="strict",
                    max_results=limit * 3,
                    size=None,
                    color=None,
                    type_image="photo",
                    layout=None,
                    license_image=None,
                )
            except Exception:
                found = []
            urls = []
            for r in found or []:
                img = r.get("image") or r.get("thumbnail") or r.get("url")
                if img and img.lower().startswith(("http://", "https://")):
                    urls.append(img)
                if len(urls) >= limit:
                    break
            return urls
        except Exception as e:
            logger.warning(f"Web image search failed for {scientific_name}: {e}")
            return []
    
    async def _download_and_convert_url(self, usage_key: int, url: str) -> Optional[Dict]:
        try:
            response = await self.client.get(url, follow_redirects=True)
            response.raise_for_status()
            
            content = response.content
            if len(content) > self.images_config["max_download_mb"] * 1024 * 1024:
                return None
            
            # Convert to WebP
            webp_data = self._convert_to_webp(content)
            if not webp_data:
                return None
            
            # Save
            filename = f"{usage_key}.webp"
            filepath = self.output_dir / filename
            
            with open(filepath, "wb") as f:
                f.write(webp_data)
            
            # Verify
            with Image.open(filepath) as img:
                width, height = img.size
            
            size_kb = len(webp_data) / 1024
            
            result = {
                "usage_key": usage_key,
                "source_url": url,
                "local_path": str(filepath),
                "file_size_kb": int(size_kb),
                "width": width,
                "height": height
            }
            
            self.cache.set_image(usage_key, result)
            return result
            
        except Exception as e:
            logger.warning(f"Error processing image {url}: {e}")
            return None
    
    def _convert_to_webp(self, image_data: bytes) -> Optional[bytes]:
        try:
            img = Image.open(BytesIO(image_data))
            
            # Convert to RGB if needed
            if img.mode in ('RGBA', 'LA', 'P'):
                background = Image.new('RGB', img.size, (255, 255, 255))
                if img.mode == 'P':
                    img = img.convert('RGBA')
                background.paste(img, mask=img.split()[-1] if img.mode in ('RGBA', 'LA') else None)
                img = background
            elif img.mode != 'RGB':
                img = img.convert('RGB')
            
            # Resize if needed
            max_dim = self.images_config["max_dimension_px"]
            if img.width > max_dim or img.height > max_dim:
                img.thumbnail((max_dim, max_dim), Image.LANCZOS)
            
            # Save as WebP with quality adjustment
            quality = self.images_config["webp_quality"]
            target_bytes = self.images_config["target_max_kb"] * 1024
            
            while quality >= 30:
                output = BytesIO()
                img.save(output, format='WEBP', quality=quality, method=6)
                if len(output.getvalue()) <= target_bytes:
                    return output.getvalue()
                quality -= 5
            
            # If still too large, save at minimum quality
            output = BytesIO()
            img.save(output, format='WEBP', quality=30, method=6)
            return output.getvalue()
            
        except Exception as e:
            logger.warning(f"WebP conversion failed: {e}")
            return None
    
    def _load_cached_results(self, df: pd.DataFrame) -> pd.DataFrame:
        results = []
        for _, row in df.iterrows():
            usage_key = _get_valid_usage_key(row)
            if usage_key:
                cached = self.cache.get_image(int(usage_key))
                if cached and cached.get("status") != "none":
                    rec = {k: v for k, v in cached.items() if k != "status"}
                    results.append({"usage_key": usage_key, **rec})
        
        if results:
            img_df = pd.DataFrame(results)
            img_df = img_df.rename(columns={"local_path": "File Gambar (WebP)", "source_url": "URL Gambar"})
            df = merge_results_on_valid_key(df, img_df.to_dict("records"))
        
        return df
    
    async def close(self):
        await self.client.aclose()