import asyncio
import hashlib
import json
import mimetypes
import os
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

import httpx
from io import BytesIO
from PIL import Image
from tenacity import retry, stop_after_attempt, wait_exponential

from .config import ScraperConfig, get_config


@dataclass
class ImageMetadata:
    species: str
    source: str
    source_url: str
    file_path: str
    file_name: str
    width: int
    height: int
    file_size_bytes: int
    mime_type: str
    downloaded_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    license: str = ""
    attribution: str = ""
    observation_id: str = ""
    taxon_id: int = 0

    def to_dict(self) -> dict:
        return {
            "species": self.species,
            "source": self.source,
            "source_url": self.source_url,
            "file_path": self.file_path,
            "file_name": self.file_name,
            "width": self.width,
            "height": self.height,
            "file_size_bytes": self.file_size_bytes,
            "mime_type": self.mime_type,
            "downloaded_at": self.downloaded_at,
            "license": self.license,
            "attribution": self.attribution,
            "observation_id": self.observation_id,
            "taxon_id": self.taxon_id,
        }


class ImageDownloader:
    def __init__(self, config: Optional[ScraperConfig] = None):
        self.config = config or get_config()
        self._semaphore: Optional[asyncio.Semaphore] = None
        self._client: Optional[httpx.AsyncClient] = None
        self._metadata_lock = asyncio.Lock()
        self._metadata: list[ImageMetadata] = []

    async def __aenter__(self):
        self._semaphore = asyncio.Semaphore(self.config.max_concurrent_downloads)
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(self.config.download_timeout),
            headers={"User-Agent": self.config.user_agent},
            follow_redirects=True,
        )
        await self._load_metadata()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._client:
            await self._client.aclose()
        await self._save_metadata()

    async def _load_metadata(self):
        if self.config.metadata_file.exists():
            try:
                with open(self.config.metadata_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    self._metadata = [ImageMetadata(**item) for item in data]
            except (json.JSONDecodeError, TypeError):
                self._metadata = []
        self._downloaded_urls = {m.source_url for m in self._metadata}

    async def _save_metadata(self):
        async with self._metadata_lock:
            data = [item.to_dict() for item in self._metadata]
            with open(self.config.metadata_file, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)

    def _is_url_downloaded(self, url: str) -> bool:
        return url in self._downloaded_urls

    def _sanitize_filename(self, name: str) -> str:
        name = re.sub(r'[<>:"/\\|?*]', "_", name)
        name = name.strip(". ")
        return name[:200]

    def _get_species_dir(self, species: str) -> Path:
        safe_species = self._sanitize_filename(species.replace(" ", "_"))
        return self.config.output_dir / safe_species

    def _get_file_extension(self, url: str, content_type: str) -> str:
        ext = Path(urlparse(url).path).suffix.lower()
        if ext in self.config.allowed_extensions:
            return ext
        guessed = mimetypes.guess_extension(content_type) or ".jpg"
        return guessed if guessed in self.config.allowed_extensions else ".jpg"

    def _validate_image(self, file_path: Path) -> tuple[bool, Optional[tuple[int, int]]]:
        try:
            with Image.open(file_path) as img:
                img.verify()
            with Image.open(file_path) as img:
                width, height = img.size
                if width < self.config.min_image_width or height < self.config.min_image_height:
                    return False, None
                return True, (width, height)
        except Exception:
            return False, None

    def _normalize_for_save(self, img: Image.Image) -> Image.Image:
        if img.mode in ("RGBA", "LA", "P"):
            background = Image.new("RGB", img.size, (255, 255, 255))
            if img.mode == "P":
                img = img.convert("RGBA")
            mask = img.split()[-1] if img.mode in ("RGBA", "LA") else None
            background.paste(img, mask=mask)
            return background
        if img.mode != "RGB":
            return img.convert("RGB")
        return img

    def _resize_if_needed(self, img: Image.Image) -> Image.Image:
        max_dim = self.config.max_dimension_px
        if img.width > max_dim or img.height > max_dim:
            img.thumbnail((max_dim, max_dim), Image.LANCZOS)
        return img

    def _try_webp_compress(self, img: Image.Image) -> Optional[bytes]:
        quality = self.config.webp_quality
        target_bytes = self.config.target_max_kb * 1024
        best: Optional[bytes] = None
        while quality >= 30:
            output = BytesIO()
            img.save(output, format="WEBP", quality=quality, method=6)
            data = output.getvalue()
            best = data
            if len(data) <= target_bytes:
                return data
            quality -= 5
        return best

    def _optimized_original_bytes(self, img: Image.Image, ext: str) -> Optional[bytes]:
        try:
            output = BytesIO()
            if ext in (".jpg", ".jpeg"):
                img.save(output, format="JPEG", quality=85, optimize=True)
            elif ext == ".png":
                img.save(output, format="PNG", optimize=True)
            elif ext == ".webp":
                img.save(output, format="WEBP", quality=self.config.webp_quality, method=6)
            else:
                img.save(output, format="JPEG", quality=85, optimize=True)
            return output.getvalue()
        except Exception:
            return None

    @retry(
        wait=wait_exponential(multiplier=1, min=2, max=10),
        stop=stop_after_attempt(3),
        reraise=True,
    )
    async def _download_with_retry(self, url: str) -> httpx.Response:
        assert self._client is not None
        response = await self._client.get(url)
        response.raise_for_status()
        return response

    async def download_image(
        self,
        url: str,
        species: str,
        source: str,
        source_url: str,
        license: str = "",
        attribution: str = "",
        observation_id: str = "",
        taxon_id: int = 0,
    ) -> Optional[ImageMetadata]:
        async with self._semaphore:
            try:
                response = await self._download_with_retry(url)
            except Exception as e:
                return None

            content_type = response.headers.get("content-type", "")
            if not content_type.startswith("image/"):
                return None

            if len(response.content) > self.config.max_image_size_mb * 1024 * 1024:
                return None

            orig_ext = self._get_file_extension(url, content_type)

            try:
                with Image.open(BytesIO(response.content)) as probe:
                    probe.load()
                    if probe.width < self.config.min_image_width or probe.height < self.config.min_image_height:
                        return None
                    img = probe.copy()
            except Exception:
                return None

            img = self._normalize_for_save(img)
            img = self._resize_if_needed(img)

            final_bytes: Optional[bytes] = None
            final_ext = orig_ext
            final_mime = content_type

            if self.config.prefer_webp:
                webp_bytes = self._try_webp_compress(img)
                orig_opt_bytes = self._optimized_original_bytes(img, orig_ext)
                candidates: list[tuple[bytes, str, str]] = []
                if webp_bytes:
                    candidates.append((webp_bytes, ".webp", "image/webp"))
                if orig_opt_bytes:
                    candidates.append((orig_opt_bytes, orig_ext, content_type))
                if not candidates:
                    return None
                # Prefer smallest file that meets target; otherwise smallest overall
                target_bytes = self.config.target_max_kb * 1024
                within = [c for c in candidates if len(c[0]) <= target_bytes]
                chosen = min(within, key=lambda c: len(c[0])) if within else min(candidates, key=lambda c: len(c[0]))
                final_bytes, final_ext, final_mime = chosen
            else:
                final_bytes = self._optimized_original_bytes(img, orig_ext) or response.content

            if not final_bytes:
                return None

            species_dir = self._get_species_dir(species)
            species_dir.mkdir(parents=True, exist_ok=True)

            existing_count = sum(
                len(list(species_dir.glob(f"*{e}"))) for e in self.config.allowed_extensions
            )
            file_num = existing_count + 1
            file_name = f"{file_num:03d}_{source}_{hashlib.md5(url.encode()).hexdigest()[:8]}{final_ext}"
            file_path = species_dir / file_name

            try:
                with open(file_path, "wb") as f:
                    f.write(final_bytes)
            except Exception:
                return None

            valid, dimensions = self._validate_image(file_path)
            if not valid:
                try:
                    file_path.unlink()
                except Exception:
                    pass
                return None

            width, height = dimensions
            file_size = file_path.stat().st_size

            metadata = ImageMetadata(
                species=species,
                source=source,
                source_url=source_url,
                file_path=str(file_path.relative_to(self.config.output_dir.parent)),
                file_name=file_name,
                width=width,
                height=height,
                file_size_bytes=file_size,
                mime_type=final_mime,
                license=license,
                attribution=attribution,
                observation_id=observation_id,
                taxon_id=taxon_id,
            )

            async with self._metadata_lock:
                self._metadata.append(metadata)

            return metadata

    async def download_multiple(
        self,
        urls: list[str],
        species: str,
        source: str,
        source_urls: list[str],
        licenses: Optional[list[str]] = None,
        attributions: Optional[list[str]] = None,
        observation_ids: Optional[list[str]] = None,
        taxon_ids: Optional[list[int]] = None,
    ) -> list[ImageMetadata]:
        licenses = licenses or [""] * len(urls)
        attributions = attributions or [""] * len(urls)
        observation_ids = observation_ids or [""] * len(urls)
        taxon_ids = taxon_ids or [0] * len(urls)

        filtered = [
            (url, source_url, lic, attr, obs_id, tax_id)
            for url, source_url, lic, attr, obs_id, tax_id in zip(
                urls, source_urls, licenses, attributions, observation_ids, taxon_ids
            )
            if not self._is_url_downloaded(source_url)
        ]

        if not filtered:
            return []

        tasks = [
            self.download_image(
                url=url,
                species=species,
                source=source,
                source_url=source_url,
                license=lic,
                attribution=attr,
                observation_id=obs_id,
                taxon_id=tax_id,
            )
            for url, source_url, lic, attr, obs_id, tax_id in filtered
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)
        return [r for r in results if isinstance(r, ImageMetadata)]