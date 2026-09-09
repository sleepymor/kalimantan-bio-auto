import asyncio
import httpx
from dataclasses import dataclass
from typing import Optional
from urllib.parse import quote

from .config import ScraperConfig, get_config


@dataclass
class INaturalistTaxon:
    id: int
    name: str
    rank: str
    iconic_taxon_name: str
    preferred_common_name: str = ""


@dataclass
class INaturalistPhoto:
    id: int
    url: str
    attribution: str
    license_code: str
    width: int
    height: int


@dataclass
class INaturalistObservation:
    id: int
    taxon: INaturalistTaxon
    photos: list[INaturalistPhoto]
    quality_grade: str


class INaturalistScraper:
    def __init__(self, config: Optional[ScraperConfig] = None):
        self.config = config or get_config()
        self._client: Optional[httpx.AsyncClient] = None

    async def __aenter__(self):
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(self.config.request_timeout),
            headers={"User-Agent": self.config.user_agent},
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._client:
            await self._client.aclose()

    async def search_taxa(self, query: str) -> list[INaturalistTaxon]:
        assert self._client is not None
        url = f"{self.config.inaturalist_base_url}/taxa"
        params = {
            "q": query,
            "rank": "species",
            "per_page": 5,
        }
        try:
            response = await self._client.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            results = data.get("results", [])
            return [
                INaturalistTaxon(
                    id=r["id"],
                    name=r["name"],
                    rank=r["rank"],
                    iconic_taxon_name=r.get("iconic_taxon_name", ""),
                    preferred_common_name=r.get("preferred_common_name", ""),
                )
                for r in results
                if r.get("rank") == "species"
            ]
        except Exception:
            return []

    async def get_taxon_by_name(self, scientific_name: str) -> Optional[INaturalistTaxon]:
        taxa = await self.search_taxa(scientific_name)
        for taxon in taxa:
            if taxon.name.lower() == scientific_name.lower():
                return taxon
        return taxa[0] if taxa else None

    async def get_observations(
        self,
        taxon_id: int,
        per_page: int = 30,
        page: int = 1,
        quality_grade: str = "research",
    ) -> list[INaturalistObservation]:
        assert self._client is not None
        url = f"{self.config.inaturalist_base_url}/observations"
        params = {
            "taxon_id": taxon_id,
            "per_page": per_page,
            "page": page,
            "quality_grade": quality_grade,
            "photos": "true",
            "order": "desc",
            "order_by": "created_at",
        }
        try:
            response = await self._client.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            results = data.get("results", [])
            observations = []
            for obs in results:
                taxon_data = obs.get("taxon", {})
                photos = []
                for photo in obs.get("photos", []):
                    photos.append(
                        INaturalistPhoto(
                            id=photo.get("id", 0),
                            url=photo.get("url", "").replace("square", "original"),
                            attribution=photo.get("attribution", ""),
                            license_code=photo.get("license_code", ""),
                            width=photo.get("original_dimensions", {}).get("width", 0),
                            height=photo.get("original_dimensions", {}).get("height", 0),
                        )
                    )
                if photos:
                    observations.append(
                        INaturalistObservation(
                            id=obs.get("id", 0),
                            taxon=INaturalistTaxon(
                                id=taxon_data.get("id", 0),
                                name=taxon_data.get("name", ""),
                                rank=taxon_data.get("rank", ""),
                                iconic_taxon_name=taxon_data.get("iconic_taxon_name", ""),
                                preferred_common_name=taxon_data.get("preferred_common_name", ""),
                            ),
                            photos=photos,
                            quality_grade=obs.get("quality_grade", ""),
                        )
                    )
            return observations
        except Exception:
            return []

    async def get_species_images(
        self,
        scientific_name: str,
        max_images: int = 10,
        quality_grade: str = "research",
    ) -> tuple[list[str], list[str], list[str], list[str], list[str], list[int]]:
        taxon = await self.get_taxon_by_name(scientific_name)
        if not taxon:
            return [], [], [], [], [], []

        all_image_urls = []
        all_source_urls = []
        all_licenses = []
        all_attributions = []
        all_observation_ids = []
        all_taxon_ids = []

        page = 1
        while len(all_image_urls) < max_images:
            observations = await self.get_observations(
                taxon_id=taxon.id,
                per_page=min(30, max_images - len(all_image_urls)),
                page=page,
                quality_grade=quality_grade,
            )
            if not observations:
                break

            for obs in observations:
                for photo in obs.photos:
                    if len(all_image_urls) >= max_images:
                        break
                    all_image_urls.append(photo.url)
                    all_source_urls.append(f"https://www.inaturalist.org/observations/{obs.id}")
                    all_licenses.append(photo.license_code)
                    all_attributions.append(photo.attribution)
                    all_observation_ids.append(str(obs.id))
                    all_taxon_ids.append(taxon.id)
                if len(all_image_urls) >= max_images:
                    break

            if len(observations) < 30:
                break
            page += 1
            await asyncio.sleep(0.5)

        return (
            all_image_urls[:max_images],
            all_source_urls[:max_images],
            all_licenses[:max_images],
            all_attributions[:max_images],
            all_observation_ids[:max_images],
            all_taxon_ids[:max_images],
        )