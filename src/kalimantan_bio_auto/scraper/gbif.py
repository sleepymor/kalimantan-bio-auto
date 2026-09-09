import httpx
from dataclasses import dataclass
from typing import Optional

from .config import ScraperConfig, get_config


@dataclass
class GBIFSpecies:
    key: int
    scientific_name: str
    rank: str
    kingdom: str
    phylum: str = ""
    class_: str = ""
    order: str = ""
    family: str = ""
    genus: str = ""


@dataclass
class GBIFMedia:
    identifier: str
    type: str
    format: str
    license: str
    source: str
    rights_holder: str = ""


@dataclass
class GBIFOccurrence:
    key: int
    scientific_name: str
    media: list[GBIFMedia]


class GBIFScraper:
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

    async def search_species(self, query: str) -> list[GBIFSpecies]:
        assert self._client is not None
        url = f"{self.config.gbif_base_url}/species/match"
        params = {"name": query, "strict": "false", "verbose": "true"}
        try:
            response = await self._client.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            if data.get("matchType") in ("EXACT", "FUZZY", "HIGHERRANK") and data.get("usageKey"):
                return [
                    GBIFSpecies(
                        key=data["usageKey"],
                        scientific_name=data.get("scientificName", ""),
                        rank=data.get("rank", ""),
                        kingdom=data.get("kingdom", ""),
                        phylum=data.get("phylum", ""),
                        class_=data.get("class", ""),
                        order=data.get("order", ""),
                        family=data.get("family", ""),
                        genus=data.get("genus", ""),
                    )
                ]
        except Exception:
            pass
        return []

    async def get_species_key(self, scientific_name: str) -> Optional[int]:
        species = await self.search_species(scientific_name)
        return species[0].key if species else None

    async def get_occurrences_with_media(
        self,
        species_key: int,
        limit: int = 50,
        offset: int = 0,
    ) -> list[GBIFOccurrence]:
        assert self._client is not None
        url = f"{self.config.gbif_base_url}/occurrence/search"
        params = {
            "taxonKey": species_key,
            "mediaType": "StillImage",
            "limit": limit,
            "offset": offset,
            "fields": "key,scientificName,media",
        }
        try:
            response = await self._client.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            results = data.get("results", [])
            occurrences = []
            for occ in results:
                media_list = []
                for media in occ.get("media", []):
                    if media.get("type") == "StillImage":
                        media_list.append(
                            GBIFMedia(
                                identifier=media.get("identifier", ""),
                                type=media.get("type", ""),
                                format=media.get("format", ""),
                                license=media.get("license", ""),
                                source=media.get("source", ""),
                                rights_holder=media.get("rightsHolder", ""),
                            )
                        )
                if media_list:
                    occurrences.append(
                        GBIFOccurrence(
                            key=occ.get("key", 0),
                            scientific_name=occ.get("scientificName", ""),
                            media=media_list,
                        )
                    )
            return occurrences
        except Exception:
            return []

    async def get_species_images(
        self,
        scientific_name: str,
        max_images: int = 10,
    ) -> tuple[list[str], list[str], list[str], list[str], list[str], list[int]]:
        species_key = await self.get_species_key(scientific_name)
        if not species_key:
            return [], [], [], [], [], []

        all_image_urls = []
        all_source_urls = []
        all_licenses = []
        all_attributions = []
        all_observation_ids = []
        all_taxon_ids = []

        offset = 0
        limit = 50

        while len(all_image_urls) < max_images:
            occurrences = await self.get_occurrences_with_media(
                species_key=species_key,
                limit=limit,
                offset=offset,
            )
            if not occurrences:
                break

            for occ in occurrences:
                for media in occ.media:
                    if len(all_image_urls) >= max_images:
                        break
                    all_image_urls.append(media.identifier)
                    all_source_urls.append(f"https://www.gbif.org/occurrence/{occ.key}")
                    all_licenses.append(media.license)
                    all_attributions.append(media.rights_holder or media.source)
                    all_observation_ids.append(str(occ.key))
                    all_taxon_ids.append(species_key)
                if len(all_image_urls) >= max_images:
                    break

            if len(occurrences) < limit:
                break
            offset += limit

        return (
            all_image_urls[:max_images],
            all_source_urls[:max_images],
            all_licenses[:max_images],
            all_attributions[:max_images],
            all_observation_ids[:max_images],
            all_taxon_ids[:max_images],
        )