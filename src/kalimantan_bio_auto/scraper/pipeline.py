import asyncio
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from rich.console import Console
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TaskProgressColumn,
    TextColumn,
    TimeRemainingColumn,
)

from .config import ScraperConfig, get_config
from .downloader import ImageDownloader, ImageMetadata
from .gbif import GBIFScraper
from .inaturalist import INaturalistScraper
from .web_search import WebSearchScraper


@dataclass
class ScrapeResult:
    species: str
    requested: int
    downloaded: int
    sources: dict[str, int]
    metadata: list[ImageMetadata]


class ScraperPipeline:
    def __init__(self, config: Optional[ScraperConfig] = None, console: Optional[Console] = None):
        self.config = config or get_config()
        self.console = console or Console()
        self._downloader: Optional[ImageDownloader] = None
        self._inaturalist: Optional[INaturalistScraper] = None
        self._gbif: Optional[GBIFScraper] = None
        self._web_search: Optional[WebSearchScraper] = None

    async def __aenter__(self):
        self._downloader = ImageDownloader(self.config)
        await self._downloader.__aenter__()
        self._inaturalist = INaturalistScraper(self.config)
        await self._inaturalist.__aenter__()
        self._gbif = GBIFScraper(self.config)
        await self._gbif.__aenter__()
        self._web_search = WebSearchScraper(self.config)
        await self._web_search.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._downloader:
            await self._downloader.__aexit__(exc_type, exc_val, exc_tb)
        if self._inaturalist:
            await self._inaturalist.__aexit__(exc_type, exc_val, exc_tb)
        if self._gbif:
            await self._gbif.__aexit__(exc_type, exc_val, exc_tb)
        if self._web_search:
            await self._web_search.__aexit__(exc_type, exc_val, exc_tb)

    def _load_species_list(self, file_path: Path) -> list[tuple[str, int]]:
        species_list = []
        with open(file_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                parts = line.split(",")
                species = parts[0].strip()
                count = int(parts[1].strip()) if len(parts) > 1 and parts[1].strip().isdigit() else 10
                species_list.append((species, count))
        return species_list

    def _get_existing_image_count(self, species: str) -> int:
        safe_species = self._sanitize_filename(species.replace(" ", "_"))
        species_dir = self.config.output_dir / safe_species
        if not species_dir.exists():
            return 0
        existing = list(species_dir.glob("*.jpg")) + list(species_dir.glob("*.jpeg")) + \
                   list(species_dir.glob("*.png")) + list(species_dir.glob("*.webp"))
        return len(existing)

    def _sanitize_filename(self, name: str) -> str:
        import re
        name = re.sub(r'[<>:"/\\|?*]', "_", name)
        name = name.strip(". ")
        return name[:200]

    async def scrape_species(
        self,
        scientific_name: str,
        target_count: int = 10,
        progress: Optional[Progress] = None,
        task_id: Optional[int] = None,
    ) -> ScrapeResult:
        assert self._downloader is not None
        assert self._inaturalist is not None
        assert self._gbif is not None
        assert self._web_search is not None

        existing_count = self._get_existing_image_count(scientific_name)
        if existing_count >= target_count:
            self.console.print(f"[yellow]Skipping {scientific_name}: already has {existing_count}/{target_count} images[/yellow]")
            if progress and task_id is not None:
                progress.update(task_id, completed=target_count)
            return ScrapeResult(
                species=scientific_name,
                requested=target_count,
                downloaded=existing_count,
                sources={"inaturalist": 0, "gbif": 0, "web_search": 0},
                metadata=[],
            )

        downloaded_metadata: list[ImageMetadata] = []
        sources_count = {"inaturalist": 0, "gbif": 0, "web_search": 0}
        remaining = target_count - existing_count

        def update_progress(downloaded: int):
            if progress and task_id is not None:
                progress.update(task_id, completed=existing_count + downloaded)

        inat_images, inat_sources, inat_licenses, inat_attributions, inat_obs_ids, inat_taxon_ids = (
            await self._inaturalist.get_species_images(scientific_name, max_images=remaining)
        )
        if inat_images:
            meta = await self._downloader.download_multiple(
                urls=inat_images,
                species=scientific_name,
                source="inaturalist",
                source_urls=inat_sources,
                licenses=inat_licenses,
                attributions=inat_attributions,
                observation_ids=inat_obs_ids,
                taxon_ids=inat_taxon_ids,
            )
            downloaded_metadata.extend(meta)
            sources_count["inaturalist"] = len(meta)
            remaining -= len(meta)
            update_progress(target_count - remaining)

        if remaining > 0:
            gbif_images, gbif_sources, gbif_licenses, gbif_attributions, gbif_obs_ids, gbif_taxon_ids = (
                await self._gbif.get_species_images(scientific_name, max_images=remaining)
            )
            if gbif_images:
                meta = await self._downloader.download_multiple(
                    urls=gbif_images,
                    species=scientific_name,
                    source="gbif",
                    source_urls=gbif_sources,
                    licenses=gbif_licenses,
                    attributions=gbif_attributions,
                    observation_ids=gbif_obs_ids,
                    taxon_ids=gbif_taxon_ids,
                )
                downloaded_metadata.extend(meta)
                sources_count["gbif"] = len(meta)
                remaining -= len(meta)
                update_progress(target_count - remaining)

        if remaining > 0:
            web_images, web_sources, web_licenses, web_attributions, web_obs_ids, web_taxon_ids = (
                await self._web_search.get_species_images(scientific_name, max_images=remaining)
            )
            if web_images:
                meta = await self._downloader.download_multiple(
                    urls=web_images,
                    species=scientific_name,
                    source="web_search",
                    source_urls=web_sources,
                    licenses=web_licenses,
                    attributions=web_attributions,
                    observation_ids=web_obs_ids,
                    taxon_ids=web_taxon_ids,
                )
                downloaded_metadata.extend(meta)
                sources_count["web_search"] = len(meta)
                remaining -= len(meta)
                update_progress(target_count - remaining)

        return ScrapeResult(
            species=scientific_name,
            requested=target_count,
            downloaded=existing_count + len(downloaded_metadata),
            sources=sources_count,
            metadata=downloaded_metadata,
        )

    async def scrape_from_file(
        self,
        file_path: Path,
        default_count: int = 10,
    ) -> list[ScrapeResult]:
        species_list = self._load_species_list(file_path)
        if not species_list:
            self.console.print("[yellow]No species found in file[/yellow]")
            return []

        results = []
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            TimeRemainingColumn(),
            console=self.console,
        ) as progress:
            overall_task = progress.add_task(
                f"[cyan]Overall: 0/{len(species_list)} species",
                total=len(species_list),
            )

            for idx, (species, count) in enumerate(species_list):
                target = count if count > 0 else default_count
                species_task = progress.add_task(
                    f"[green]{species}[/green]",
                    total=target,
                )

                result = await self.scrape_species(
                    scientific_name=species,
                    target_count=target,
                    progress=progress,
                    task_id=species_task,
                )
                results.append(result)

                progress.update(species_task, visible=False)
                progress.update(
                    overall_task,
                    advance=1,
                    description=f"[cyan]Overall: {idx + 1}/{len(species_list)} species",
                )

        return results

    async def scrape_single(
        self,
        scientific_name: str,
        count: int = 10,
    ) -> ScrapeResult:
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            TimeRemainingColumn(),
            console=self.console,
        ) as progress:
            task = progress.add_task(f"[green]{scientific_name}[/green]", total=count)
            return await self.scrape_species(
                scientific_name=scientific_name,
                target_count=count,
                progress=progress,
                task_id=task,
            )