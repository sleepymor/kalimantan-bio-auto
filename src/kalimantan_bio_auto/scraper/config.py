import os
from pathlib import Path
from dataclasses import dataclass
from typing import Optional


@dataclass
class ScraperConfig:
    output_dir: Path = Path("data/images")
    metadata_file: Path = Path("data/images/metadata.json")
    request_timeout: int = 30
    download_timeout: int = 60
    max_concurrent_downloads: int = 5
    max_retries: int = 3
    retry_wait: float = 2.0
    user_agent: str = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
    inaturalist_base_url: str = "https://api.inaturalist.org/v1"
    gbif_base_url: str = "https://api.gbif.org/v1"
    min_image_width: int = 200
    min_image_height: int = 200
    max_image_size_mb: int = 50
    allowed_extensions: tuple = (".jpg", ".jpeg", ".png", ".webp")
    prefer_webp: bool = True
    target_max_kb: int = 300
    max_dimension_px: int = 1600
    webp_quality: int = 75

    def __post_init__(self):
        self.output_dir = Path(self.output_dir)
        self.metadata_file = Path(self.metadata_file)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.metadata_file.parent.mkdir(parents=True, exist_ok=True)


DEFAULT_CONFIG = ScraperConfig()


def get_config() -> ScraperConfig:
    return DEFAULT_CONFIG