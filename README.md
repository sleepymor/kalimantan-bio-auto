# Kalimantan Bio Auto - Species Image Scraper

Automated species image scraper for biodiversity data collection. Retrieves verified species imagery from iNaturalist and GBIF APIs (primary sources for taxonomic accuracy) with SafeSearch-enabled web search fallback.

## Features

- **Verified Sources**: iNaturalist (research-grade observations) and GBIF (scientific occurrence records)
- **Species Verification**: Confirms taxonomic identity before downloading
- **SafeSearch Fallback**: DuckDuckGo web search with strict safesearch when API sources are exhausted
- **Image Validation**: Pillow validation (min 200x200px, valid JPEG/PNG/WebP, max 50MB)
- **Organized Output**: Species folders with metadata tracking
- **Concurrent Downloads**: Async with configurable concurrency and retry logic
- **Progress Tracking**: Rich terminal progress bars

## Installation

```bash
# Install in development mode
pip install -e .

# Or with uv
uv pip install -e .
```

> **Note:** The CLI entry point (`kalimantan-bio-auto`) installs to Python's Scripts directory which may not be in your system PATH. Use `python -m kalimantan_bio_auto.cli` or `uv run kalimantan-bio-auto` instead.

## Quick Start

### 1. Create a species list file

Create a text file with one species per line. Optionally specify count per species with comma:

```text
# species.txt
Pongo pygmaeus,5
Nasalis larvatus,5
Helarctos malayanus,3
Panthera tigris
```

### 2. Run the scraper

```bash
# Scrape from file (uses per-species counts from file, or default --count)
python -m kalimantan_bio_auto.cli scrape --file species.txt --count 10

# Scrape single species
python -m kalimantan_bio_auto.cli scrape --species "Pongo pygmaeus" --count 15

# Custom output directory
python -m kalimantan_bio_auto.cli scrape --file species.txt --output /path/to/images

# Or with uv:
# uv run kalimantan-bio-auto scrape --file species.txt --count 10
```

### 3. Check results

```
data/
└── images/
    ├── Pongo_pygmaeus/
    │   ├── 001_inaturalist_a1b2c3d4.jpg
    │   ├── 002_inaturalist_e5f6g7h8.jpg
    │   └── ...
    ├── Nasalis_larvatus/
    │   └── ...
    └── metadata.json
```

## CLI Commands

Run with `python -m kalimantan_bio_auto.cli` or `uv run kalimantan-bio-auto`:

### `scrape` - Download species images

```bash
python -m kalimantan_bio_auto.cli scrape [OPTIONS]

Options:
  -f, --file PATH       Species list file (one per line, or "species,count")
  -s, --species TEXT    Single species scientific name
  -c, --count INTEGER   Images per species (default: 10, max: 500)
  -o, --output PATH     Output directory (default: data/images)
  --help                Show help
```

**Input file format:**
- One species per line
- Comments with `#` are ignored
- Optional count: `Species name,5`
- Blank lines ignored

### `validate` - Validate species file

```bash
python -m kalimantan_bio_auto.cli validate species.txt
```

### `config` - Show configuration

```bash
python -m kalimantan_bio_auto.cli config --show
```

## Output Structure

### Image Files
```
data/images/
└── {Genus_species}/
    ├── 001_inaturalist_{hash}.jpg
    ├── 002_gbif_{hash}.jpg
    ├── 003_web_search_{hash}.jpg
    └── ...
```

Naming: `{number:03d}_{source}_{md5(url)[:8]}.{ext}`

### Metadata (data/images/metadata.json)

```json
[
  {
    "species": "Pongo pygmaeus",
    "source": "inaturalist",
    "source_url": "https://www.inaturalist.org/observations/394591862",
    "file_path": "images/Pongo_pygmaeus/001_inaturalist_b6142ee5.jpg",
    "file_name": "001_inaturalist_b6142ee5.jpg",
    "width": 2048,
    "height": 1536,
    "file_size_bytes": 773890,
    "mime_type": "image/jpeg",
    "downloaded_at": "2026-08-27T04:58:41.540432+00:00",
    "license": "cc-by-nc",
    "attribution": "(c) Thibaud Rossard, some rights reserved (CC BY-NC)",
    "observation_id": "394591862",
    "taxon_id": 43582
  }
]
```

## Data Sources & Priority

1. **iNaturalist API** (Primary)
   - Research-grade observations only
   - Verified taxonomic IDs
   - CC-BY / CC-BY-NC licenses with attribution

2. **GBIF Occurrence API** (Secondary)
   - Scientific occurrence records with media
   - Species-level matching

3. **DuckDuckGo SafeSearch** (Fallback)
   - Strict safesearch enabled
   - Filtered query: `"{species} species wildlife nature photograph"`
   - Blocked domains: social media, wikimedia, etc.

## Configuration

Default settings (customizable via `ScraperConfig`):

```python
from kalimantan_bio_auto.scraper.config import ScraperConfig

config = ScraperConfig(
    output_dir=Path("data/images"),
    metadata_file=Path("data/images/metadata.json"),
    request_timeout=30,
    download_timeout=60,
    max_concurrent_downloads=5,
    max_retries=3,
    retry_wait=2.0,
    min_image_width=200,
    min_image_height=200,
    max_image_size_mb=50,
    allowed_extensions=(".jpg", ".jpeg", ".png", ".webp"),
)
```

## Python API Usage

```python
import asyncio
from pathlib import Path
from kalimantan_bio_auto.scraper import ScraperPipeline

async def main():
    async with ScraperPipeline() as pipeline:
        # Single species
        result = await pipeline.scrape_single("Pongo pygmaeus", count=10)
        print(f"Downloaded {result.downloaded}/{result.requested}")

        # From file
        results = await pipeline.scrape_from_file(Path("species.txt"), default_count=10)
        for r in results:
            print(f"{r.species}: {r.downloaded}/{r.requested}")

asyncio.run(main())
```

## Requirements

- Python 3.10+
- Dependencies: `httpx`, `pillow`, `ddgs`, `rich`, `typer`, `tenacity`

## License

MIT License - See individual image licenses in metadata.json for usage rights.