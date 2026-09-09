import asyncio
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.table import Table

from .scraper.pipeline import ScraperPipeline
from .scraper.config import get_config

app = typer.Typer(
    name="kalimantan-bio-auto",
    help="Species image scraper for biodiversity data collection",
    add_completion=False,
)
console = Console()


def print_results_table(results: list) -> None:
    table = Table(title="Scraping Results", show_header=True, header_style="bold cyan")
    table.add_column("Species", style="green")
    table.add_column("Requested", justify="right")
    table.add_column("Downloaded", justify="right", style="bold green")
    table.add_column("iNaturalist", justify="right")
    table.add_column("GBIF", justify="right")
    table.add_column("Web Search", justify="right")
    table.add_column("Success Rate", justify="right")

    for r in results:
        rate = f"{(r.downloaded / r.requested * 100):.1f}%" if r.requested > 0 else "N/A"
        table.add_row(
            r.species,
            str(r.requested),
            str(r.downloaded),
            str(r.sources.get("inaturalist", 0)),
            str(r.sources.get("gbif", 0)),
            str(r.sources.get("web_search", 0)),
            rate,
        )
    console.print(table)


@app.command()
def scrape(
    file: Optional[Path] = typer.Option(
        None,
        "--file",
        "-f",
        help="Path to species list file (one species per line, or 'species,count' format)",
        exists=True,
        file_okay=True,
        dir_okay=False,
        readable=True,
    ),
    species: Optional[str] = typer.Option(
        None,
        "--species",
        "-s",
        help="Single species scientific name to scrape",
    ),
    count: int = typer.Option(
        10,
        "--count",
        "-c",
        help="Number of images to download per species (default: 10)",
        min=1,
        max=500,
    ),
    output: Path = typer.Option(
        Path("data/images"),
        "--output",
        "-o",
        help="Output directory for downloaded images",
    ),
):
    """
    Scrape species images from iNaturalist, GBIF, and web search (fallback).

    Provide either --file with a species list or --species for a single species.
    """
    if not file and not species:
        console.print("[red]Error: Must provide either --file or --species[/red]")
        raise typer.Exit(1)

    if file and species:
        console.print("[red]Error: Cannot use both --file and --species[/red]")
        raise typer.Exit(1)

    config = get_config()
    config.output_dir = output

    async def run():
        async with ScraperPipeline(config=config, console=console) as pipeline:
            if file:
                results = await pipeline.scrape_from_file(file, default_count=count)
            else:
                result = await pipeline.scrape_single(species, count)
                results = [result]
        return results

    results = asyncio.run(run())
    print_results_table(results)

    total_requested = sum(r.requested for r in results)
    total_downloaded = sum(r.downloaded for r in results)
    console.print(f"\n[bold]Total: {total_downloaded}/{total_requested} images downloaded[/bold]")


@app.command()
def config(
    show: bool = typer.Option(False, "--show", help="Show current configuration"),
):
    """Show or manage configuration"""
    if show:
        cfg = get_config()
        table = Table(title="Current Configuration", show_header=True)
        table.add_column("Setting", style="cyan")
        table.add_column("Value", style="green")
        for field_name in dir(cfg):
            if not field_name.startswith("_"):
                value = getattr(cfg, field_name)
                if not callable(value):
                    table.add_row(field_name, str(value))
        console.print(table)


@app.command()
def validate(
    file: Path = typer.Argument(..., help="Path to species list file to validate", exists=True),
):
    """Validate a species list file format"""
    species_list = []
    with open(file, "r", encoding="utf-8") as f:
        for i, line in enumerate(f, 1):
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split(",")
            species = parts[0].strip()
            count = int(parts[1].strip()) if len(parts) > 1 and parts[1].strip().isdigit() else 10
            species_list.append((species, count))

    if not species_list:
        console.print("[yellow]No valid species found in file[/yellow]")
        return

    table = Table(title=f"Species in {file.name}", show_header=True)
    table.add_column("#", justify="right")
    table.add_column("Species", style="green")
    table.add_column("Count", justify="right")
    for idx, (sp, cnt) in enumerate(species_list, 1):
        table.add_row(str(idx), sp, str(cnt))
    console.print(table)
    console.print(f"\n[bold]Total: {len(species_list)} species[/bold]")


def main():
    app()


if __name__ == "__main__":
    main()