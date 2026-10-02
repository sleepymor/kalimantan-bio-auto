#!/usr/bin/env python3
"""
Download GRIIS Indonesia invasive species data from GBIF DwC Archive
"""

import httpx
import zipfile
import csv
import io
import json
from pathlib import Path

DWCA_URL = "https://cloud.gbif.org/griis/archive.do?r=griis-indonesia"

async def download_griis():
    output_path = Path("data/cache/griis_indonesia.json")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    print(f"Downloading GRIIS DwC Archive from {DWCA_URL}...")
    
    async with httpx.AsyncClient(timeout=120, follow_redirects=True) as client:
        response = await client.get(DWCA_URL)
        response.raise_for_status()
        
        zip_bytes = response.content
    
    print(f"Downloaded {len(zip_bytes)} bytes")
    
    # Extract and process
    invasive = set()
    introduced = set()
    
    with zipfile.ZipFile(io.BytesIO(zip_bytes), 'r') as z:
        # Build taxon lookup: id -> scientificName
        taxon_map = {}
        if "taxon.txt" in z.namelist():
            with z.open("taxon.txt") as f:
                content = f.read().decode('utf-8')
                reader = csv.DictReader(io.StringIO(content), delimiter='\t')
                for row in reader:
                    taxon_id = row.get("id", "").strip()
                    scientific_name = row.get("scientificName", "").strip().lower()
                    if taxon_id and scientific_name:
                        taxon_map[taxon_id] = scientific_name
        
        print(f"Loaded {len(taxon_map)} taxa")
        
        # Process speciesprofile.txt with invasive flag
        if "speciesprofile.txt" in z.namelist():
            with z.open("speciesprofile.txt") as f:
                content = f.read().decode('utf-8')
                reader = csv.DictReader(io.StringIO(content), delimiter='\t')
                for row in reader:
                    taxon_id = row.get("id", "").strip()
                    is_invasive = row.get("isInvasive", "").strip().lower() == "invasive"
                    scientific_name = taxon_map.get(taxon_id, "").lower()
                    if scientific_name:
                        if is_invasive:
                            invasive.add(scientific_name)
                        else:
                            introduced.add(scientific_name)
    
    print(f"Processed: {len(invasive)} invasive, {len(introduced)} introduced species")
    
    output = {
        "invasive": sorted(list(invasive)),
        "introduced": sorted(list(introduced))
    }
    
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    
    print(f"Saved GRIIS data to {output_path}")

if __name__ == "__main__":
    import asyncio
    asyncio.run(download_griis())