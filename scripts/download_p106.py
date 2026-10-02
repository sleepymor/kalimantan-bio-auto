#!/usr/bin/env python3
"""
Download P106 PDF from JDih
"""

import httpx
from pathlib import Path

P106_URL = "https://jdih.kehutanan.go.id/new2/uploads/files/P_106_2018_JENIS_TSL_menlhk_07252019152513.pdf"

async def download_p106():
    output_path = Path("data/p106/P106_2018.pdf")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    print(f"Downloading P106 PDF from {P106_URL}...")
    
    async with httpx.AsyncClient(timeout=120, follow_redirects=True) as client:
        response = await client.get(P106_URL)
        response.raise_for_status()
        
        with open(output_path, "wb") as f:
            f.write(response.content)
    
    size_mb = len(response.content) / (1024 * 1024)
    print(f"Downloaded P106 PDF: {size_mb:.1f} MB")
    print(f"Saved to: {output_path}")

if __name__ == "__main__":
    import asyncio
    asyncio.run(download_p106())