#!/usr/bin/env python3
"""
Kalimantan Species Data Enrichment Pipeline

Usage:
    python -m src.main --mode test      # Run on 100 species (test)
    python -m src.main --mode full      # Run on all 9,154 species
    python -m src.main --mode full --resume  # Resume interrupted full run
"""

import asyncio
import argparse
import sys
from pathlib import Path

# Load environment variables
from dotenv import load_dotenv
load_dotenv(Path("config/.env"))

from src.pipeline.orchestrator import main

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kalimantan Species Data Enrichment Pipeline")
    parser.add_argument("--mode", choices=["test", "full"], default="test", help="Run mode")
    parser.add_argument("--resume", action="store_true", help="Resume from checkpoint")
    
    args = parser.parse_args()
    
    if args.resume:
        import yaml
        with open("config/settings.yaml", "r") as f:
            config = yaml.safe_load(f)
        config["pipeline"]["resume_from_checkpoint"] = True
        with open("config/settings.yaml", "w") as f:
            yaml.dump(config, f)
    
    asyncio.run(main(args.mode))