import pandas as pd
import logging
import httpx
import json
from typing import Dict, Any, Optional, Set
from pathlib import Path

from src.cache.sqlite_cache import SQLiteCache
from src.utils.checkpoint import CheckpointManager
from src.pipeline.stages.enrichment_base import EnrichmentStage, _get_valid_usage_key, merge_results_on_valid_key

logger = logging.getLogger("pipeline.griis")

class GRIISEnrichmentStage(EnrichmentStage):
    def __init__(self, config: Dict, cache: SQLiteCache, checkpoint: CheckpointManager):
        super().__init__(config, cache, checkpoint, "griis")
        self.griis_invasive: Set[str] = set()
        self.griis_introduced: Set[str] = set()
        self._load_griis_data()
    
    def _load_griis_data(self):
        # GRIIS Indonesia dataset from GBIF
        # Download the checklist data
        griis_file = Path("data/cache/griis_indonesia.json")
        if griis_file.exists():
            with open(griis_file, "r") as f:
                data = json.load(f)
                self.griis_invasive = set(data.get("invasive", []))
                self.griis_introduced = set(data.get("introduced", []))
            logger.info(f"Loaded GRIIS data: {len(self.griis_invasive)} invasive, {len(self.griis_introduced)} introduced")
        else:
            logger.warning("GRIIS data not found. Run download_griis.py first.")
    
    async def _enrich_species(self, usage_key: int, scientific_name: str) -> Optional[Dict]:
        # This is a lookup, not an API call
        return None
    
    def _load_cached_results(self, df: pd.DataFrame) -> pd.DataFrame:
        results = []
        for _, row in df.iterrows():
            usage_key = _get_valid_usage_key(row)
            canonical = row.get("canonical_name") or row.get("original_name") or row.get("Taxon Name")
            if not canonical:
                results.append({"usage_key": usage_key, "Tipe": "asli"})
                continue
            
            # Check synonyms from POWO
            synonyms = row.get("synonyms", [])
            if isinstance(synonyms, str):
                try:
                    synonyms = json.loads(synonyms)
                except:
                    synonyms = []
            
            all_names = {canonical.lower().strip()}
            for syn in synonyms:
                all_names.add(syn.lower().strip())
            
            # Determine type
            tipo = "asli"
            for name in all_names:
                if name in self.griis_invasive:
                    tipo = "invasif"
                    break
                elif name in self.griis_introduced:
                    tipo = "asli"
            
            # If not in GRIIS, check Wikidata endemicTo
            if tipo == "asli":
                endemic_to = row.get("endemic_to", [])
                if isinstance(endemic_to, str):
                    try:
                        endemic_to = json.loads(endemic_to)
                    except:
                        endemic_to = []
                
                for loc in endemic_to:
                    label = loc.get("label", "").lower()
                    if "indonesia" in label or "kalimantan" in label or "borneo" in label:
                        tipo = "endemic"
                        break
                
                if tipo == "asli":
                    native_to = row.get("native_to", [])
                    if isinstance(native_to, str):
                        try:
                            native_to = json.loads(native_to)
                        except:
                            native_to = []
                    
                    for loc in native_to:
                        label = loc.get("label", "").lower()
                        if "indonesia" in label or "kalimantan" in label or "borneo" in label:
                            tipo = "asli"
                            break
            
            results.append({"usage_key": usage_key, "Tipe": tipo})
        
        if results:
            df = merge_results_on_valid_key(df, results)
        
        return df
    
    async def close(self):
        pass