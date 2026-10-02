import pandas as pd
import logging
from typing import List, Dict, Any, Optional
from pathlib import Path
from tqdm.asyncio import tqdm_asyncio
import asyncio

from src.api.gbif_client import GBIFClient
from src.cache.sqlite_cache import SQLiteCache
from src.utils.checkpoint import CheckpointManager
from src.utils.validators import validate_scientific_name, normalize_scientific_name

logger = logging.getLogger("pipeline.name_resolution")

class NameResolutionStage:
    def __init__(self, config: Dict, cache: SQLiteCache, checkpoint: CheckpointManager):
        self.config = config
        self.cache = cache
        self.checkpoint = checkpoint
        self.gbif = GBIFClient(cache, **config["apis"]["gbif"])
        self.batch_size = config["pipeline"]["batch_size"]
        self.concurrency_limit = 10  # Limit concurrent requests
    
    async def run(self, input_csv: Path, run_mode: str) -> pd.DataFrame:
        logger.info(f"Starting name resolution for {run_mode} run")
        
        df = pd.read_csv(input_csv)
        
        if run_mode == "test":
            sample_size = self.config["pipeline"]["test_sample_size"]
            df = df.head(sample_size).copy()
            logger.info(f"Test mode: processing {len(df)} species")
        
        if self.checkpoint.is_stage_complete("name_resolution"):
            logger.info("Name resolution already complete, loading cached results")
            return self._load_results(df)
        
        results = []
        total = len(df)
        
        for batch_num in range(0, total, self.batch_size):
            if self.checkpoint.is_batch_complete("name_resolution", batch_num):
                continue
            
            batch = df.iloc[batch_num:batch_num + self.batch_size]
            batch_results = await self._process_batch(batch)
            results.extend(batch_results)
            
            self.checkpoint.mark_batch_complete("name_resolution", batch_num)
        
        self.checkpoint.mark_stage_complete("name_resolution")
        
        result_df = self._merge_results(df, results)
        return result_df
    
    async def _process_batch(self, batch: pd.DataFrame) -> List[Dict]:
        semaphore = asyncio.Semaphore(self.concurrency_limit)
        
        async def resolve_with_semaphore(taxon_name: str, row_id: int):
            async with semaphore:
                return await self._resolve_name(taxon_name, row_id)
        
        tasks = []
        for _, row in batch.iterrows():
            taxon_name = row["Taxon Name"]
            tasks.append(resolve_with_semaphore(taxon_name, row["ID"]))
        
        batch_results = await tqdm_asyncio.gather(*tasks, desc="Resolving names")
        return batch_results
    
    async def _resolve_name(self, taxon_name: str, row_id: int) -> Dict:
        validation = validate_scientific_name(taxon_name)
        if not validation.is_valid:
            logger.warning(f"Invalid scientific name (ID {row_id}): {taxon_name} - {validation.errors}")
        
        normalized = normalize_scientific_name(taxon_name)
        
        match = await self.gbif.match_species(normalized)
        
        if not match or not match.get("usage_key"):
            logger.warning(f"No GBIF match for: {taxon_name}")
            return {
                "row_id": row_id,
                "original_name": taxon_name,
                "normalized_name": normalized,
                "usage_key": None,
                "accepted_usage_key": None,
                "canonical_name": None,
                "rank": None,
                "status": "NO_MATCH",
                "taxonomy": {}
            }
        
        return {
            "row_id": row_id,
            "original_name": taxon_name,
            "normalized_name": normalized,
            "usage_key": match.get("usage_key"),
            "accepted_usage_key": match.get("accepted_usage_key"),
            "canonical_name": match.get("canonical_name"),
            "rank": match.get("rank"),
            "status": match.get("status"),
            "confidence": match.get("confidence"),
            "taxonomy": match.get("taxonomy", {})
        }
    
    def _merge_results(self, df: pd.DataFrame, results: List[Dict]) -> pd.DataFrame:
        result_df = pd.DataFrame(results)
        merged = df.merge(result_df, left_on="ID", right_on="row_id", how="left")
        return merged
    
    def _load_results(self, df: pd.DataFrame) -> pd.DataFrame:
        results = []
        for _, row in df.iterrows():
            taxon_name = row["Taxon Name"]
            cached = self.cache.get_gbif_match(taxon_name)
            if cached:
                results.append({
                    "row_id": row["ID"],
                    "original_name": taxon_name,
                    **cached
                })
        
        return self._merge_results(df, results)
    
    async def close(self):
        await self.gbif.close()