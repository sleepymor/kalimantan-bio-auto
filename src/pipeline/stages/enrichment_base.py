import logging
from typing import Dict, Any, Optional, List
from pathlib import Path
import pandas as pd
from tqdm.asyncio import tqdm_asyncio
import asyncio
import math

from src.cache.sqlite_cache import SQLiteCache
from src.utils.checkpoint import CheckpointManager

logger = logging.getLogger("pipeline.enrichment")

def _get_valid_usage_key(row: pd.Series) -> Optional[int]:
    """Extract valid usage key from row, handling NaN values."""
    for key in ["accepted_usage_key", "usage_key"]:
        val = row.get(key)
        if val is not None and not (isinstance(val, float) and math.isnan(val)):
            return int(val)
    return None

def _get_scientific_name(row: pd.Series) -> Optional[str]:
    """Extract scientific name from row."""
    for key in ["canonical_name", "original_name", "Taxon Name"]:
        val = row.get(key)
        if val is not None and not (isinstance(val, float) and math.isnan(val)):
            return str(val)
    return None

def merge_results_on_valid_key(df: pd.DataFrame, results: List[Dict]) -> pd.DataFrame:
    """Merge per-species result dicts (keyed by accepted-or-fallback usage key)
    onto the dataframe.
    
    A plain `merge(on="usage_key")` silently drops every row whose accepted
    usage key differs from its GBIF usage key (synonym resolutions), so all
    stages must merge through this helper.
    """
    if not results:
        return df
    res_df = pd.DataFrame(results)
    if "usage_key" in res_df.columns:
        res_df = res_df.rename(columns={"usage_key": "_ekey"})
    keys = [_get_valid_usage_key(row) for _, row in df.iterrows()]
    df = df.copy()
    df["_ekey"] = keys
    df = df.merge(res_df, on="_ekey", how="left")
    return df.drop(columns=["_ekey"])

class EnrichmentStage:
    def __init__(self, config: Dict, cache: SQLiteCache, checkpoint: CheckpointManager, stage_name: str):
        self.config = config
        self.cache = cache
        self.checkpoint = checkpoint
        self.stage_name = stage_name
        self.batch_size = config["pipeline"]["batch_size"]
    
    async def run(self, df: pd.DataFrame, run_mode: str) -> pd.DataFrame:
        logger.info(f"Starting {self.stage_name} enrichment for {run_mode} run")
        
        if self.checkpoint.is_stage_complete(self.stage_name):
            logger.info(f"{self.stage_name} already complete, loading cached results")
            return self._load_cached_results(df)
        
        total = len(df)
        
        for batch_num in range(0, total, self.batch_size):
            if self.checkpoint.is_batch_complete(self.stage_name, batch_num):
                continue
            
            batch = df.iloc[batch_num:batch_num + self.batch_size]
            await self._process_batch(batch)
            
            self.checkpoint.mark_batch_complete(self.stage_name, batch_num)
        
        self.checkpoint.mark_stage_complete(self.stage_name)
        return self._load_cached_results(df)
    
    async def _process_batch(self, batch: pd.DataFrame):
        tasks = []
        for _, row in batch.iterrows():
            usage_key = _get_valid_usage_key(row)
            scientific_name = _get_scientific_name(row)
            
            if usage_key and scientific_name:
                tasks.append(self._enrich_species(usage_key, scientific_name))
        
        if tasks:
            # Per-species failures (dead source, 403, flaky network) must not
            # abort the whole batch/run - they resolve to None via cache miss.
            await tqdm_asyncio.gather(*tasks, desc=self.stage_name, return_exceptions=True)
    
    async def _enrich_species(self, usage_key: int, scientific_name: str) -> Optional[Dict]:
        raise NotImplementedError
    
    def _load_cached_results(self, df: pd.DataFrame) -> pd.DataFrame:
        raise NotImplementedError
    
    async def close(self):
        pass