import pandas as pd
import logging
from typing import Dict, Any, Optional

from src.api.cites_client import CITESClient
from src.cache.sqlite_cache import SQLiteCache
from src.utils.checkpoint import CheckpointManager
from src.pipeline.stages.enrichment_base import EnrichmentStage, _get_valid_usage_key, merge_results_on_valid_key

logger = logging.getLogger("pipeline.cites")

class CITESEnrichmentStage(EnrichmentStage):
    def __init__(self, config: Dict, cache: SQLiteCache, checkpoint: CheckpointManager):
        super().__init__(config, cache, checkpoint, "cites")
        self.cites = CITESClient(cache, **config["apis"]["cites"])
    
    async def _enrich_species(self, usage_key: int, scientific_name: str) -> Optional[Dict]:
        return await self.cites.enrich(usage_key, scientific_name)
    
    def _load_cached_results(self, df: pd.DataFrame) -> pd.DataFrame:
        results = []
        for _, row in df.iterrows():
            usage_key = _get_valid_usage_key(row)
            if usage_key:
                cached = self.cache.get_cites(usage_key)
                if cached:
                    results.append({"usage_key": usage_key, **cached})
        
        if results:
            cites_df = pd.DataFrame(results)
            cites_df = cites_df.rename(columns={
                "appendix": "CITES Appendix",
                "effective_date": "CITES Effective Date",
                "party": "CITES Party",
                "annotation": "CITES Annotation"
            })
            df = merge_results_on_valid_key(df, cites_df.to_dict("records"))
        
        return df
    
    async def close(self):
        await self.cites.close()