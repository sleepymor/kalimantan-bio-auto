import asyncio
import logging
import yaml
from pathlib import Path
from datetime import datetime

from src.cache.sqlite_cache import SQLiteCache
from src.utils.checkpoint import CheckpointManager
from src.utils.logging import setup_logging, get_logger

from src.pipeline.stages.name_resolution import NameResolutionStage
from src.pipeline.stages.powo_enrichment import POWOEnrichmentStage
from src.pipeline.stages.iucn_enrichment import IUCNEnrichmentStage
from src.pipeline.stages.cites_enrichment import CITESEnrichmentStage
from src.pipeline.stages.wikidata_enrichment import WikidataEnrichmentStage
from src.pipeline.stages.griis_enrichment import GRIISEnrichmentStage
from src.pipeline.stages.localization import LocalizationStage
from src.pipeline.stages.image_download import ImageDownloadStage
from src.pipeline.stages.p106_lookup import P106LookupStage
from src.pipeline.stages.merge_export import MergeExportStage

logger = get_logger("pipeline.orchestrator")

class PipelineOrchestrator:
    def __init__(self, config_path: Path, run_mode: str = "test"):
        self.config_path = config_path
        self.run_mode = run_mode
        self.config = self._load_config()
        
        # Setup paths
        self.data_dir = Path("data")
        self.cache_dir = self.data_dir / "cache"
        self.input_csv = Path(self.config["pipeline"]["input_csv"])
        
        # Setup logging
        log_file = Path("logs") / f"pipeline_{run_mode}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        self.logger = setup_logging(log_file)
        
        # Initialize cache and checkpoint
        self.cache = SQLiteCache(self.cache_dir / "species_cache.db")
        self.checkpoint = CheckpointManager(
            Path(self.config["output"]["test_dir"] if run_mode == "test" else self.config["output"]["full_dir"]),
            run_mode
        )
        
        # Initialize stages
        self.stages = {}
        self._init_stages()
    
    def _load_config(self) -> dict:
        with open(self.config_path, "r") as f:
            return yaml.safe_load(f)
    
    def _init_stages(self):
        self.stages["name_resolution"] = NameResolutionStage(self.config, self.cache, self.checkpoint)
        self.stages["powo"] = POWOEnrichmentStage(self.config, self.cache, self.checkpoint)
        self.stages["iucn"] = IUCNEnrichmentStage(self.config, self.cache, self.checkpoint)
        self.stages["cites"] = CITESEnrichmentStage(self.config, self.cache, self.checkpoint)
        self.stages["wikidata"] = WikidataEnrichmentStage(self.config, self.cache, self.checkpoint)
        self.stages["griis"] = GRIISEnrichmentStage(self.config, self.cache, self.checkpoint)
        self.stages["localization"] = LocalizationStage(self.config, self.cache, self.checkpoint)
        self.stages["images"] = ImageDownloadStage(self.config, self.cache, self.checkpoint, self.run_mode)
        self.stages["p106"] = P106LookupStage(self.config, self.cache, self.checkpoint)
        self.merge_export = MergeExportStage(self.config, self.cache, self.checkpoint, self.run_mode)
    
    async def run(self) -> None:
        self.logger.info(f"Starting pipeline in {self.run_mode} mode")
        start_time = datetime.now()
        
        # Load checkpoint if resuming
        if self.config["pipeline"]["resume_from_checkpoint"]:
            self.checkpoint.load()
            self.logger.info(f"Resumed from checkpoint: {self.checkpoint.checkpoint.processed_count} processed")
        
        try:
            # Stage 1: Name Resolution
            df = await self.stages["name_resolution"].run(self.input_csv, self.run_mode)
            
            # Stages 2a-2e: Parallel Enrichment (run sequentially but each stage processes in batches)
            enrichment_stages = ["powo", "iucn", "cites", "wikidata", "griis"]
            
            for stage_name in enrichment_stages:
                if self.checkpoint.is_stage_complete(stage_name):
                    self.logger.info(f"Skipping {stage_name} - already complete")
                    df = self.stages[stage_name]._load_cached_results(df)
                    continue
                
                df = await self.stages[stage_name].run(df, self.run_mode)
            
            # Stage 3: Localization (Indonesian translation)
            if not self.checkpoint.is_stage_complete("localization"):
                df = await self.stages["localization"].run(df, self.run_mode)
                self.checkpoint.mark_stage_complete("localization")
            else:
                df = self.stages["localization"]._load_cached_results(df)
            
            # Stage 4: Image Download
            if not self.checkpoint.is_stage_complete("images"):
                df = await self.stages["images"].run(df, self.run_mode)
                self.checkpoint.mark_stage_complete("images")
            else:
                df = self.stages["images"]._load_cached_results(df)
            
            # Stage 5: P106 Lookup
            if not self.checkpoint.is_stage_complete("p106"):
                df = await self.stages["p106"].run(df, self.run_mode)
                self.checkpoint.mark_stage_complete("p106")
            else:
                df = self.stages["p106"]._load_cached_results(df)
            
            # Stage 6: Merge & Export
            final_df = self.merge_export.run(df)
            
            # Cleanup
            await self._cleanup()
            
            elapsed = datetime.now() - start_time
            self.logger.info(f"Pipeline completed in {elapsed}")
            self.logger.info(f"Output: {self.merge_export.output_dir}")
            
        except Exception as e:
            self.logger.exception(f"Pipeline failed: {e}")
            await self._cleanup()
            raise
    
    async def _cleanup(self):
        for stage in self.stages.values():
            try:
                await stage.close()
            except Exception as e:
                self.logger.warning(f"Error closing stage: {e}")
        
        self.cache.close()


async def main(run_mode: str = "test"):
    config_path = Path("config/settings.yaml")
    orchestrator = PipelineOrchestrator(config_path, run_mode)
    await orchestrator.run()


if __name__ == "__main__":
    import sys
    mode = sys.argv[1] if len(sys.argv) > 1 else "test"
    asyncio.run(main(mode))