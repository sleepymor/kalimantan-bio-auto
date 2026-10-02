import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, asdict, field

@dataclass
class Checkpoint:
    completed_stages: List[str] = field(default_factory=list)
    completed_batches: Dict[str, List[int]] = field(default_factory=dict)
    total_species: int = 0
    processed_count: int = 0
    last_batch: Dict[str, int] = field(default_factory=dict)
    errors: List[Dict[str, Any]] = field(default_factory=list)
    started_at: str = field(default_factory=lambda: datetime.utcnow().isoformat() + "Z")
    updated_at: str = field(default_factory=lambda: datetime.utcnow().isoformat() + "Z")
    run_mode: str = "test"
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Checkpoint":
        # Backward compat: legacy checkpoint files stored a single shared batch list
        data = dict(data)
        if isinstance(data.get("completed_batches"), list):
            data["completed_batches"] = {}
        if isinstance(data.get("last_batch"), int):
            data["last_batch"] = {}
        return cls(**data)
    
    def update_timestamp(self):
        self.updated_at = datetime.utcnow().isoformat() + "Z"

class CheckpointManager:
    def __init__(self, output_dir: Path, run_mode: str):
        self.output_dir = output_dir
        self.run_mode = run_mode
        self.checkpoint_file = output_dir / f"checkpoint_{run_mode}.json"
        self.checkpoint = Checkpoint(run_mode=run_mode)
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def load(self) -> Checkpoint:
        if self.checkpoint_file.exists():
            with open(self.checkpoint_file, "r", encoding="utf-8") as f:
                data = json.load(f)
                self.checkpoint = Checkpoint.from_dict(data)
        return self.checkpoint
    
    def save(self):
        self.checkpoint.update_timestamp()
        with open(self.checkpoint_file, "w", encoding="utf-8") as f:
            json.dump(self.checkpoint.to_dict(), f, ensure_ascii=False, indent=2)
    
    def mark_stage_complete(self, stage: str):
        if stage not in self.checkpoint.completed_stages:
            self.checkpoint.completed_stages.append(stage)
            self.save()
    
    def mark_batch_complete(self, stage: str, batch_num: int):
        batches = self.checkpoint.completed_batches.setdefault(stage, [])
        if batch_num not in batches:
            batches.append(batch_num)
            self.checkpoint.last_batch[stage] = batch_num
            self.save()
    
    def add_error(self, taxon_name: str, stage: str, error: str):
        self.checkpoint.errors.append({
            "taxon_name": taxon_name,
            "stage": stage,
            "error": error,
            "timestamp": datetime.utcnow().isoformat() + "Z"
        })
        self.save()
    
    def is_stage_complete(self, stage: str) -> bool:
        return stage in self.checkpoint.completed_stages
    
    def is_batch_complete(self, stage: str, batch_num: int) -> bool:
        return batch_num in self.checkpoint.completed_batches.get(stage, [])
    
    def get_last_batch(self, stage: str) -> int:
        return self.checkpoint.last_batch.get(stage, -1)