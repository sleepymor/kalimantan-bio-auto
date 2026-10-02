import pandas as pd
import logging
import pdfplumber
import re
import json
from pathlib import Path
from typing import Dict, Any, Optional, List

from src.cache.sqlite_cache import SQLiteCache
from src.utils.checkpoint import CheckpointManager
from src.pipeline.stages.enrichment_base import EnrichmentStage

logger = logging.getLogger("pipeline.p106")

class P106LookupStage(EnrichmentStage):
    def __init__(self, config: Dict, cache: SQLiteCache, checkpoint: CheckpointManager):
        super().__init__(config, cache, checkpoint, "p106")
        self.config = config
        self.pdf_path = Path(config["p106"]["pdf_local_path"])
        self.json_path = Path(config["p106"]["extracted_json"])
        self._protected_species = {}
        self._load_protected_species()
    
    def _load_protected_species(self):
        if self.json_path.exists():
            with open(self.json_path, "r", encoding="utf-8") as f:
                self._protected_species = json.load(f)
            logger.info(f"Loaded {len(self._protected_species)} P106 protected species from cache")
            self._populate_cache()
            return
        
        if self.pdf_path.exists():
            logger.info("Extracting P106 protected species from PDF...")
            self._protected_species = self._extract_from_pdf()
            self.json_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self.json_path, "w", encoding="utf-8") as f:
                json.dump(self._protected_species, f, ensure_ascii=False, indent=2)
            self._populate_cache()
            logger.info(f"Extracted and cached {len(self._protected_species)} P106 protected species")
        else:
            logger.warning(f"P106 PDF not found at {self.pdf_path}. Run download_p106.py first.")
    
    def _extract_from_pdf(self) -> Dict[str, Dict]:
        """Extract protected species from the P106 annex numbered list.
        
        Annex lines look like: `788. Amorphophallus decus-silvae acung jangkung`
        (number, scientific name, Indonesian name). Narrative pages, family
        headers and page markers are skipped.
        """
        species: Dict[str, Dict] = {}
        
        numbered_re = re.compile(r"^(\d+)\.\s+(.+)$")
        genus_re = re.compile(r"^[A-Z][a-z]+$")
        epithet_re = re.compile(r"^[a-z][a-z\-]*\.?$")
        
        def add_entry(scientific: str, indonesian: str):
            tokens = scientific.split()
            if len(tokens) < 2 or not genus_re.match(tokens[0]):
                return
            genus = tokens[0].lower()
            epithet = tokens[1].rstrip(".").lower()
            if epithet in ("spp", "sp", "spp.", "sp."):
                # Genus-level listing (e.g. "Nepenthes spp."): genus key.
                key = f"genus:{genus}"
            elif not epithet_re.match(tokens[1]):
                return
            else:
                key = f"{genus} {epithet}"
            if key not in species:
                species[key] = {
                    "category": "Dilindungi",
                    "indonesian_name": indonesian.strip(),
                }
        
        try:
            with pdfplumber.open(self.pdf_path) as pdf:
                in_annex = False
                for page in pdf.pages:
                    text = page.extract_text()
                    if not text:
                        continue
                    if not in_annex:
                        if "LAMPIRAN" in text[:800]:
                            in_annex = True
                        else:
                            continue
                    pending_indonesian: List[str] = []
                    last_key: Optional[str] = None
                    for raw_line in text.split("\n"):
                        line = raw_line.strip()
                        if not line or re.match(r"^-\s*\d+\s*-$", line):
                            continue
                        m = numbered_re.match(line)
                        if m:
                            rest = m.group(2).strip()
                            tokens = rest.split()
                            if len(tokens) >= 2 and genus_re.match(tokens[0]):
                                sci = " ".join(tokens[:2])
                                indo = " ".join(tokens[2:])
                                before = len(species)
                                add_entry(sci, indo)
                                last_key = list(species.keys())[-1] if len(species) > before else None
                                pending_indonesian = []
                            else:
                                last_key = None
                        elif last_key and pending_indonesian is not None:
                            # Wrapped continuation of the Indonesian name.
                            tokens = line.split()
                            if len(tokens) <= 2 and (
                                genus_re.match(tokens[0])
                                or tokens[0].isupper()
                                or tokens[0].endswith(("ceae", "dae"))
                            ):
                                continue  # family header, not a continuation
                            if line.startswith("Salinan sesuai"):
                                break  # end of annex, signature block follows
                            species[last_key]["indonesian_name"] = (
                                species[last_key]["indonesian_name"] + " " + line
                            ).strip()
        
        except Exception as e:
            logger.error(f"Failed to extract P106 from PDF: {e}")
        
        return species
    
    def _normalize_name(self, name: str) -> str:
        if not name:
            return ""
        tokens = re.sub(r"\s+", " ", name.strip()).split()
        if len(tokens) >= 2:
            return f"{tokens[0].lower()} {tokens[1].rstrip('.').lower()}"
        if len(tokens) == 1:
            return tokens[0].lower()
        return ""
    
    def _lookup_keys(self, canonical: str) -> List[str]:
        """Candidate keys: binomial first, then genus-level (for `Genus spp.` listings)."""
        tokens = re.sub(r"\s+", " ", canonical.strip()).split()
        keys = []
        if len(tokens) >= 2:
            keys.append(f"{tokens[0].lower()} {tokens[1].rstrip('.').lower()}")
            keys.append(f"genus:{tokens[0].lower()}")
        elif tokens:
            keys.append(tokens[0].lower())
        return keys
    
    def _populate_cache(self):
        entries = [
            {"normalized_name": k, "category": v["category"], "indonesian_name": v["indonesian_name"]}
            for k, v in self._protected_species.items()
        ]
        if entries:
            self.cache.bulk_set_p106(entries)
    
    async def _enrich_species(self, usage_key: int, scientific_name: str) -> Optional[Dict]:
        return None
    
    def _load_cached_results(self, df: pd.DataFrame) -> pd.DataFrame:
        logger.info("Applying P106 protected species lookup")
        
        for idx, row in df.iterrows():
            canonical = row.get("canonical_name") or row.get("original_name") or row.get("Taxon Name")
            if not canonical:
                continue
            
            normalized = self._normalize_name(canonical)
            
            protected = None
            for key in self._lookup_keys(canonical):
                protected = self._protected_species.get(key)
                if protected:
                    break
            
            # Also check synonyms
            if not protected:
                synonyms = row.get("synonyms", [])
                if isinstance(synonyms, str):
                    try:
                        import json
                        synonyms = json.loads(synonyms)
                    except Exception:
                        synonyms = []
                
                for syn in synonyms:
                    for key in self._lookup_keys(str(syn)):
                        if key in self._protected_species:
                            protected = self._protected_species[key]
                            break
                    if protected:
                        break
            
            if protected:
                df.at[idx, "Dilindungi P106 (Ya/Tidak)"] = "Ya"
                df.at[idx, "Kategori P106"] = protected.get("category", "Dilindungi")
            else:
                df.at[idx, "Dilindungi P106 (Ya/Tidak)"] = "Tidak"
                df.at[idx, "Kategori P106"] = ""
        
        return df
    
    async def close(self):
        pass