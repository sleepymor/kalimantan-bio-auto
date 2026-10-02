import pandas as pd
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List

from src.cache.sqlite_cache import SQLiteCache
from src.utils.checkpoint import CheckpointManager
from src.pipeline.stages.enrichment_base import EnrichmentStage

logger = logging.getLogger("pipeline.merge_export")

class MergeExportStage:
    def __init__(self, config: Dict, cache: SQLiteCache, checkpoint: CheckpointManager, run_mode: str):
        self.config = config
        self.cache = cache
        self.checkpoint = checkpoint
        self.run_mode = run_mode
        self.output_dir = Path(config["output"]["test_dir"] if run_mode == "test" else config["output"]["full_dir"])
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def run(self, df: pd.DataFrame) -> pd.DataFrame:
        logger.info(f"Merging and exporting results for {self.run_mode} run")
        
        # Build final DataFrame with all required columns
        final_df = self._build_final_dataframe(df)
        
        # Write CSV
        output_file = self.output_dir / (f"enriched_test_100.csv" if self.run_mode == "test" else "enriched_kalimantan_species.csv")
        final_df.to_csv(output_file, index=False, encoding="utf-8")
        logger.info(f"Exported {len(final_df)} rows to {output_file}")
        
        # Write unmatched species (computed from the pre-export df so the
        # usage_key column is still present).
        if "usage_key" in df.columns:
            unmatched_mask = df["usage_key"].isna()
        else:
            unmatched_mask = pd.Series([False] * len(df), index=df.index)
        
        if "status" in df.columns:
            unmatched_mask = unmatched_mask | (df["status"] == "NO_MATCH")
        
        unmatched = df[unmatched_mask]
        if len(unmatched) > 0:
            unmatched_file = self.output_dir / "unmatched_species.csv"
            cols = ["ID", "Taxon Name", "Genus", "Family"]
            if "status" in unmatched.columns:
                cols.append("status")
            unmatched[cols].to_csv(unmatched_file, index=False)
            logger.warning(f"{len(unmatched)} unmatched species written to {unmatched_file}")
        
        # Generate summary
        self._generate_summary(final_df, df)
        
        return final_df
    
    def _build_final_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        # Define the final column order and names
        column_mapping = {
            "ID": "ID",
            "Taxon Name": "Taxon Name",
            "Genus": "Genus",
            "Family": "Family",
            "kingdom": "Kingdom",
            "phylum": "Phylum",
            "class": "Class",
            "order": "Order",
            "family": "Family (confirmed)",
            "genus": "Genus (confirmed)",
            "canonical_name": "Scientific Name (accepted)",
            "Nama Umum (Indonesia)": "Nama Umum (Indonesia)",
            "Nama Umum (Lainnya)": "Nama Umum (Lainnya)",
            "Tipe": "Tipe",
            "URL Gambar": "URL Gambar",
            "File Gambar (WebP)": "File Gambar (WebP)",
            "Deskripsi (Indonesia)": "Deskripsi (Indonesia)",
            "Catatan (Indonesia)": "Catatan (Indonesia)",
            "IUCN Category": "Kategori IUCN",
            "IUCN Criteria": "Kriteria IUCN",
            "IUCN Year": "Tahun Penilaian",
            "CITES Appendix": "Lampiran CITES",
            "CITES Effective Date": "Tanggal Pencantuman CITES",
            "Dilindungi P106 (Ya/Tidak)": "Dilindungi P106 (Ya/Tidak)",
            "Kategori P106": "Kategori P106",
            "Translated (Ya/Tidak)": "Translated (Ya/Tidak)"
        }
        
        # Extract taxonomy from taxonomy_json
        taxonomy_data = []
        for _, row in df.iterrows():
            taxonomy = row.get("taxonomy", {})
            if isinstance(taxonomy, str):
                try:
                    import json
                    taxonomy = json.loads(taxonomy)
                except:
                    taxonomy = {}
            
            taxonomy_data.append({
                "kingdom": taxonomy.get("kingdom", "Plantae"),
                "phylum": taxonomy.get("phylum", ""),
                "class": taxonomy.get("class", ""),
                "order": taxonomy.get("order", ""),
                "family": taxonomy.get("family", ""),
                "genus": taxonomy.get("genus", "")
            })
        
        taxonomy_df = pd.DataFrame(taxonomy_data)
        df = pd.concat([df.reset_index(drop=True), taxonomy_df], axis=1)
        
        # Fill missing columns with empty strings
        for col in column_mapping.keys():
            if col not in df.columns:
                df[col] = ""
        
        # Select and rename columns
        final_columns = [col for col in column_mapping.keys() if col in df.columns]
        final_df = df[final_columns].copy()
        final_df = final_df.rename(columns=column_mapping)
        
        # Add metadata columns
        final_df["Sumber Data Utama"] = "GBIF+POWO+IUCN+CITES+Wikidata+GRIIS+P106"
        final_df["Terakhir Diperbarui"] = datetime.utcnow().isoformat() + "Z"
        
        # Per-row explicit statuses: a blank cell means "not checked", while
        # NE / Tidak Tercantum mean "checked and not assessed / not listed".
        if "Tipe" in final_df.columns:
            final_df["Tipe"] = final_df["Tipe"].fillna("asli").replace("", "asli")
        else:
            final_df["Tipe"] = "asli"
        
        if "Kategori IUCN" in final_df.columns:
            final_df["Kategori IUCN"] = (
                final_df["Kategori IUCN"].replace("", None).fillna("NE")
            )
        else:
            final_df["Kategori IUCN"] = "NE"
        
        if "Lampiran CITES" in final_df.columns:
            final_df["Lampiran CITES"] = (
                final_df["Lampiran CITES"]
                .replace("", None)
                .fillna("Tidak Tercantum")
                .replace("NOT_LISTED", "Tidak Tercantum")
            )
        else:
            final_df["Lampiran CITES"] = "Tidak Tercantum"
        
        if "Dilindungi P106 (Ya/Tidak)" not in final_df.columns:
            final_df["Dilindungi P106 (Ya/Tidak)"] = "Tidak"
        
        if "Translated (Ya/Tidak)" not in final_df.columns:
            final_df["Translated (Ya/Tidak)"] = "Tidak"
        
        return final_df
    
    def _count_filled(self, series: pd.Series) -> int:
        """Count cells holding real content (empty strings do not count)."""
        if series is None:
            return 0
        return int(
            series.apply(
                lambda v: pd.notna(v)
                and str(v).strip() not in ("", "nan", "None")
            ).sum()
        )
    
    def _generate_summary(self, df: pd.DataFrame, source_df: pd.DataFrame = None):
        total = len(df)
        if source_df is not None and "usage_key" in source_df.columns:
            matched = int(source_df["usage_key"].notna().sum())
        else:
            matched = 0
        
        iucn = df["Kategori IUCN"] if "Kategori IUCN" in df.columns else pd.Series([], dtype=object)
        cites = df["Lampiran CITES"] if "Lampiran CITES" in df.columns else pd.Series([], dtype=object)
        
        summary = {
            "total_species": total,
            "gbif_matched": int(matched),
            "match_rate": f"{matched/total*100:.1f}%" if total > 0 else "0%",
            "with_indonesian_name": self._count_filled(df["Nama Umum (Indonesia)"]) if "Nama Umum (Indonesia)" in df.columns else 0,
            "with_description": self._count_filled(df["Deskripsi (Indonesia)"]) if "Deskripsi (Indonesia)" in df.columns else 0,
            "with_iucn_assessed": int(iucn.isin(["EX", "CR", "EN", "VU", "NT", "LC", "DD"]).sum()),
            "iucn_ne": int(iucn.eq("NE").sum()),
            "with_cites_listed": int((cites.astype(str).str.strip()).isin(["I", "II", "III"]).sum()),
            "cites_unlisted": int((cites.astype(str).str.strip()).eq("Tidak Tercantum").sum()),
            "with_p106": int((df["Dilindungi P106 (Ya/Tidak)"] == "Ya").sum()) if "Dilindungi P106 (Ya/Tidak)" in df.columns else 0,
            "with_images": int(df["File Gambar (WebP)"].fillna("").astype(str).str.strip().ne("").sum()) if "File Gambar (WebP)" in df.columns else 0,
            "translated_fields": int((df["Translated (Ya/Tidak)"] == "Ya").sum()) if "Translated (Ya/Tidak)" in df.columns else 0,
            "tipo_distribution": df["Tipe"].value_counts().to_dict() if "Tipe" in df.columns else {},
            "iucn_distribution": iucn.value_counts().to_dict() if not iucn.empty else {},
            "cites_distribution": cites.value_counts().to_dict() if not cites.empty else {},
        }
        
        summary_file = self.output_dir / f"summary_{self.run_mode}.json"
        import json
        with open(summary_file, "w", encoding="utf-8") as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)
        
        logger.info(f"Summary: {summary}")