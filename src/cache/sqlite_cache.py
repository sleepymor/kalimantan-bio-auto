import sqlite3
import json
import hashlib
from pathlib import Path
from typing import Any, Optional, Dict, List
from datetime import datetime
from contextlib import contextmanager
import threading

class SQLiteCache:
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._local = threading.local()
        self._init_db()
    
    def _get_connection(self) -> sqlite3.Connection:
        if not hasattr(self._local, 'conn') or self._local.conn is None:
            self._local.conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
            self._local.conn.row_factory = sqlite3.Row
        return self._local.conn
    
    @contextmanager
    def transaction(self):
        conn = self._get_connection()
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    
    def _init_db(self):
        with self.transaction() as conn:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS gbif_match (
                    taxon_name TEXT PRIMARY KEY,
                    usage_key INTEGER,
                    accepted_usage_key INTEGER,
                    canonical_name TEXT,
                    rank TEXT,
                    status TEXT,
                    taxonomy_json TEXT,
                    fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                
                CREATE TABLE IF NOT EXISTS powo (
                    usage_key INTEGER PRIMARY KEY,
                    description_en TEXT,
                    distribution_json TEXT,
                    image_urls_json TEXT,
                    synonyms_json TEXT,
                    fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                
                CREATE TABLE IF NOT EXISTS iucn (
                    usage_key INTEGER PRIMARY KEY,
                    category TEXT,
                    criteria TEXT,
                    year INTEGER,
                    assessment_id TEXT,
                    fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                
                CREATE TABLE IF NOT EXISTS cites (
                    usage_key INTEGER PRIMARY KEY,
                    appendix TEXT,
                    effective_date TEXT,
                    fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                
                CREATE TABLE IF NOT EXISTS wikidata (
                    usage_key INTEGER PRIMARY KEY,
                    labels_id_json TEXT,
                    labels_other_json TEXT,
                    description_id TEXT,
                    description_en TEXT,
                    native_to_json TEXT,
                    endemic_to_json TEXT,
                    notes_id TEXT,
                    fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                
                CREATE TABLE IF NOT EXISTS translations (
                    source_text_hash TEXT PRIMARY KEY,
                    source_lang TEXT,
                    target_lang TEXT,
                    translated_text TEXT,
                    engine TEXT,
                    translated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                
                CREATE TABLE IF NOT EXISTS images (
                    usage_key INTEGER PRIMARY KEY,
                    source_url TEXT,
                    local_path TEXT,
                    file_size_kb INTEGER,
                    width INTEGER,
                    height INTEGER,
                    status TEXT DEFAULT 'ok',
                    downloaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                
                CREATE TABLE IF NOT EXISTS p106_lookup (
                    normalized_name TEXT PRIMARY KEY,
                    category TEXT,
                    indonesian_name TEXT
                );
                
                CREATE TABLE IF NOT EXISTS localization_cache (
                    usage_key INTEGER PRIMARY KEY,
                    sci_name TEXT,
                    names_id_json TEXT,
                    names_other_json TEXT,
                    inat_name TEXT,
                    wiki_titles_json TEXT,
                    wiki_extract TEXT,
                    wiki_en_extract TEXT,
                    genus_name TEXT,
                    fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                
                CREATE TABLE IF NOT EXISTS synonyms (
                    usage_key INTEGER PRIMARY KEY,
                    names_json TEXT,
                    fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                
                CREATE INDEX IF NOT EXISTS idx_gbif_accepted_key ON gbif_match(accepted_usage_key);
                CREATE INDEX IF NOT EXISTS idx_translations_hash ON translations(source_text_hash);
            """)
            # Migration: add genus_name to existing localization_cache tables.
            cols = [r["name"] for r in conn.execute("PRAGMA table_info(localization_cache)")]
            if cols and "genus_name" not in cols:
                conn.execute("ALTER TABLE localization_cache ADD COLUMN genus_name TEXT")
            icols = [r["name"] for r in conn.execute("PRAGMA table_info(images)")]
            if icols and "status" not in icols:
                conn.execute("ALTER TABLE images ADD COLUMN status TEXT DEFAULT 'ok'")
    
    def get_gbif_match(self, taxon_name: str) -> Optional[Dict]:
        conn = self._get_connection()
        cur = conn.execute("SELECT * FROM gbif_match WHERE taxon_name = ?", (taxon_name,))
        row = cur.fetchone()
        if row:
            data = dict(row)
            raw_taxonomy = data.pop("taxonomy_json", None)
            try:
                data["taxonomy"] = json.loads(raw_taxonomy) if raw_taxonomy else {}
            except (json.JSONDecodeError, TypeError):
                data["taxonomy"] = {}
            data.pop("fetched_at", None)
            data.setdefault("confidence", None)
            data.setdefault("match_type", None)
            return data
        return None
    
    def set_gbif_match(self, taxon_name: str, data: Dict):
        with self.transaction() as conn:
            conn.execute("""
                INSERT OR REPLACE INTO gbif_match 
                (taxon_name, usage_key, accepted_usage_key, canonical_name, rank, status, taxonomy_json)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                taxon_name,
                data.get("usage_key"),
                data.get("accepted_usage_key"),
                data.get("canonical_name"),
                data.get("rank"),
                data.get("status"),
                json.dumps(data.get("taxonomy", {}))
            ))
    
    def get_powo(self, usage_key: int) -> Optional[Dict]:
        conn = self._get_connection()
        cur = conn.execute("SELECT * FROM powo WHERE usage_key = ?", (usage_key,))
        row = cur.fetchone()
        if row:
            data = dict(row)
            for key in ["distribution_json", "image_urls_json", "synonyms_json"]:
                if data[key]:
                    data[key] = json.loads(data[key])
            return data
        return None
    
    def set_powo(self, usage_key: int, data: Dict):
        with self.transaction() as conn:
            conn.execute("""
                INSERT OR REPLACE INTO powo 
                (usage_key, description_en, distribution_json, image_urls_json, synonyms_json)
                VALUES (?, ?, ?, ?, ?)
            """, (
                usage_key,
                data.get("description_en"),
                json.dumps(data.get("distribution", [])),
                json.dumps(data.get("image_urls", [])),
                json.dumps(data.get("synonyms", []))
            ))
    
    def get_iucn(self, usage_key: int) -> Optional[Dict]:
        conn = self._get_connection()
        cur = conn.execute("SELECT * FROM iucn WHERE usage_key = ?", (usage_key,))
        row = cur.fetchone()
        return dict(row) if row else None
    
    def set_iucn(self, usage_key: int, data: Dict):
        with self.transaction() as conn:
            conn.execute("""
                INSERT OR REPLACE INTO iucn 
                (usage_key, category, criteria, year, assessment_id)
                VALUES (?, ?, ?, ?, ?)
            """, (
                usage_key,
                data.get("category"),
                data.get("criteria"),
                data.get("year"),
                data.get("assessment_id")
            ))
    
    def get_cites(self, usage_key: int) -> Optional[Dict]:
        conn = self._get_connection()
        cur = conn.execute("SELECT * FROM cites WHERE usage_key = ?", (usage_key,))
        row = cur.fetchone()
        return dict(row) if row else None
    
    def set_cites(self, usage_key: int, data: Dict):
        with self.transaction() as conn:
            conn.execute("""
                INSERT OR REPLACE INTO cites 
                (usage_key, appendix, effective_date)
                VALUES (?, ?, ?)
            """, (
                usage_key,
                data.get("appendix"),
                data.get("effective_date")
            ))
    
    def get_wikidata(self, usage_key: int) -> Optional[Dict]:
        conn = self._get_connection()
        cur = conn.execute("SELECT * FROM wikidata WHERE usage_key = ?", (usage_key,))
        row = cur.fetchone()
        if row:
            data = dict(row)
            for key in ["labels_id_json", "labels_other_json", "native_to_json", "endemic_to_json"]:
                if data[key]:
                    data[key] = json.loads(data[key])
            return data
        return None
    
    def set_wikidata(self, usage_key: int, data: Dict):
        with self.transaction() as conn:
            conn.execute("""
                INSERT OR REPLACE INTO wikidata 
                (usage_key, labels_id_json, labels_other_json, description_id, description_en, native_to_json, endemic_to_json, notes_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                usage_key,
                json.dumps(data.get("labels_id", [])),
                json.dumps(data.get("labels_other", [])),
                data.get("description_id"),
                data.get("description_en"),
                json.dumps(data.get("native_to", [])),
                json.dumps(data.get("endemic_to", [])),
                data.get("notes_id")
            ))
    
    def get_translation(self, source_text: str, source_lang: str, target_lang: str) -> Optional[str]:
        text_hash = hashlib.sha256(f"{source_lang}:{target_lang}:{source_text}".encode()).hexdigest()
        conn = self._get_connection()
        cur = conn.execute("SELECT translated_text FROM translations WHERE source_text_hash = ?", (text_hash,))
        row = cur.fetchone()
        return row["translated_text"] if row else None
    
    def set_translation(self, source_text: str, source_lang: str, target_lang: str, translated_text: str, engine: str):
        text_hash = hashlib.sha256(f"{source_lang}:{target_lang}:{source_text}".encode()).hexdigest()
        with self.transaction() as conn:
            conn.execute("""
                INSERT OR REPLACE INTO translations 
                (source_text_hash, source_lang, target_lang, translated_text, engine)
                VALUES (?, ?, ?, ?, ?)
            """, (text_hash, source_lang, target_lang, translated_text, engine))
    
    def get_image(self, usage_key: int) -> Optional[Dict]:
        conn = self._get_connection()
        cur = conn.execute("SELECT * FROM images WHERE usage_key = ?", (usage_key,))
        row = cur.fetchone()
        return dict(row) if row else None
    
    def set_image(self, usage_key: int, data: Dict):
        with self.transaction() as conn:
            conn.execute("""
                INSERT OR REPLACE INTO images 
                (usage_key, source_url, local_path, file_size_kb, width, height, status)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                usage_key,
                data.get("source_url"),
                data.get("local_path"),
                data.get("file_size_kb"),
                data.get("width"),
                data.get("height"),
                data.get("status", "ok")
            ))
    
    def get_p106(self, normalized_name: str) -> Optional[Dict]:
        conn = self._get_connection()
        cur = conn.execute("SELECT * FROM p106_lookup WHERE normalized_name = ?", (normalized_name,))
        row = cur.fetchone()
        return dict(row) if row else None
    
    def set_p106(self, normalized_name: str, category: str, indonesian_name: str):
        with self.transaction() as conn:
            conn.execute("""
                INSERT OR REPLACE INTO p106_lookup 
                (normalized_name, category, indonesian_name)
                VALUES (?, ?, ?)
            """, (normalized_name, category, indonesian_name))
    
    def bulk_set_p106(self, entries: List[Dict]):
        with self.transaction() as conn:
            conn.executemany("""
                INSERT OR REPLACE INTO p106_lookup 
                (normalized_name, category, indonesian_name)
                VALUES (?, ?, ?)
            """, [(e["normalized_name"], e["category"], e["indonesian_name"]) for e in entries])
    
    def get_localization(self, usage_key: int) -> Optional[Dict]:
        conn = self._get_connection()
        cur = conn.execute("SELECT * FROM localization_cache WHERE usage_key = ?", (usage_key,))
        row = cur.fetchone()
        if row:
            data = dict(row)
            for key in ["names_id_json", "names_other_json", "wiki_titles_json"]:
                raw = data.pop(key, None)
                try:
                    data[key.replace("_json", "")] = json.loads(raw) if raw else []
                except (json.JSONDecodeError, TypeError):
                    data[key.replace("_json", "")] = []
            data.pop("fetched_at", None)
            return data
        return None
    
    def set_localization(self, usage_key: int, data: Dict):
        with self.transaction() as conn:
            conn.execute("""
                INSERT OR REPLACE INTO localization_cache
                (usage_key, sci_name, names_id_json, names_other_json, inat_name,
                 wiki_titles_json, wiki_extract, wiki_en_extract, genus_name)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                usage_key,
                data.get("sci_name", ""),
                json.dumps(data.get("names_id", [])),
                json.dumps(data.get("names_other", [])),
                data.get("inat_name", ""),
                json.dumps(data.get("wiki_titles", [])),
                data.get("wiki_extract", ""),
                data.get("wiki_en_extract", ""),
                data.get("genus_name", ""),
            ))
    
    def get_synonyms(self, usage_key: int) -> Optional[List[str]]:
        conn = self._get_connection()
        cur = conn.execute("SELECT names_json FROM synonyms WHERE usage_key = ?", (usage_key,))
        row = cur.fetchone()
        if row:
            try:
                return json.loads(row["names_json"]) or []
            except (json.JSONDecodeError, TypeError):
                return []
        return None
    
    def set_synonyms(self, usage_key: int, names: List[str]):
        with self.transaction() as conn:
            conn.execute("""
                INSERT OR REPLACE INTO synonyms (usage_key, names_json)
                VALUES (?, ?)
            """, (usage_key, json.dumps(names or [])))
    
    def close(self):
        if hasattr(self._local, 'conn') and self._local.conn:
            self._local.conn.close()
            self._local.conn = None