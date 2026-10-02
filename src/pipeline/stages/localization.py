import pandas as pd
import logging
import asyncio
import json
import re
import math
import httpx
from typing import Dict, Any, Optional, List, Tuple

from src.api.gbif_client import GBIFClient
from src.api.wikidata_client import strip_generic_description
from src.api.synonyms import fetch_synonyms
from src.translation.translator import Translator
from src.cache.sqlite_cache import SQLiteCache
from src.utils.checkpoint import CheckpointManager
from src.pipeline.stages.enrichment_base import EnrichmentStage, _get_valid_usage_key, _get_scientific_name

logger = logging.getLogger("pipeline.localization")

WIKIPEDIA_SUMMARY_URL = "https://id.wikipedia.org/api/rest_v1/page/summary/{title}"
WIKI_SEM = asyncio.Semaphore(2)


async def _get_with_backoff(
    client: httpx.AsyncClient, url: str, params: Optional[Dict[str, Any]] = None
) -> Optional[httpx.Response]:
    """GET with one Retry-After-aware retry on 429. Returns None on failure."""
    async with WIKI_SEM:
        for attempt in (1, 2):
            try:
                r = await client.get(url, params=params)
            except Exception:
                return None
            if r.status_code == 200:
                return r
            if r.status_code == 429 and attempt == 1:
                retry_after = r.headers.get("Retry-After", "")
                try:
                    delay = min(float(retry_after), 90.0)
                except (ValueError, TypeError):
                    delay = 30.0
                logger.warning(f"Wikipedia 429 - backing off {delay:.0f}s")
                await asyncio.sleep(delay)
                continue
            return None
    return None

def _safe_str(val) -> str:
    """Return stripped string or empty for None/NaN/non-string values."""
    if isinstance(val, str):
        return val.strip()
    return ""


def _row_genus(row: pd.Series) -> str:
    """Genus for a row: original 'Genus' column, else taxonomy genus."""
    genus = _safe_str(row.get("Genus"))
    if not genus:
        tax = row.get("taxonomy")
        if isinstance(tax, dict):
            genus = _safe_str(tax.get("genus"))
    return genus

# Indonesian function words that indicate a descriptive phrase rather than
# a common name (e.g. 'dikenal dengan nama Jeruju' should be just 'Jeruju').
_FUNCTION_WORDS = {
    "dikenal", "dikenali", "dengan", "nama", "sebagai", "adalah", "ialah",
    "merupakan", "disebut", "juga", "yaitu", "atau", "yang", "dari", "ini",
}


def _is_real_common_name(candidate: str, scientific_name: str) -> bool:
    """Reject Latin names leaking in as common names.

    Wikidata/GBIF labels sometimes repeat the binomial (or a misspelled
    variant) - those are not Indonesian common names.
    """
    cand = (candidate or "").strip()
    if not cand:
        return False
    sci = (scientific_name or "").strip()
    if cand.lower() == sci.lower():
        return False
    parts = sci.split()
    genus = parts[0].lower() if parts else ""
    epithet = parts[1].rstrip(".").lower() if len(parts) > 1 else ""
    if cand.lower() == genus:
        return False
    if re.match(r"^[A-Z][a-z]+\s+[a-z\-]+$", cand):
        cgen, cepi = cand.lower().split()
        if cgen == genus or cepi == epithet or cgen.rstrip("s") == genus or genus in cgen:
            return False
    # Reject common names containing taxonomic authority suffixes or
    # infraspecific ranks (e.g. "Achyranthes bidentata Blume").
    if _AUTHORITY_SUFFIXES.search(cand):
        return False
    # Reject descriptive phrases ('dikenal dengan nama Jeruju').
    cand_words = set(cand.lower().split())
    if cand_words & _FUNCTION_WORDS:
        return False
    return True


def _is_list_title(title: str) -> bool:
    """Reject Wikipedia list/category pages ('Daftar spesies Shorea').

    Their titles and extracts describe a list, not the taxon.
    """
    t = (title or "").strip().lower()
    return bool(re.match(r"^(daftar|list of|lists of|kategori|category)\b", t))


_LEAD_NAME_RE = re.compile(
    r"^([A-ZÀ-ÖØ-ÞœŒÆ][\w\-\' ]{0,80}?)\s+(?:adalah|ialah|merupakan|dalah)\b"
)

# Taxonomic authority / rank suffixes that leak into common-name phrases.
_AUTHORITY_SUFFIXES = re.compile(
    r"(?:\s+(?:Blume|L\.|DC\.|Hook\.|Arn\.|Roxb\.|Korth\.|Miq\.|Scheff\.|"
    r"Perr\.|Bedd\.|King|Ridley|Ridl\.|Backer|Bakh\.f\.|Steen\.|Airy Shaw|"
    r"Hook\.f\.|Steud\.|Merr\.|Valeton|Burck|C.B\.Rob\.|Griff\.|Baill\.|"
    r"Kunth|Willd\.|Baill\.|Stafh\.|Reinw\.|Zoll\.|J.J\.Sm\.|Kurz|Teijsm\.|"
    r"Binn\.|Foxw\.|Carr\.|Náves|Merr\.|Filipino)\b"
    r"|\s+(?:var\.|subsp\.|f\.|ssp\.)\b"
    r"|\s+\d+$)"
)


def _extract_lead_name(extract: str) -> List[str]:
    """Harvest vernacular noun-phrases from the lead clause of an id.wiki
    extract. The name precedes 'adalah/ialah/merupakan', often in the form
    'X atau Y adalah ...' (e.g. 'Daun gedi atau aibika adalah ...'). The
    scientific genus also sits in that position ('Acalypha atau bantinuh
    adalah ...'), so candidates are validated later with
    _is_real_common_name against the scientific name.
    """
    t = (extract or "").strip()
    if not t:
        return []
    m = _LEAD_NAME_RE.match(t)
    if not m:
        return []
    phrase = m.group(1).strip()
    # "Acanthus atau dikenal dengan nama Jeruju adalah..."
    if " atau " in phrase:
        parts = [p.strip() for p in phrase.split(" atau ")]
    else:
        parts = [phrase]
    # Strip common Indonesian preambles that precede the real name.
    _PREAMBLE = re.compile(
        r"^(?:yang\s+)?dikenal\s+(?:dengan\s+)?(?:nama|juga)\s+",
        re.IGNORECASE,
    )
    out: List[str] = []
    for p in parts:
        p = _PREAMBLE.sub("", p)
        p = _AUTHORITY_SUFFIXES.sub("", p)
        p = p.strip(" ,.—:;()")
        if 2 <= len(p) <= 40 and re.match(r"^[A-ZÀ-ÖØ-Þa-zà-öø-ÿ][\w\- ]*$", p):
            out.append(p)
    return out

class LocalizationStage(EnrichmentStage):
    def __init__(self, config: Dict, cache: SQLiteCache, checkpoint: CheckpointManager):
        super().__init__(config, cache, checkpoint, "localization")
        self.translator = Translator(
            cache,
            engine=config["translation"]["engine"],
            target_lang=config["translation"]["target_language"],
            source_lang=config["translation"]["source_language"]
        )
        self.strategy = config["translation"]["strategy"]
        self.gbif_config = config["apis"]["gbif"]
        # Prefetched per usage_key: {"names_id": [...], "names_other": [...], "wiki_extract": "..."}
        self._prefetch: Dict[int, Dict[str, Any]] = {}
    
    async def run(self, df: pd.DataFrame, run_mode: str) -> pd.DataFrame:
        # Prefetch external Indonesian sources concurrently before the
        # (synchronous) merge step, including on resume paths.
        await self._prefetch_sources(df)
        try:
            return await super().run(df, run_mode)
        finally:
            self._prefetch = {}
    
    async def _prefetch_sources(self, df: pd.DataFrame) -> None:
        gbif = GBIFClient(self.cache, **self.gbif_config)
        sem = asyncio.Semaphore(5)
        try:
            keys: List[int] = []
            sci_by_key: Dict[int, str] = {}
            genus_by_key: Dict[int, str] = {}
            acc_by_key: Dict[int, Optional[int]] = {}
            labels_by_key: Dict[int, Any] = {}
            for _, row in df.iterrows():
                uk = _get_valid_usage_key(row)
                if uk and uk not in keys:
                    keys.append(uk)
                    sci_by_key[uk] = _get_scientific_name(row) or ""
                    genus_by_key[uk] = _row_genus(row)
                    acc_val = row.get("accepted_usage_key")
                    acc_by_key[uk] = int(acc_val) if acc_val is not None and not (
                        isinstance(acc_val, float) and math.isnan(acc_val)
                    ) else None
                    if uk not in labels_by_key:
                        labels_by_key[uk] = row.get("labels_id_json")
            
            # Seed from persistent cache (incl. negative results) so reruns
            # and resumes don't re-hammer free APIs.
            todo_keys: List[int] = []
            for uk in keys:
                cached = self.cache.get_localization(uk)
                if cached is not None:
                    self._prefetch[uk] = {
                        "names_id": cached.get("names_id", []),
                        "names_other": cached.get("names_other", []),
                        "inat_name": cached.get("inat_name", ""),
                        "wiki_titles": cached.get("wiki_titles", []),
                        "wiki_extract": cached.get("wiki_extract", ""),
                        "wiki_en_extract": cached.get("wiki_en_extract", ""),
                        "genus_name": cached.get("genus_name", ""),
                    }
                else:
                    todo_keys.append(uk)
            
            # Phase 1: GBIF vernacular names, also trying the accepted key
            # (recent splits have their vernaculars on the accepted taxon).
            async def fetch_vernacular(uk: int, acc: Optional[int]) -> Tuple[int, List[str], List[str]]:
                async with sem:
                    names_id: List[str] = []
                    names_other: List[str] = []
                    for key in dict.fromkeys([k for k in (uk, acc) if k]):
                        try:
                            nid, noth = await gbif.get_indonesian_vernacular(key)
                        except Exception:
                            continue
                        for n in nid:
                            if n not in names_id:
                                names_id.append(n)
                        for n in noth:
                            if n not in names_other:
                                names_other.append(n)
                    return uk, names_id, names_other
            
            for uk, names_id, names_other in await asyncio.gather(
                *[fetch_vernacular(uk, acc_by_key.get(uk)) for uk in todo_keys]
            ):
                entry = self._prefetch.setdefault(uk, {})
                entry["names_id"] = names_id
                entry["names_other"] = names_other
            
            # Phase 1b: GBIF synonyms (used for defined fallbacks and by the
            # IUCN/CITES clients). Only needed for keys we are fetching.
            for uk in todo_keys:
                try:
                    syns = await fetch_synonyms(self.cache, gbif.client, uk, timeout=gbif.timeout)
                    if syns:
                        self._prefetch.setdefault(uk, {})["synonyms"] = syns
                except Exception as e:
                    logger.warning(f"Synonym fetch failed for {uk}: {e}")
            
            # Phase 2: id.wikipedia summary per row (needs vernacular for titles).
            async with httpx.AsyncClient(
                timeout=20, headers={"User-Agent": "KalimantanBioEnrichment/1.0"}
            ) as client:
                async def fetch_wiki(titles: List[str]) -> str:
                    for title in titles:
                        if not title:
                            continue
                        r = await _get_with_backoff(
                            client,
                            WIKIPEDIA_SUMMARY_URL.format(
                                title=title.strip().replace(" ", "_")
                            ),
                        )
                        if r is not None:
                            extract = (r.json().get("extract") or "").strip()
                            if extract:
                                return extract[:1500]
                    return ""
                
                jobs: List[Tuple[Optional[int], List[str]]] = []
                for _, row in df.iterrows():
                    uk = _get_valid_usage_key(row)
                    sci = _get_scientific_name(row) or ""
                    vern_id = (self._prefetch.get(uk, {}).get("names_id") or [None])[0] if uk else None
                    titles = [t for t in [vern_id or "", sci, str(row.get("Taxon Name", ""))] if t]
                    jobs.append((uk, titles))
                
                extracts = await asyncio.gather(*[fetch_wiki(t) for _, t in jobs])
                for (uk, _), extract in zip(jobs, extracts):
                    if uk and extract:
                        self._prefetch.setdefault(uk, {})["wiki_extract"] = extract
                        # Harvest the vernacular noun-phrase from the lead
                        # clause so names that only appear in extract prose
                        # (e.g. 'Daun gedi' for Abelmoschus manihot) are kept.
                        entry = self._prefetch.setdefault(uk, {})
                        entry.setdefault("names_id", [])
                        for lead in _extract_lead_name(extract):
                            if lead not in entry["names_id"]:
                                entry["names_id"].append(lead)
                
                # Phase 2b: en.wikipedia summary for rows still without a
                # description (translated later via Translator + MyMemory).
                async def fetch_wiki_en(title: str) -> str:
                    if not title:
                        return ""
                    r = await _get_with_backoff(
                        client,
                        f"https://en.wikipedia.org/api/rest_v1/page/summary/{title.strip().replace(' ', '_')}",
                    )
                    if r is not None:
                        return ((r.json().get("extract") or "").strip())[:1200]
                    return ""
                
                need_en = [
                    (uk, sci) for uk, sci in
                    ((_get_valid_usage_key(row), _get_scientific_name(row) or "") for _, row in df.iterrows())
                    if uk and not self._prefetch.get(uk, {}).get("wiki_extract")
                ]
                en_extracts = await asyncio.gather(*[fetch_wiki_en(sci) for _, sci in need_en])
                for (uk, _), extract in zip(need_en, en_extracts):
                    if uk and extract:
                        self._prefetch.setdefault(uk, {})["wiki_en_extract"] = extract
                
                # Phase 3: search-API fallback, only for rows still missing a
                # name or description after deterministic sources.
                # 3a. iNaturalist Indonesian preferred common name (locale=id).
                async def fetch_inat_name(sci: str) -> str:
                    async with sem:
                        if not sci:
                            return ""
                        try:
                            r = await client.get(
                                "https://api.inaturalist.org/v1/taxa",
                                params={"q": sci, "rank": "species",
                                        "per_page": 3, "locale": "id"},
                            )
                            if r.status_code != 200:
                                return ""
                            for t in r.json().get("results", []):
                                pref = (t.get("preferred_common_name") or "").strip()
                                if pref and pref.lower() != sci.lower():
                                    return pref
                        except Exception:
                            pass
                        return ""
                
                # 3b. Wikipedia search API resolves pages titled by common
                # name (e.g. "Jeringau" for Acorus calamus) that phase-2
                # title guessing misses. Resolved titles double as candidate
                # Indonesian names when no other source has one.
                async def wiki_search_extract(sci: str, lang: str) -> tuple[List[str], str]:
                    if not sci:
                        return [], ""
                    r = await _get_with_backoff(
                        client,
                        f"https://{lang}.wikipedia.org/w/api.php",
                        params={"action": "query", "list": "search",
                                "srsearch": sci, "srlimit": 5,
                                "format": "json"},
                    )
                    if r is None:
                        return [], ""
                    try:
                        hits = r.json().get("query", {}).get("search", [])
                    except Exception:
                        return [], ""
                    titles: List[str] = []
                    best_extract = ""
                    for h in hits:
                        title = h.get("title", "")
                        if not title or "disambiguasi" in title.lower():
                            continue
                        if _is_list_title(title):
                            continue
                        s = await _get_with_backoff(
                            client,
                            f"https://{lang}.wikipedia.org/api/rest_v1/page/summary/{title.replace(' ', '_')}",
                        )
                        if s is None:
                            continue
                        try:
                            data = s.json()
                        except Exception:
                            continue
                        if data.get("type") == "disambiguation":
                            continue
                        extract = (data.get("extract") or "").strip()
                        if extract:
                            if title not in titles:
                                titles.append(title)
                            if not best_extract:
                                cap = 1500 if lang == "id" else 1200
                                best_extract = extract[:cap]
                        if len(titles) >= 3:
                            break
                    return titles, best_extract
                
                def _has_wiki_label(val) -> bool:
                    if isinstance(val, str):
                        return bool(_safe_str(val))
                    if isinstance(val, list):
                        return any(str(v).strip() for v in val)
                    return False
                
                need_name, need_desc_id, need_desc_en = [], [], []
                for _, row in df.iterrows():
                    uk = _get_valid_usage_key(row)
                    if not uk:
                        continue
                    entry = self._prefetch.get(uk, {})
                    sci = _get_scientific_name(row) or ""
                    has_name = _has_wiki_label(row.get("labels_id_json")) \
                        or bool(entry.get("names_id"))
                    has_desc = bool(_safe_str(row.get("description_id"))) or bool(entry.get("wiki_extract"))
                    if not has_name:
                        need_name.append((uk, sci))
                    # Resolve id.wiki titles when the name OR the description
                    # is missing - titles double as name candidates.
                    if not has_desc or not has_name:
                        need_desc_id.append((uk, sci))
                        if not has_desc and not entry.get("wiki_en_extract"):
                            need_desc_en.append((uk, sci))
                
                for (uk, _), name in zip(need_name, await asyncio.gather(
                    *[fetch_inat_name(sci) for _, sci in need_name]
                )):
                    if uk and name:
                        entry = self._prefetch.setdefault(uk, {})
                        entry["inat_name"] = name
                        entry.setdefault("names_id", [])
                        if name not in entry["names_id"]:
                            entry["names_id"].append(name)
                
                for (uk, _), (titles, extract) in zip(need_desc_id, await asyncio.gather(
                    *[wiki_search_extract(sci, "id") for _, sci in need_desc_id]
                )):
                    if uk:
                        if extract:
                            self._prefetch.setdefault(uk, {})["wiki_extract"] = extract
                            for lead in _extract_lead_name(extract):
                                entry = self._prefetch.setdefault(uk, {})
                                entry.setdefault("names_id", [])
                                if lead not in entry["names_id"]:
                                    entry["names_id"].append(lead)
                        entry = self._prefetch.setdefault(uk, {})
                        entry.setdefault("wiki_titles", [])
                        for title in titles:
                            if title not in entry["wiki_titles"]:
                                entry["wiki_titles"].append(title)

                for (uk, _), (titles, extract) in zip(need_desc_en, await asyncio.gather(
                    *[wiki_search_extract(sci, "en") for _, sci in need_desc_en]
                )):
                    if uk and extract:
                        self._prefetch.setdefault(uk, {})["wiki_en_extract"] = extract
                
                # 3c. Genus-level Indonesian names as a last resort (e.g.
                # "Meranti" for Shorea/Anthoshorea), for species whose own
                # name is still missing after every species-level source.
                async def fetch_inat_genus_name(genus: str) -> str:
                    async with sem:
                        if not genus:
                            return ""
                        try:
                            r = await client.get(
                                "https://api.inaturalist.org/v1/taxa",
                                params={"q": genus, "per_page": 5, "locale": "id"},
                            )
                            if r.status_code != 200:
                                return ""
                            for t in r.json().get("results", []):
                                pref = (t.get("preferred_common_name") or "").strip()
                                if pref and pref.lower() != genus.lower():
                                    return pref
                        except Exception:
                            pass
                        return ""
                
                need_genus: List[Tuple[Optional[int], str]] = []
                for uk in keys:
                    entry = self._prefetch.get(uk, {})
                    if entry.get("names_id") or entry.get("wiki_titles") or entry.get("genus_name"):
                        continue
                    if _has_wiki_label(labels_by_key.get(uk)):
                        continue
                    need_genus.append((uk, genus_by_key.get(uk, "")))
                
                for (uk, _), gname in zip(need_genus, await asyncio.gather(
                    *[fetch_inat_genus_name(g) for _, g in need_genus]
                )):
                    if uk and gname:
                        self._prefetch.setdefault(uk, {})["genus_name"] = gname
                
                # Persist (incl. negative results) so reruns/resumes skip refetching.
                for uk in keys:
                    entry = self._prefetch.get(uk, {})
                    try:
                        self.cache.set_localization(uk, {
                            "sci_name": sci_by_key.get(uk, ""),
                            "names_id": entry.get("names_id", []),
                            "names_other": entry.get("names_other", []),
                            "inat_name": entry.get("inat_name", ""),
                            "wiki_titles": entry.get("wiki_titles", []),
                            "wiki_extract": entry.get("wiki_extract", ""),
                            "wiki_en_extract": entry.get("wiki_en_extract", ""),
                            "genus_name": entry.get("genus_name", ""),
                        })
                    except Exception as e:
                        logger.warning(f"Failed to cache localization for {uk}: {e}")
        finally:
            await gbif.close()
    
    async def _enrich_species(self, usage_key: int, scientific_name: str) -> Optional[Dict]:
        # Localization is done in _load_cached_results after all enrichment
        return None
    
    def _load_cached_results(self, df: pd.DataFrame) -> pd.DataFrame:
        logger.info("Applying Indonesian localization")
        # Seed _prefetch from the persistent cache so resume paths (which
        # skip _prefetch_sources) still have names_id/wiki_extract available
        # for the resolver.
        self._seed_prefetch_from_cache(df)
        
        # Process each row
        for idx, row in df.iterrows():
            # Common name (Indonesian)
            nama_umum_id = self._get_indonesian_common_name(row)
            df.at[idx, "Nama Umum (Indonesia)"] = nama_umum_id
            
            # Other common names
            nama_umum_lain = self._get_other_common_names(row)
            df.at[idx, "Nama Umum (Lainnya)"] = nama_umum_lain
            
            # Description
            deskripsi, translated_desc = self._get_description(row)
            df.at[idx, "Deskripsi (Indonesia)"] = deskripsi
            
            # Notes
            catatan, translated_notes = self._get_notes(row)
            df.at[idx, "Catatan (Indonesia)"] = catatan
            
            # Translation flag
            any_translated = translated_desc or translated_notes
            df.at[idx, "Translated (Ya/Tidak)"] = "Ya" if any_translated else "Tidak"
        
        return df
    
    def _seed_prefetch_from_cache(self, df: pd.DataFrame) -> None:
        """Populate _prefetch from the persistent localization cache for any
        usage keys not already present (used by resume paths)."""
        seen = set(self._prefetch.keys())
        for _, row in df.iterrows():
            uk = _get_valid_usage_key(row)
            if not uk or uk in seen:
                continue
            cached = self.cache.get_localization(uk)
            if cached is None:
                continue
            self._prefetch[uk] = {
                "names_id": cached.get("names_id", []),
                "names_other": cached.get("names_other", []),
                "inat_name": cached.get("inat_name", ""),
                "wiki_titles": cached.get("wiki_titles", []),
                "wiki_extract": cached.get("wiki_extract", ""),
                "wiki_en_extract": cached.get("wiki_en_extract", ""),
                "genus_name": cached.get("genus_name", ""),
            }
            seen.add(uk)
    
    def _get_indonesian_common_name(self, row: pd.Series) -> str:
        # Priority: Wikidata id labels > GBIF vernacular (ind) > iNaturalist
        # locale=id > Wikipedia extract lead-names > resolved Wikipedia
        # titles > genus-level name. Latin-looking labels are rejected.
        sci = _get_scientific_name(row) or ""
        
        # Wikidata Indonesian labels
        labels_id = row.get("labels_id_json")
        if labels_id:
            if isinstance(labels_id, str):
                try:
                    labels_id = json.loads(labels_id)
                except Exception:
                    labels_id = []
            if labels_id and isinstance(labels_id, list):
                for label in labels_id:
                    if _is_real_common_name(str(label), sci):
                        return str(label).strip()
        
        # GBIF vernacular + iNaturalist names (prefetched)
        uk = _get_valid_usage_key(row)
        entry = self._prefetch.get(uk, {}) if uk else {}
        if uk:
            names_id = entry.get("names_id") or []
            for name in names_id:
                if _is_real_common_name(str(name), sci):
                    return str(name).strip()
            # Indonesian noun-phrases harvested from id.wiki extract prose
            # (covers cached rows where names_id predates the harvest).
            for lead in _extract_lead_name(entry.get("wiki_extract") or ""):
                if _is_real_common_name(lead, sci):
                    return lead
            # Resolved Wikipedia titles ("Mersawa" page for Anisoptera
            # reticulata). List pages are rejected.
            for title in entry.get("wiki_titles") or []:
                if not _is_list_title(str(title)) and _is_real_common_name(str(title), sci):
                    return str(title).strip()
            # Genus-level iNaturalist name (e.g. "Meranti" group) as a last
            # resort for species with no published Indonesian name.
            genus_name = entry.get("genus_name") or ""
            if genus_name and _is_real_common_name(str(genus_name), sci):
                return str(genus_name).strip()
        
        return ""
    
    def _get_other_common_names(self, row: pd.Series) -> str:
        seen, out = set(), []
        def add(names):
            for n in names or []:
                key = str(n).strip()
                if key and key.lower() not in seen:
                    seen.add(key.lower())
                    out.append(key)
        
        labels_other = row.get("labels_other_json")
        if labels_other:
            if isinstance(labels_other, str):
                try:
                    labels_other = json.loads(labels_other)
                except Exception:
                    labels_other = []
            if isinstance(labels_other, list):
                add(labels_other)
        
        uk = _get_valid_usage_key(row)
        if uk:
            add(self._prefetch.get(uk, {}).get("names_other"))
        
        return "; ".join(out[:10])
    
    def _get_description(self, row: pd.Series) -> tuple[str, bool]:
        # Priority: Wikidata id description > id.wikipedia summary (native) >
        #   en.wikipedia summary (translated) > Translate POWO/GBIF English
        # Generic "species of plant" boilerplate (incl. older cache rows) is
        # skipped so real prose wins.
        
        desc_id = strip_generic_description(_safe_str(row.get("description_id")))
        if desc_id:
            return desc_id, False
        
        # id.wikipedia summary in Indonesian (prefetched, native - no translation flag)
        uk = _get_valid_usage_key(row)
        if uk:
            wiki = strip_generic_description(
                (self._prefetch.get(uk, {}).get("wiki_extract") or "").strip()
            )
            if wiki:
                return wiki, False
            
            # en.wikipedia summary translated to Indonesian
            wiki_en = strip_generic_description(
                (self._prefetch.get(uk, {}).get("wiki_en_extract") or "").strip()
            )
            if wiki_en:
                translated = self.translator.translate(wiki_en)
                if translated and strip_generic_description(translated):
                    return translated, True
        
        # Try POWO description (wikidata description_en included - both filtered)
        desc_en = strip_generic_description(_safe_str(row.get("description_en")))
        
        if desc_en:
            translated = self.translator.translate(desc_en)
            if translated and strip_generic_description(translated):
                return translated, True
        
        return "", False
    
    def _get_notes(self, row: pd.Series) -> tuple[str, bool]:
        notes_id = strip_generic_description(_safe_str(row.get("notes_id")))
        if notes_id:
            return notes_id, False
        
        # Derive honest notes from status data already collected for this row.
        parts: List[str] = []
        
        iucn = _safe_str(row.get("IUCN Category"))
        if iucn and iucn != "NE":
            cat_id = {
                "LC": "Risiko Rendah",
                "NT": "Hampir Terancam",
                "VU": "Rentan",
                "EN": "Genting",
                "CR": "Kritis",
                "DD": "Data Kurang",
                "EX": "Punah",
                "EW": "Punah di Alam Liar",
            }.get(iucn, iucn)
            note = f"Status konservasi internasional: {cat_id} ({iucn}, IUCN)"
            year = row.get("IUCN Year")
            year_txt = ""
            if year is not None and str(year).strip() not in ("", "nan"):
                try:
                    year_txt = str(int(float(str(year).strip())))
                except (ValueError, TypeError):
                    year_txt = str(year).strip()
            if year_txt:
                note += f", penilaian {year_txt}"
            criteria = _safe_str(row.get("IUCN Criteria"))
            if criteria:
                note += f" ({criteria})"
            parts.append(note + ".")
        
        cites = _safe_str(row.get("CITES Appendix"))
        if cites in ("I", "II", "III"):
            parts.append(f"Tercantum dalam Lampiran CITES {cites}.")
        
        if parts:
            return " ".join(parts), False
        
        # Could add more note sources here
        return "", False
    
    async def close(self):
        pass