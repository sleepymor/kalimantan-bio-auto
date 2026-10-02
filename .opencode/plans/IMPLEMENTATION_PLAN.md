# Kalimantan Species Data Enrichment — Implementation Plan

## Overview
Enrich 9,154 Kalimantan vascular plant species with taxonomy, Indonesian descriptions, conservation status, CITES, P106 protection, endemic/invasive/native classification, and WebP images (≤300KB).

---

## Project Structure

```
kalimantan-bio-auto/
├── config/
│   ├── .env                    # API tokens (gitignored)
│   └── settings.yaml           # Runtime config
├── src/
│   ├── __init__.py
│   ├── main.py                 # CLI entry point
│   ├── pipeline/
│   │   ├── __init__.py
│   │   ├── orchestrator.py     # Main pipeline coordination
│   │   ├── stages/
│   │   │   ├── __init__.py
│   │   │   ├── name_resolution.py      # Stage 1: GBIF match
│   │   │   ├── powo_enrichment.py      # Stage 2a: POWO
│   │   │   ├── iucn_enrichment.py      # Stage 2b: IUCN
│   │   │   ├── cites_enrichment.py     # Stage 2c: CITES
│   │   │   ├── wikidata_enrichment.py  # Stage 2d: Wikidata SPARQL
│   │   │   ├── griis_enrichment.py     # Stage 2e: GRIIS Indonesia
│   │   │   ├── localization.py         # Stage 3: Indonesian text
│   │   │   ├── image_download.py       # Stage 4: Download + WebP
│   │   │   └── p106_lookup.py          # Stage 5: P106 protected
│   │   └── merge_export.py             # Stage 6: Merge + CSV output
│   ├── api/
│   │   ├── __init__.py
│   │   ├── gbif_client.py
│   │   ├── powo_client.py
│   │   ├── iucn_client.py
│   │   ├── cites_client.py
│   │   └── wikidata_client.py
│   ├── cache/
│   │   ├── __init__.py
│   │   └── sqlite_cache.py
│   ├── translation/
│   │   ├── __init__.py
│   │   └── translator.py
│   ├── p106/
│   │   ├── __init__.py
│   │   └── pdf_extractor.py
│   ├── images/
│   │   ├── __init__.py
│   │   └── webp_converter.py
│   └── utils/
│       ├── __init__.py
│       ├── logging.py
│       ├── checkpoint.py
│       └── validators.py
├── data/
│   ├── input/
│   │   └── kalimantan_species.csv      # Original CSV (symlink or copy)
│   ├── output/
│   │   ├── test_run/
│   │   │   ├── enriched_test_100.csv
│   │   │   └── images_test/
│   │   └── full_run/
│   │       ├── enriched_kalimantan_species.csv
│   │       └── images/
│   ├── cache/
│   │   └── species_cache.db
│   └── p106/
│       ├── P106_2018.pdf               # Downloaded PDF
│       └── protected_species.json      # Extracted lookup
├── tests/
│   ├── test_gbif_match.py
│   ├── test_powo.py
│   ├── test_translation.py
│   └── test_webp_conversion.py
├── logs/
│   └── pipeline.log
├── requirements.txt
├── Dockerfile
├── .gitignore
└── README.md
```

---

## Configuration

### `config/.env` (gitignored)
```bash
# Required - user provides
IUCN_REDLIST_API_TOKEN=your_token_here
SPECIESPLUS_API_TOKEN=your_token_here

# Optional
GOOGLE_TRANSLATE_API_KEY=           # If using paid Google Translate
LIBRETRANSLATE_URL=http://localhost:5000  # If self-hosted
```

### `config/settings.yaml`
```yaml
pipeline:
  input_csv: "data/input/kalimantan_species.csv"
  test_mode: true
  test_sample_size: 100
  batch_size: 50
  max_concurrent_workers: 5
  checkpoint_interval: 500
  resume_from_checkpoint: true

apis:
  gbif:
    base_url: "https://api.gbif.org/v1"
    rate_limit_per_sec: 100
    timeout_sec: 30
  powo:
    base_url: "https://powo.science.kew.org/api/2"
    rate_limit_per_sec: 2
    timeout_sec: 30
  iucn:
    base_url: "https://api.iucnredlist.org/api/v4"
    rate_limit_per_sec: 0.5
    timeout_sec: 60
  cites:
    base_url: "https://api.speciesplus.net/api/v1"
    rate_limit_per_sec: 1
    timeout_sec: 60
  wikidata:
    sparql_endpoint: "https://query.wikidata.org/sparql"
    rate_limit_per_sec: 1
    timeout_sec: 60

images:
  max_download_mb: 5
  max_dimension_px: 1600
  webp_quality: 75
  target_max_kb: 300
  concurrent_downloads: 5
  timeout_sec: 30
  output_dir: "data/output/{run_mode}/images"

translation:
  strategy: "hybrid"  # wikidata_id_first, then translate gaps
  engine: "google"    # or "libretranslate"
  target_language: "id"
  source_language: "en"
  cache_translations: true

p106:
  pdf_url: "https://jdih.kehutanan.go.id/new2/home/portfolioDetails3/P_106_2018_JENIS_TSL_menlhk_07252019152513.pdf/106/2018/4/949"
  pdf_local_path: "data/p106/P106_2018.pdf"
  extracted_json: "data/p106/protected_species.json"

output:
  test_dir: "data/output/test_run"
  full_dir: "data/output/full_run"
  schema_version: "1.0"
```

---

## Output CSV Schema (Final)

| Column | Source | Indonesian | Notes |
|--------|--------|------------|-------|
| `ID` | Input | - | Original row ID |
| `Taxon Name` | Input | - | Original name |
| `Genus` | Input | - | Original genus |
| `Family` | Input | - | Original family |
| `Kingdom` | GBIF | - | `Plantae` |
| `Phylum` | GBIF | - | e.g., `Tracheophyta` |
| `Class` | GBIF | - | e.g., `Magnoliopsida` |
| `Order` | GBIF | - | e.g., `Malvales` |
| `Family (confirmed)` | GBIF | - | Matched family |
| `Genus (confirmed)` | GBIF | - | Matched genus |
| `Scientific Name (accepted)` | GBIF | - | Accepted name from backbone |
| `Nama Umum (Indonesia)` | Wikidata `id` labels / GBIF vernacular | ✅ | Primary Indonesian common name |
| `Nama Umum (Lainnya)` | Wikidata other labels / GBIF | ✅ | Semicolon-separated alternatives |
| `Tipe` | GRIIS + Wikidata `nativeTo`/`endemicTo` | ✅ | `endemic` / `invasif` / `asli` |
| `URL Gambar` | POWO / GBIF / Wikidata | - | Best source image URL |
| `File Gambar (WebP)` | Local | - | `images/{usageKey}.webp` |
| `Deskripsi (Indonesia)` | Wikidata `id` desc → translate POWO/GBIF | ✅ | Full description |
| `Catatan (Indonesia)` | Wikidata `id` statements → translate | ✅ | Additional notes |
| `Kategori IUCN` | IUCN API | - | `LC`, `VU`, `EN`, `CR`, `DD`, `NE`, etc. |
| `Kriteria IUCN` | IUCN API | - | e.g., `B1ab(iii)` |
| `Tahun Penilaian` | IUCN API | - | Assessment year |
| `Lampiran CITES` | Species+ API | - | `I`, `II`, `III`, or `Tidak Tercantum` |
| `Tanggal Pencantuman CITES` | Species+ API | - | Effective listing date |
| `Dilindungi P106 (Ya/Tidak)` | P106 PDF lookup | ✅ | `Ya` / `Tidak` |
| `Kategori P106` | P106 PDF lookup | ✅ | Protection category from regulation |
| `Sumber Data Utama` | Pipeline | - | `GBIF+POWO+IUCN+CITES+Wikidata+GRIIS+P106` |
| `Terakhir Diperbarui` | Pipeline | - | ISO timestamp |
| `Translated (Ya/Tidak)` | Pipeline | ✅ | `Ya` if any field machine-translated |

---

## Stage Details

### Stage 1: Name Resolution (GBIF)
**Input**: `Taxon Name` from CSV  
**Process**:
1. POST `/v1/species/match` with `name`, `strict=false`, `verbose=true`
2. Extract `usageKey`, `acceptedUsageKey`, `canonicalName`, `rank`, `status` (ACCEPTED/SYNONYM)
3. GET `/v1/species/{acceptedUsageKey}` for full taxonomy hierarchy
4. Handle varieties/subspecies: match at species level, preserve infraspecific epithet
5. Cache all responses keyed by `usageKey`

**Output**: `name_resolution_{batch}.json` + cache entries

**Error Handling**:
- No match → try fuzzy (remove authorship, try genus-only)
- Multiple matches → pick `status=ACCEPTED` with highest `confidence`
- Log unmatched to `unmatched_species.csv` for manual review

---

### Stage 2a: POWO Enrichment
**Input**: `usageKey`, accepted scientific name  
**Process**:
1. Search POWO: `GET /search?q={name}&f=species_f:true`
2. Get taxon detail: `GET /taxon/urn:lsid:ipni.org:names:{id}`
3. Extract: description, distribution, images (prioritize `thumbnail` → `medium` → `original`), synonyms
4. Cache response

**Output**: Description (EN), distribution, image URLs, synonyms

---

### Stage 2b: IUCN Enrichment
**Input**: Accepted scientific name  
**Process**:
1. GET `/api/v4/taxa/scientific_name/{name}` → get assessment IDs
2. GET latest assessment: `/api/v4/assessments/{id}`
3. Extract: `red_list_category` (code), `criteria`, `year_published`, `assessment_id`
4. Cache response

**Rate Limit**: 1 request per 2 seconds (strict)

---

### Stage 2c: CITES Enrichment
**Input**: Accepted scientific name  
**Process**:
1. Search taxon concept: `GET /taxon_concepts?q={name}&kingdom=plantae`
2. GET legislation: `/taxon_concepts/{taxon_concept_id}/cites_legislation`
3. Extract: current appendix (`I`, `II`, `III`), `effective_at` date, `is_current`
4. Cache response

---

### Stage 2d: Wikidata Enrichment (SPARQL)
**Input**: Accepted scientific name, GBIF usageKey  
**Process**:
1. SPARQL query for entity by scientific name (P225) or GBIF ID (P846)
2. Fetch:
   - Labels in `id` (Indonesian) → `Nama Umum (Indonesia)`
   - Labels in other languages → `Nama Umum (Lainnya)`
   - Description in `id` → `Deskripsi (Indonesia)` primary
   - `nativeTo` (P183), `endemicTo` (P183 with qualifier) → `Tipe`
   - Statements for notes → `Catatan (Indonesia)`
3. Cache response

**SPARQL Template**:
```sparql
SELECT ?item ?itemLabel_id ?itemLabel_en ?desc_id ?desc_en ?nativeTo ?endemicTo WHERE {
  ?item wdt:P225 "{scientific_name}" .
  OPTIONAL { ?item rdfs:label ?itemLabel_id FILTER(LANG(?itemLabel_id) = "id") }
  OPTIONAL { ?item rdfs:label ?itemLabel_en FILTER(LANG(?itemLabel_en) = "en") }
  OPTIONAL { ?item schema:description ?desc_id FILTER(LANG(?desc_id) = "id") }
  OPTIONAL { ?item schema:description ?desc_en FILTER(LANG(?desc_en) = "en") }
  OPTIONAL { ?item wdt:P183 ?nativeTo }
  OPTIONAL { ?item wdt:P183 ?endemicTo . ?endemicTo pq:P131 ?region }
}
```

---

### Stage 2e: GRIIS Indonesia (Invasive Status)
**Input**: Accepted scientific name  
**Process**:
1. Download GRIIS Indonesia dataset from GBIF: `https://api.gbif.org/v1/dataset/61fb216d-1216-4287-8b78-fdfef45e8e18/occurrence` or use checklist download
2. Build local lookup set of invasive species names (accepted + synonyms)
3. Match accepted name + synonyms against set
4. Classify:
   - In GRIIS + `invasiveness=invasive` → `invasif`
   - In GRIIS + `invasiveness=introduced` → `asli` (introduced but not invasive)
   - Not in GRIIS + Wikidata `endemicTo` includes Indonesia/Kalimantan → `endemic`
   - Not in GRIIS + Wikidata `nativeTo` includes Indonesia → `asli`
   - Default → `asli`

---

### Stage 3: Indonesian Localization
**Input**: All English text from Stages 2a-2d, Wikidata Indonesian fields  
**Process**:
1. For each text field (description, notes):
   - If Wikidata `id` description exists → use as primary
   - Else → translate English text via Google Translate / LibreTranslate
   - Flag `translated=true` if any translation used
2. For common names:
   - Prioritize Wikidata `id` labels
   - Fallback: GBIF vernacularNames with `language=id`
   - Fallback: Translate English common name
3. Cache translations to avoid re-translation

**Translation Engine**: `deep_translator.GoogleTranslator` (free) with fallback to LibreTranslate

---

### Stage 4: Image Download + WebP Conversion
**Input**: Image URLs from POWO (primary), GBIF media, Wikidata Commons  
**Process**:
1. For each species, select best image URL (priority: POWO > GBIF > Wikidata)
2. Stream download with `httpx` (max 5MB, 30s timeout)
3. Validate: content-type image/*, not HTML error page
4. Convert with Pillow:
   ```python
   img = Image.open(BytesIO(content))
   img.thumbnail((1600, 1600), Image.LANCZOS)
   if img.mode in ('RGBA', 'LA', 'P'):
       img = img.convert('RGB')
   output = BytesIO()
   img.save(output, format='WEBP', quality=75, method=6)
   # If >300KB, reduce quality iteratively
   while output.tell() > 300 * 1024 and quality > 30:
       quality -= 5
       output = BytesIO()
       img.save(output, format='WEBP', quality=quality, method=6)
   ```
5. Save to `data/output/{run_mode}/images/{usageKey}.webp`
6. Record local path in enrichment data

**Skip Logic**: If file exists and size ≤300KB → skip download

---

### Stage 5: P106 Protected Species Lookup
**Input**: Accepted scientific name + synonyms  
**Process**:
1. Download PDF from JDih link (one-time)
2. Extract text with `pdfplumber` or `PyMuPDF`
3. Parse annex tables: scientific name, Indonesian name, protection category
4. Normalize names (strip authorship, lowercase)
5. Build lookup dict: `{normalized_name: {category, indonesian_name}}`
6. Match: accepted name + all synonyms against lookup keys
7. Output: `Dilindungi P106 (Ya/Tidak)`, `Kategori P106`

**Note**: PDF parsing is one-time; save extracted JSON for future runs.

---

### Stage 6: Merge & Export
**Input**: All stage outputs keyed by `usageKey`  
**Process**:
1. Load input CSV, join with name resolution → get `usageKey` per row
2. Left-join all enrichment data on `usageKey`
3. Validate required fields present
4. Write test CSV (first 100) and full CSV
5. Generate summary stats: match rates, coverage per field, errors

---

## Caching Strategy

**SQLite Database**: `data/cache/species_cache.db`

Tables:
```sql
CREATE TABLE gbif_match (
    taxon_name TEXT PRIMARY KEY,
    usage_key INTEGER,
    accepted_usage_key INTEGER,
    canonical_name TEXT,
    rank TEXT,
    status TEXT,
    taxonomy_json TEXT,
    fetched_at TIMESTAMP
);

CREATE TABLE powo (
    usage_key INTEGER PRIMARY KEY,
    description_en TEXT,
    distribution_json TEXT,
    image_urls_json TEXT,
    synonyms_json TEXT,
    fetched_at TIMESTAMP
);

CREATE TABLE iucn (
    usage_key INTEGER PRIMARY KEY,
    category TEXT,
    criteria TEXT,
    year INTEGER,
    assessment_id TEXT,
    fetched_at TIMESTAMP
);

CREATE TABLE cites (
    usage_key INTEGER PRIMARY KEY,
    appendix TEXT,
    effective_date TEXT,
    fetched_at TIMESTAMP
);

CREATE TABLE wikidata (
    usage_key INTEGER PRIMARY KEY,
    labels_id_json TEXT,
    labels_other_json TEXT,
    description_id TEXT,
    description_en TEXT,
    native_to_json TEXT,
    endemic_to_json TEXT,
    notes_id TEXT,
    fetched_at TIMESTAMP
);

CREATE TABLE translations (
    source_text_hash TEXT PRIMARY KEY,
    source_lang TEXT,
    target_lang TEXT,
    translated_text TEXT,
    engine TEXT,
    translated_at TIMESTAMP
);

CREATE TABLE images (
    usage_key INTEGER PRIMARY KEY,
    source_url TEXT,
    local_path TEXT,
    file_size_kb INTEGER,
    width INTEGER,
    height INTEGER,
    downloaded_at TIMESTAMP
);

CREATE TABLE p106_lookup (
    normalized_name TEXT PRIMARY KEY,
    category TEXT,
    indonesian_name TEXT
);
```

---

## Checkpointing & Resume

**Checkpoint File**: `data/output/{run_mode}/checkpoint_{timestamp}.json`
```json
{
  "completed_stages": ["name_resolution", "powo", "iucn", "cites", "wikidata", "griis"],
  "completed_batches": [0, 1, 2, ...],
  "total_species": 9154,
  "processed_count": 4500,
  "last_batch": 90,
  "errors": [
    {"taxon_name": "...", "stage": "iucn", "error": "..."}
  ],
  "started_at": "2026-09-09T10:00:00Z",
  "updated_at": "2026-09-09T14:30:00Z"
}
```

**Resume Logic**: On startup, load latest checkpoint → skip completed batches/stages

---

## Test Run Workflow

```bash
# 1. Set test_mode=true in settings.yaml
# 2. Run pipeline
python -m src.main --mode test

# Output:
# data/output/test_run/
#   ├── enriched_test_100.csv
#   ├── images_test/ (100 WebP files)
#   ├── checkpoint_test.json
#   └── test_run_log.txt

# 3. Review test output:
#    - Check Indonesian translation quality
#    - Verify image sizes ≤300KB
#    - Validate all required columns populated
#    - Confirm match rate >95%
```

---

## Full Run Workflow

```bash
# 1. Set test_mode=false in settings.yaml
# 2. Run pipeline
python -m src.main --mode full

# Output:
# data/output/full_run/
#   ├── enriched_kalimantan_species.csv
#   ├── images/ (9,154 WebP files, ~3GB)
#   ├── checkpoint_full_final.json
#   ├── full_run_log.txt
#   └── unmatched_species.csv (if any)
```

---

## Requirements (requirements.txt)

```txt
# Core
pandas>=2.1
httpx>=0.27
tenacity>=8.2
tqdm>=4.66
PyYAML>=6.0
python-dotenv>=1.0

# APIs
# (custom clients, no extra deps)

# Database
sqlite3 (stdlib)

# Images
Pillow>=10.0

# PDF
pdfplumber>=0.11

# Translation
deep-translator>=1.11

# SPARQL
sparqlwrapper>=2.0

# Utils
rich>=13.0  # pretty logging
```

---

## Dockerfile (Optional)

```dockerfile
FROM python:3.11-slim

WORKDIR /app

# System deps for Pillow, pdfplumber
RUN apt-get update && apt-get install -y \
    libjpeg-dev zlib1g-dev libpng-dev \
    poppler-utils \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/
COPY config/settings.yaml ./config/
COPY data/input/ ./data/input/

# Run test by default
CMD ["python", "-m", "src.main", "--mode", "test"]
```

---

## Execution Commands

```bash
# Setup
cd /home/tiramissu/Projects/automations/kalimantan-bio-auto
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Configure
cp config/.env.example config/.env
# Edit .env with your IUCN and Species+ tokens

# Test run (100 species)
python -m src.main --mode test

# Review test output, then full run
python -m src.main --mode full

# Resume interrupted run
python -m src.main --mode full --resume
```

---

## Monitoring & Logging

- **Console**: `rich` progress bars per stage, batch, species
- **File**: `logs/pipeline.log` (JSON lines for parsing)
- **Metrics**: Match rate, API success rate, translation count, image success rate, runtime per stage

---

## Error Handling & Data Quality

| Scenario | Handling |
|----------|----------|
| GBIF no match | Log to `unmatched_species.csv`, continue with original name |
| API timeout/5xx | Retry 3x with exponential backoff (tenacity) |
| API 429 (rate limit) | Back off 60s, retry |
| Missing Indonesian description | Translate English, flag `translated=true` |
| Image download fail | Log, leave `File Gambar (WebP)` blank |
| P106 PDF parse fail | Manual fallback: provide CSV of protected names |
| Duplicate usageKey | Keep first, log warning |

---

## Attribution Requirements

Output CSV must include source attribution in `Sumber Data Utama` and README:
- GBIF.org (taxonomy, names, IUCN category)
- POWO (Kew) (descriptions, distributions, images)
- IUCN Red List (conservation assessments)
- CITES / Species+ (trade listings)
- Wikidata (Indonesian labels, descriptions, endemism)
- GRIIS / ISSG (invasive status Indonesia)
- PerMen LHK P.106/2018 (Indonesian protected species)

---

## Next Steps

1. **User provides**: IUCN token, Species+ token
2. **I implement**: Full pipeline per this plan
3. **Test run**: Validate on 100 species
4. **Review**: You check output quality
5. **Full run**: Process all 9,154 species