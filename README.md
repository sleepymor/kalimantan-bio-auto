# Kalimantan Species Data Enrichment Pipeline

Enriches 9,154 Kalimantan vascular plant species with taxonomy, Indonesian descriptions, conservation status, CITES listings, P106 protection status, endemic/invasive/native classification, and WebP images (≤300KB).

## Features

- **Taxonomy**: Kingdom, Phylum, Class, Order, Family, Genus from GBIF
- **Names**: Scientific name (accepted), Indonesian common names from Wikidata
- **Descriptions**: Indonesian descriptions (Wikidata + machine translation fallback)
- **Conservation**: IUCN Red List category, criteria, assessment year
- **CITES**: Appendix listing (I, II, III) and effective date
- **P106**: Indonesian protected species status from PerMen LHK P.106/2018
- **Status**: Endemic / Invasive / Native classification from GRIIS + Wikidata
- **Images**: WebP format (≤300KB, max 1600px)

## Quick Start

### 1. Get API Tokens (Required for IUCN & CITES)

| API | URL | Cost |
|-----|-----|------|
| IUCN Red List | https://api.iucnredlist.org/ | Free (research) |
| Species+ / CITES | https://api.speciesplus.net/ | Free (research) |

### 2. Configure

```bash
cd /home/tiramissu/Projects/automations/kalimantan-bio-auto
cp config/.env.example config/.env
# Edit config/.env with your tokens:
# IUCN_REDLIST_API_TOKEN=your_token
# SPECIESPLUS_API_TOKEN=your_token
```

### 3. Install Dependencies

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 4. Run Test (100 species)

```bash
python -m src.main --mode test
```

Output: `data/output/test_run/enriched_test_100.csv`

### 5. Run Full (9,154 species)

```bash
python -m src.main --mode full
```

Output: `data/output/full_run/enriched_kalimantan_species.csv`

## Pipeline Architecture

```
Input CSV (9,154 species)
    │
    ▼
┌─────────────────────────────────────┐
│  Stage 1: Name Resolution (GBIF)    │
│  • Match scientific names           │
│  • Get accepted names & taxonomy    │
└─────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────┐
│  Stage 2: Parallel Enrichment       │
│  ├─ POWO: Descriptions, images      │
│  ├─ IUCN: Conservation status       │
│  ├─ CITES: Trade listings           │
│  ├─ Wikidata: Indonesian names      │
│  └─ GRIIS: Invasive status          │
└─────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────┐
│  Stage 3: Indonesian Localization   │
│  • Wikidata Indonesian labels       │
│  • Machine translation (Google)     │
└─────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────┐
│  Stage 4: Image Download + WebP     │
│  • Download → Convert → WebP ≤300KB │
└─────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────┐
│  Stage 5: P106 Lookup               │
│  • PDF extraction → Species lookup  │
└─────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────┐
│  Stage 6: Merge & Export            │
│  • Final CSV with all 28 columns    │
└─────────────────────────────────────┘
```

## Output CSV Schema (28 columns)

| Column | Description |
|--------|-------------|
| ID | Original row ID |
| Taxon Name | Original scientific name |
| Genus | Original genus |
| Family | Original family |
| Kingdom | Kingdom (Plantae) |
| Phylum | Phylum (e.g., Tracheophyta) |
| Class | Class (e.g., Magnoliopsida) |
| Order | Order (e.g., Malvales) |
| Family (confirmed) | GBIF-matched family |
| Genus (confirmed) | GBIF-matched genus |
| Scientific Name (accepted) | Accepted scientific name |
| Nama Umum (Indonesia) | Primary Indonesian common name |
| Nama Umum (Lainnya) | Other common names (semicolon-separated) |
| Tipe | endemic / invasif / asli |
| URL Gambar | Source image URL |
| File Gambar (WebP) | Local WebP file path |
| Deskripsi (Indonesia) | Indonesian description |
| Catatan (Indonesia) | Indonesian notes |
| Kategori IUCN | IUCN category (LC, VU, EN, CR, DD, NE) |
| Kriteria IUCN | IUCN criteria (e.g., B1ab(iii)) |
| Tahun Penilaian | Assessment year |
| Lampiran CITES | CITES Appendix (I, II, III, Tidak Tercantum) |
| Tanggal Pencantuman CITES | Listing effective date |
| Dilindungi P106 (Ya/Tidak) | Protected under P106 |
| Kategori P106 | Protection category |
| Translated (Ya/Tidak) | Machine translation used |
| Sumber Data Utama | Data source attribution |
| Terakhir Diperbarui | ISO timestamp |

## Caching & Resume

- SQLite cache: `data/cache/species_cache.db` (stores all API responses)
- Checkpoints: `data/output/{mode}/checkpoint_{mode}.json`
- Resume interrupted runs: `python -m src.main --mode full --resume`

## Data Sources & Attribution

| Source | Data | License |
|--------|------|---------|
| GBIF.org | Taxonomy, names, occurrences | CC0 |
| POWO (Kew) | Descriptions, distributions, images | CC BY 4.0 |
| IUCN Red List | Conservation assessments | CC BY-NC 4.0 |
| CITES / Species+ | Trade listings | CC BY 4.0 |
| Wikidata | Indonesian labels, descriptions | CC0 |
| GRIIS / ISSG | Invasive status Indonesia | CC BY 4.0 |
| PerMen LHK P.106/2018 | Indonesian protected species | Government |

## Known Limitations

| Feature | Status | Notes |
|---------|--------|-------|
| POWO API | ⚠️ 403 Forbidden | Use GBIF taxonomy instead |
| Wikidata SPARQL | ⚠️ Rate limited (1 req/min) | Added delay handling |
| IUCN API | 🔑 Requires token | Free for research |
| CITES API | 🔑 Requires token | Free for research |
| Indonesian descriptions | 🤖 Machine translated | Google Translate fallback |
| Images | ❌ POWO blocked | Need alternative source |

## Development

### Project Structure

```
kalimantan-bio-auto/
├── config/
│   ├── .env                    # API tokens (gitignored)
│   └── settings.yaml           # Runtime configuration
├── src/
│   ├── main.py                 # CLI entry point
│   ├── pipeline/
│   │   ├── orchestrator.py     # Main pipeline coordination
│   │   └── stages/             # Pipeline stages
│   ├── api/                    # API clients
│   ├── cache/                  # SQLite caching
│   ├── translation/            # Translation engine
│   ├── p106/                   # PDF extraction
│   ├── images/                 # WebP conversion
│   └── utils/                  # Logging, checkpoints, validators
├── data/
│   ├── input/                  # Input CSV
│   ├── output/                 # Test & full outputs
│   ├── cache/                  # SQLite cache
│   └── p106/                   # P106 PDF & extracted JSON
├── scripts/
│   ├── download_griis.py       # Download GRIIS data
│   └── download_p106.py        # Download P106 PDF
├── requirements.txt
└── README.md
```

### Helper Scripts

```bash
# Download GRIIS Indonesia invasive species data
python scripts/download_griis.py

# Download P106 PDF
python scripts/download_p106.py
```

## Performance

- **Test run (100 species)**: ~30 seconds
- **Full run (9,154 species)**: ~4-6 hours
- **Cache size**: ~500 MB
- **Images**: ~2.5-4 GB (9,154 × ~300KB WebP)

## License

This pipeline code is MIT licensed. Data sources have their own licenses (see Attribution table).