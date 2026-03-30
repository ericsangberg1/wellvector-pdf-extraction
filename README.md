# Wellvector PDF Extraction

Extracts wellbore casing programme and formation test data from scanned PDFs sourced via the Norwegian Sodir (NPD) FactPages API.

Input: CSV of document metadata from NPD FactPages
Output: Structured CSV with casing strings, shoe depths, hole sizes, and LOT/FIT mud weights.

## Pipeline

```
CSV filter
  → async PDF download (cached)
  → Universal Document Scan (Haiku) — assess document relevance and candidate pages
  → Collector (Haiku)               — per page: extract tagged fragments as JSON-L with source provenance
  → Synthesizer (Sonnet)            — per wellbore: resolve conflicts, output final structured JSON
  → casing_data.csv + casing_conflicts.csv
```

Documents are processed in tier order (NPD Paper → WDSS → Licensee reports). The Universal Document Scan gates whether collection runs at all — irrelevant documents (core reports, petrophysics, DST-only, etc.) are skipped without collecting fragments.

## Requirements

```bash
pip install anthropic pymupdf requests
```

Requires `ANTHROPIC_API_KEY` set in the environment.

## Usage

```bash
export ANTHROPIC_API_KEY=sk-...
python extract_casing_data.py --csv FILE [options]
```

### CLI flags

| Flag | Description |
|---|---|
| `--csv FILE` | CSV input file path |
| `--url PDF_URL` | Run on a single PDF URL, bypassing CSV entirely |
| `--wellbore NAME` | Filter to a single wellbore (e.g. `7/11-2`) |
| `--doc-name PATTERN` | Only process documents whose name contains this substring |
| `--resynth RUN` | Skip collection and re-synthesize from a previous run (e.g. `run56`) |

`--url` and `--csv` are mutually exclusive.

**Examples:**

```bash
# Run on all documents for a single wellbore
python extract_casing_data.py --csv wellbore_document_7_11.csv --wellbore 7/11-2

# Run on documents matching a name pattern
python extract_casing_data.py --csv wellbore_document_7_11.csv --doc-name COMPLETION

# Re-synthesize from a previous collection run
python extract_casing_data.py --resynth run56

# Run on a single PDF by URL
python extract_casing_data.py --url https://factpages.sodir.no/.../document.pdf
```

## Outputs

Each run writes to a new `Runs/runN/` directory:

| File | Contents |
|---|---|
| `casing_data.csv` | Synthesized rows with source provenance and conflict flags |
| `casing_conflicts.csv` | Per-field conflict log with both values, sources, severity, resolution |
| `fragments.json` | Raw collected fragments (used for re-synthesis without re-running collection) |
| `run_info.json` | Timestamp, config snapshot, script MD5 |

PDFs are downloaded once and cached under `pdfs/` — subsequent runs reuse cached files.

## Key config

| Variable | Purpose |
|---|---|
| `TARGET_DOC_TYPES` | Document types to process |
| `DOC_TYPE_TIERS` | Processing order (lower index = higher priority) |
| `ANCHOR_CASING_THRESHOLD` | Min casing mentions before skipping lower-tier docs |
| `MAX_CONCURRENT_PDFS` | PDF download concurrency |
| `MAX_CONCURRENT_CLAUDE` | Claude API call concurrency |
| `SYNTH_TOKEN_LIMIT` | Max tokens before chunking Synthesizer input by topic |
