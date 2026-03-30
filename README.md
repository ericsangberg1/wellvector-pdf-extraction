# Wellvector PDF Extraction

Extracts wellbore casing programme and formation test data from scanned PDFs.

Input: CSV of document metadata from NPD FactPages
Output: Structured CSV with casing strings, shoe depths, hole sizes, and LOT/FIT mud weights.

## Pipeline

```
CSV / URL input
  → async PDF download (cached)
  → Universal Document Scan (Haiku)        — relevance gate: skip irrelevant docs,
  |   + Screen (Haiku, pages 1–4)            identify candidate pages, check for ToC
  ↓ relevant docs only
  → Page audit (PyMuPDF)                   — digital pages → send as text (cheap)
  |                                           scanned pages → render as JPEG image
  ↓ per candidate page
  → Collector (Haiku)                      — extract fragments: casing rows, LOT/FIT values
  |                                           tagged with confidence: explicit / schematic / approximate
  ↓ all fragments per wellbore
  → Synthesizer (Sonnet)                   — emit raw observations from all fragments
  → Post-processing (Python)               — scaffold locking, merge by priority,
  |                                           resolve conflicts, match LOT to casing shoes
  ↓
  → casing_data.csv + casing_conflicts.csv
```

**Universal Document Scan** — Each PDF is assessed for relevance before any data extraction runs. The scan identifies whether the document contains a casing programme or LOT/FIT tests and locates candidate data pages. In parallel, a screen call checks the first 1–4 pages for a table of contents — if found, page references from the ToC are used to restrict collection instead of the scan's candidate pages. Documents that are clearly irrelevant (core reports, petrophysics, maps, etc.) are skipped entirely.

**Collector** — Runs per candidate page using Haiku. Each page is first checked for extractable text using PyMuPDF — if the page has sufficient clean text (≥200 characters), it is sent as text directly, skipping image rendering and reducing token cost. Scanned or image-heavy pages fall back to JPEG rendering. The model extracts tagged data fragments as JSON-L with source provenance: `{page_idx, source_doc, doc_type, priority, topic, confidence, content}`. Three confidence levels are assigned: `explicit` (tabular data), `schematic` (diagram or figure), `approximate` (estimated from context).

**Synthesizer** — One call per wellbore using Sonnet. Receives all fragments grouped by topic and emits raw observations — one per distinct data point.

**Post-processing** — Documents are processed in tier order (NPD Paper → WDSS → Licensee reports). The highest-scoring anchor documents define the casing scaffold — the fixed set of casing strings for the wellbore. Lower-tier documents can only fill missing fields on existing casings, never create new rows. LOT/FIT values are matched to casing shoes by depth. Conflicts between sources are resolved by `(confidence, priority, data format)` and logged to `casing_conflicts.csv`.

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

| Flag | Description |
|---|---|
| `--csv FILE` | CSV input file path |
| `--url PDF_URL` | Run on a single PDF URL (mutually exclusive with `--csv`) |
| `--wellbore NAME` | Filter to a single wellbore (e.g. `7/11-2`) |
| `--doc-name PATTERN` | Only process documents whose name contains this substring |
| `--resynth RUN` | Skip collection and re-synthesize from a previous run (e.g. `run56`) |

## Outputs

Each run writes to a new `Runs/runN/` directory:

| File | Contents |
|---|---|
| `casing_data.csv` | Clean 8-column output: wellbore, casing type, casing/hole diameters and depths, mud weight, formation test type |
| `casing_conflicts.csv` | Per-field conflict log — where sources disagreed, showing both values and how it was resolved |
| `fragments.json` | Raw collected fragments (used for re-synthesis without re-running collection) |
| `run_info.json` | Timestamp, config snapshot, script MD5 |

PDFs are downloaded once and cached under `pdfs/` — subsequent runs reuse cached files.
