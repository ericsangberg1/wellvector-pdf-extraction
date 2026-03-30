# Wellvector PDF Extraction

Extracts wellbore casing programme and formation test data from scanned PDFs sourced via the Norwegian Sodir (NPD) FactPages API.

Input: CSV of document metadata from NPD FactPages
Output: Structured CSV with casing strings, shoe depths, hole sizes, and LOT/FIT mud weights.

## Pipeline

```
CSV filter
  → async PDF download (cached)
  → Universal Document Scan (Haiku)
  → Collector (Haiku)
  → Synthesizer (Sonnet)
  → casing_data.csv + casing_conflicts.csv
```

**Universal Document Scan** — Each PDF is assessed for relevance before any data extraction runs. The scan identifies whether the document contains a casing programme or LOT/FIT tests, locates candidate data pages, and checks for a table of contents. Documents that are clearly irrelevant (core reports, petrophysics, DST-only, etc.) are skipped entirely. The scan result also provides page hints that restrict collection to only the pages likely to contain data.

**Collector** — Runs per candidate page using Haiku. Each page is rendered as an image and the model extracts tagged data fragments as JSON-L with source provenance: `{page_idx, source_doc, doc_type, priority, topic, confidence, content}`. Three confidence levels are assigned: `explicit` (tabular data), `schematic` (diagram or figure), `approximate` (estimated from context). Only confirmed LOT/FIT values are collected — approximate or inferred mud weights are excluded.

**Synthesizer** — One call per wellbore using Sonnet. Receives all fragments grouped by topic, resolves conflicts using confidence and source priority, and outputs final structured JSON with `rows` and `conflicts` arrays. Higher-confidence and higher-priority sources win conflicts. If total fragment volume exceeds the token limit, topics are chunked into separate calls.

Documents are processed in tier order (NPD Paper → WDSS → Licensee reports). Once an anchor document provides sufficient casing structure, lower-tier documents are only used to fill gaps.

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
| `casing_data.csv` | Synthesized rows with source provenance and conflict flags |
| `casing_conflicts.csv` | Per-field conflict log with both values, sources, severity, resolution |
| `fragments.json` | Raw collected fragments (used for re-synthesis without re-running collection) |
| `run_info.json` | Timestamp, config snapshot, script MD5 |

PDFs are downloaded once and cached under `pdfs/` — subsequent runs reuse cached files.
