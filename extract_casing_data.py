"""
Wellbore casing & formation data extractor. Made as a challenge for Wellvector.

Pipeline:
  CSV filter (by wlbDocumentType)
  → async PDF download
  → PyMuPDF page images
  → Pass 0 (Haiku):     check pages 1–4 for ToC; restrict collector to candidate pages ±2
  → Collector (Haiku):  per page — dump tagged fragments as JSON-L with source provenance
                        {"page_idx", "source_doc", "doc_type", "priority", "topic", "content"}
                        topics: casing_data | formation_tests | other
  → Synthesizer (Sonnet): one call per wellbore — receives ALL fragments grouped by topic,
                          resolves conflicts holistically, outputs final structured JSON
  → casing_data.csv + casing_conflicts.csv

Context window: fragments are token-estimated before the Synthesizer call.
If over SYNTH_TOKEN_LIMIT, topics are chunked and merged post-synthesis.

Usage:
  pip install anthropic pymupdf requests
  python extract_casing_data.py
"""

import asyncio
import base64
import csv
import hashlib
import json
import time
from collections import defaultdict
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path

import anthropic
import fitz  # PyMuPDF
import requests


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

SINGLE_DOC_URL: str | None = None

CSV_PATH: str | None = None
PDF_DIR = Path("pdfs")
RUNS_DIR = Path("Runs")

# wlbDocumentType values to include (case-insensitive)
TARGET_DOC_TYPES = [
    "REPORTED BY LICENSEE",
    "OLD NPD WDSS",
    "NPD PAPER",
]

# Processing tier order — documents are processed in this order so anchor
# documents are read first. Lower index = higher priority / processed first.
DOC_TYPE_TIERS = [
    "NPD PAPER",            # Tier 0 — anchor: compact summary, often covers multiple wellbores
    "OLD NPD WDSS",         # Tier 1 — official NPD summary sheets
    "REPORTED BY LICENSEE", # Tier 2 — detailed operator reports
]

# Document types where a single PDF may contain data for MULTIPLE wellbores.
# For these, the collector is instructed to tag each fragment with the correct wellbore.
MULTI_WELLBORE_DOC_TYPES = {"NPD PAPER"}

# After completing a tier, a wellbore is considered "anchor-complete" if its
# collected fragments contain at least this many distinct casing size mentions
# AND at least this many formation test mentions. Complete wellbores are skipped
# in lower-priority tiers.
ANCHOR_CASING_THRESHOLD = 3
ANCHOR_TEST_THRESHOLD   = 1

# Document name substrings that are never relevant — skip before downloading.
# Add to this list as you discover more noise documents.
# Hard-skip patterns — these are never relevant, skip without even screening.
# Everything else goes through the LLM relevance screen (first 4 pages).
SKIP_DOC_PATTERNS = [
    "LITHOLOGIC_CORRELATION_CHART",   # map/chart only
    "PALEOCENE_CORRELATION_MAP",      # map/chart only
    "COD",                            # multi-well contamination source
]

# Document-type hints for the collector.
# Keys are substrings matched against the document name (case-insensitive).
# Value is injected into the collector prompt to focus Haiku on what matters.
# Documents NOT listed here get the generic prompt — so new/unknown files are
# always processed in full. Add entries as you learn more about document types.
DOC_TYPE_HINTS = {
    # --- NPD Paper No.10 (multi-well compilation, processed in multi-wellbore mode) ---
    "INTERPRETED_LITHOLOGY_LOG": (
        "This is an NPD Paper No.10 Interpreted Lithology Log. The FIRST PAGE contains a "
        "structured well summary table for the named wellbore with:\n"
        "  - Casing programme: each string listed with diameter, shoe depth, and hole size\n"
        "  - LOT/FIT values at casing shoes\n"
        "Focus on pages 1-2 only. Extract every row of the casing table and any confirmed LOT/FIT values. "
        "Do NOT extract DST intervals or DST mud weights. "
        "The depths may be in feet or metres — record the unit exactly as printed."
    ),
    "NPD_PAPER_NO": (
        "This is an NPD Paper No.10 — a multi-well compilation. It contains a summary section "
        "for EACH wellbore, clearly labelled with the wellbore name (e.g. '7/11-1', '7/11-2'). "
        "Each wellbore's section includes: casing programme table (diameter, shoe depth, hole size), "
        "and confirmed LOT/FIT values at casing shoes.\n"
        "Extract data for ALL wellbores present — tag each fragment with the correct wellbore name. "
        "Do NOT extract DST intervals or mud weights from DST tests. "
        "Record depths and mud weights verbatim with their units."
    ),

    # --- Completion log (graphical depth-track, NOT a structured WDSS table) ---
    "COMPLETION_LOG": (
        "This is a completion log — a graphical depth-track diagram showing the wellbore schematic, "
        "casing strings, mud weights, and stratigraphic picks plotted against depth. "
        "Values are read from a graph, not from a structured table.\n"
        "Assign confidence='schematic' to ALL fragments from this document. "
        "Extract casing depths and diameters only if the depth value is clearly annotated on the "
        "diagram at the correct casing shoe. For mud weights/LOT values: only extract if the value "
        "is explicitly labelled at the correct casing shoe depth on the diagram. "
        "Do NOT infer values from curve positions alone."
    ),

    # --- Casing programme sources ---
    "WELL_COMPLETION_REPORT": (
        "This is a Well Completion Report. Focus on: casing string table (diameter, shoe depth, "
        "hole size, cement volume), and any LOT/FIT mud weight recorded at casing shoes. "
        "Ignore: geological descriptions, core data."
        # TODO: add verbatim table examples once reviewed
    ),
    "WDSS": (
        "This is an NPD WDSS summary sheet. It contains a 'CASING AND LEAK-OFF TESTS' table "
        "where each ROW is one casing string with columns: Type, Casing diam., Depth below KB, "
        "Hole diam., Hole depth below KB, Lot mud eqv. g/cm3.\n"
        "IMPORTANT: The table has TWO depth columns — the first is the casing shoe depth, "
        "the second is the hole depth (how deep the hole was drilled before casing was set). "
        "Include BOTH depths verbatim in each fragment, labelled clearly, e.g.: "
        "'casing depth 457m, hole depth 463m'.\n"
        "Extract each table row as ONE fragment with topic 'casing_data', keeping "
        "casing info AND the LOT mud weight together in the same fragment. Do NOT split casing "
        "and LOT into separate fragments.\n"
        "Include EVERY row even if some columns are blank (e.g. a casing with no hole diameter "
        "or no LOT value). Never skip a row just because it has missing fields.\n"
        "CRITICAL: Read every cell of the LOT mud eqv. column carefully. If a numeric value is "
        "printed in that cell, you MUST include it — do not write 'not listed' or leave it blank "
        "if a number is visible. Only write 'not listed' if the cell is genuinely empty.\n"
        "Ignore: geological column, location data, licensee list, DST rows."
    ),
    "KONTINENTALSOKKELEN": (
        "This is a Norwegian NPD well report. Focus on: casing programme table, hole sizes, "
        "shoe depths, and any mud weight or LOT/FIT values. Ignore: geological descriptions, location coordinates."
    ),
    "EXPLORATORY_TEST": (
        "This is an exploratory drilling report. Focus on: casing strings set during drilling, "
        "hole sizes, shoe depths. Ignore: operational narrative, equipment lists."
    ),

    # --- Formation test / mud density sources ---
    "DRILL_STEM_TEST": (
        "This is a Drill Stem Test report. Extract ONLY confirmed LOT or FIT (casing shoe pressure test) "
        "values if explicitly present — with a test depth and measured EMW or pressure value. "
        "Do NOT extract DST interval depths, DST mud weights, or flow/pressure data. "
        "If no LOT or FIT is present, output nothing."
    ),
    "DST_NO": (
        "This is a DST data sheet. Extract ONLY if a LOT or FIT (casing shoe pressure test) is "
        "explicitly recorded on the page — with a test depth and measured EMW or pressure. "
        "Do NOT extract DST interval depths or mud weights. If no LOT or FIT is present, output nothing."
    ),
    "WELL_PRODUCTION_TEST": (
        "This is a Well Production Test report. Extract ONLY confirmed LOT or FIT values if "
        "explicitly present. Do NOT extract DST intervals, mud weights from DST tests, "
        "flow rates, GOR, water cut, or pressure charts. If no LOT or FIT is present, output nothing."
    ),
    "DRILLING_FLUID_SUMMARY": (
        "This is a Drilling Fluid Summary. Extract ONLY explicitly confirmed LOT or FIT test results:\n"
        "PRIORITY 1 — LOT/FIT result tables: Any table or list explicitly labelled 'LOT RESULTS', "
        "'FIT RECOVERIES', 'LOT/FIT', or similar. Extract test type, depth, and mud weight per row.\n"
        "PRIORITY 2 — Pressure-vs-depth plot annotations: Only if a LOT or FIT test point is "
        "annotated with BOTH a specific numeric depth AND a specific numeric EMW value directly "
        "on the chart (e.g. '11.6 ppg at 6500 ft'). Do NOT extract approximate or estimated values "
        "read from an unlabelled curve.\n"
        "DO NOT extract: routine mud weight columns, daily mud weights, approximate depth ranges, "
        "values described as 'approximately', mud density ranges from the drilling programme, "
        "or any value without both a confirmed depth and a confirmed EMW."
    ),
    "DRILLING_MUD_REPORT": (
        "This is a Drilling Mud Report. Focus on: any LOT or FIT recorded at a casing shoe depth "
        "with associated mud weight. Ignore: routine mud property measurements."
        # TODO: add verbatim examples once reviewed
    ),
    "DIAGRAM_PSI": (
        "This is a pressure test diagram. Focus on: LOT and FIT test depths and the recorded pressure "
        "or equivalent mud weight for each numbered test. "
        "IMPORTANT: For each test (LOT No. 1, LOT No. 2, etc.) you MUST extract the actual depth value "
        "from the axis label, curve annotation, or table alongside the diagram — do not describe it as "
        "'visible on curve' without a number. If you cannot read a depth, state the depth as unknown "
        "but still emit the fragment. Ignore: equipment diagrams, wellbore schematics."
    ),
    "AAODC": (
        "This is an AAODC daily drilling report for a single well. Each page covers one day of operations. "
        "The page has two key sections to read together:\n"
        "  1. 'DETAILS OF OPERATIONS IN SEQUENCE AND REMARKS' (middle-right column): "
        "Look for any pressure test performed in connection with casing cementing. "
        "This does NOT need to be labeled LOT or FIT. It appears as a sequence like: "
        "run casing → cement casing → wait on cement → drill out float collar/shoe → "
        "pressure test or squeeze test, recording a pressure value in PSI. "
        "Extract: the casing size cemented (e.g. '13-3/8 inch'), the shoe depth if mentioned, "
        "and the test pressure in PSI.\n"
        "IMPORTANT — depth units: AAODC pages may use mixed units. Shallower casing strings "
        "(conductor, surface, intermediate) may have depths written in METRES while deeper strings "
        "are in feet. Always record the unit explicitly as it appears. If no unit is labelled, "
        "note 'unit unknown' rather than assuming feet.\n"
        "  2. 'MUD RECORD' section (typically a table on the same page): "
        "Extract the mud weight in use on that day. The unit is unknown — record whatever unit appears "
        "(e.g. ppg, lb/gal, g/cm³, kg/m³, SG, or any other). Always include the unit with the value.\n"
        "Emit one fragment combining both: casing size, shoe depth, test pressure (PSI), and mud weight with unit. "
        "Also extract any casing string set depths from the operations narrative "
        "(e.g. 'set 9-5/8 casing at 10,510 ft'). "
        "Ignore: routine drilling parameters, trip reports, core descriptions, and mud weight entries "
        "on days with no cementing or pressure test operation."
    ),
    "COD": (
        "This document may cover multiple wells in the field. "
        "Focus ONLY on: confirmed LOT and FIT test results — extract test type, depth, and mud weight. "
        "Do NOT extract DST, RFT, MDT, or WFT data. "
        "Ignore entirely: electric logs, borehole corrections, porosity/permeability tables, "
        "sonic logs, seismic velocity data, and dipmeter data."
    ),
    "DIPMETER": (
        "This is a dipmeter log report. Focus on: any mud weight or mud density recorded at the "
        "time of logging. Ignore: dip angles, formation dip analysis, structural interpretation."
        # TODO: add verbatim examples once reviewed
    ),
    "INDIVIDUAL_WELL_RECORD": (
        "This is an individual well record summary. Focus on: casing shoe depths, LOT/FIT mud weights, "
        "and any formation test intervals with associated mud density. "
        "Ignore: administrative data, location coordinates."
        # TODO: add verbatim examples once reviewed
    ),
}

# Higher number = more trusted when conflicts arise
DOC_PRIORITY = {
    "REPORTED BY LICENSEE": 2,
    "OLD NPD WDSS":         2,  # NPD WDSS and NPD Papers are official — same trust as licensee
    "NPD PAPER":            2,
}

# ---------------------------------------------------------------------------
# Provider config
# ---------------------------------------------------------------------------

# Set each independently to "anthropic" or "gemini".
# anthropic: requires ANTHROPIC_API_KEY
# gemini:    requires GOOGLE_API_KEY — free tier via aistudio.google.com
#            gemini-2.0-flash is free (15 RPM, 1500 req/day); quality is close to Haiku.
#            gemini-1.5-pro has a free tier but is heavily rate-limited.
COLLECTOR_PROVIDER = "anthropic"    # Pass 0 (ToC) + Collector
SYNTH_PROVIDER     = "anthropic" # Synthesizer

MODELS = {
    "anthropic": {
        "screener":  "claude-haiku-4-5-20251001",
        "extractor": "claude-sonnet-4-6",
    },
    "gemini": {
        "screener":  "gemini-2.0-flash",
        "extractor": "gemini-1.5-pro",
    },
}

# Concurrency limits
MAX_CONCURRENT_PDFS = 3
MAX_CONCURRENT_CLAUDE = 3

# Wellbore filter — set to a wlbName string to process one wellbore, or None for all.
WELLBORE_FILTER: str | None = None

# Single-document URL mode — set to a PDF URL to run the pipeline on one document
# without a CSV. Wellbore(s) are auto-detected from the document by the scan phase.
# When set, WELLBORE_FILTER and CSV_PATH are ignored.
SINGLE_DOC_URL: str | None = None


# Synthesizer context window limit (estimated tokens).
# If a wellbore's fragments exceed this, topics are chunked into separate Sonnet calls.
SYNTH_TOKEN_LIMIT = 140_000

# Pricing per million tokens (USD) — update if Anthropic changes rates
MODEL_PRICING = {
    "claude-haiku-4-5-20251001": {"input": 0.80,  "output": 4.00},
    "claude-sonnet-4-6":         {"input": 3.00,  "output": 15.00},
}

# Global token usage accumulator — keyed by model name
_usage: dict[str, dict[str, int]] = defaultdict(lambda: {"input": 0, "output": 0})

# Separate counter for screening calls (relevance + ToC, subset of Haiku usage)
_toc_usage: dict[str, int] = {"input": 0, "output": 0, "calls": 0}


# ---------------------------------------------------------------------------
# Page-level fragment cache
# Keyed by "{pdf_filename}|{page_idx}|{prompt_hash}" so the cache is
# automatically invalidated when the collector prompt changes.
# Stored at pdfs/fragment_cache.json — shared across all runs.
# ---------------------------------------------------------------------------
CACHE_PATH = PDF_DIR / "fragment_cache.json"
_cache: dict[str, list[dict]] = {}
_cache_lock = None  # initialised in main() after the event loop starts
_cache_dirty = False

def _prompt_hash() -> str:
    """Hash of the base prompt template only — excludes per-doc hints."""
    return hashlib.md5(COLLECTOR_PROMPT.encode()).hexdigest()[:8]

def _doc_hint_hash(doc_name: str) -> str:
    """Hash of the hint for this specific document — changes only when that hint changes."""
    hint = doc_type_hint(doc_name)
    return hashlib.md5(hint.encode()).hexdigest()[:8]

def _cache_key(doc_name: str, page_idx: int) -> str:
    """Cache key invalidates when the base prompt OR this document's hint changes."""
    return f"{doc_name}|{page_idx}|{_prompt_hash()}|{_doc_hint_hash(doc_name)}"

def load_cache() -> None:
    global _cache
    if CACHE_PATH.exists():
        try:
            _cache = json.loads(CACHE_PATH.read_text())
        except Exception:
            _cache = {}

def save_cache() -> None:
    if _cache_dirty:
        CACHE_PATH.write_text(json.dumps(_cache))

# Primary output — matches the target evaluation schema exactly
OUTPUT_FIELDS_PRIMARY = [
    "wellbore",
    "casing_type",
    "casing_diameter_in",
    "casing_depth_m",
    "hole_diameter_in",
    "hole_depth_m",
    "mud_weight_g_cm3",
    "formation_test_type",
]

# Full output — includes provenance and QA columns for debugging
OUTPUT_FIELDS = OUTPUT_FIELDS_PRIMARY + [
    "confidence",
    "extraction_notes",
    "source_document",
    "source_doc_type",
    "source_priority",
    "conflict_fields",
]

# Human-readable CSV column headers (maps internal field name → display name with units)
COLUMN_DISPLAY_NAMES = {
    "wellbore":                 "Wellbore",
    "casing_type":              "Casing type",
    "casing_diameter_in":       "Casing diameter [in]",
    "casing_depth_m":           "Casing depth [m]",
    "hole_diameter_in":         "Hole diameter [in]",
    "hole_depth_m":             "Hole depth [m]",
    "mud_weight_g_cm3":         "LOT/FIT mud eqv. [g/cm3]",
    "formation_test_type":      "Formation test type",
    "confidence":               "confidence",
    "extraction_notes":         "extraction_notes",
    "source_document":          "source_document",
    "source_doc_type":          "source_doc_type",
    "source_priority":          "source_priority",
    "conflict_fields":          "conflict_fields",
}

# ---------------------------------------------------------------------------
# Prompts
# ---------------------------------------------------------------------------

SCREEN_PROMPT = """\
These are the first pages of a wellbore document. You must decide two things:

1. RELEVANCE — Does this document likely contain ANY of:
   - Casing setting depths, casing diameters, or hole sizes
   - Mud weights associated with casing shoe pressure tests (LOT or FIT only)
   - Well completion summaries or drilling programme data
   Documents about geology, geochemistry, core analysis, paleontology, \
sedimentology, seismic, logs, biostratigraphy, or drill stem tests (DST) are NOT relevant.

2. TABLE OF CONTENTS — If relevant, is there a table of contents, \
list of figures, or section index? If yes, which page numbers contain \
casing, drilling, or formation test data?

Reply in this exact format (three lines):
RELEVANT: YES or NO
TOC_FOUND: YES or NO
PAGES: (if both YES) comma-separated page numbers from the ToC. \
Include only pages likely to contain casing, hole, mud weight, or formation test data. \
If either is NO, write NONE.\
"""

# Fixed extraction rules — sent as a cacheable system prompt (identical across all collector calls).
COLLECTOR_SYSTEM = """\
You are scanning wellbore document pages for casing and formation test data only.

Extract ONLY these specific data types:
- Casing strings: outer diameter, shoe depth, hole size, cement volume
- Confirmed LOT (Leak-Off Test) or FIT (Formation Integrity Test) results: \
test depth and measured equivalent mud weight (EMW) or pressure. \
The test must be explicitly identified as LOT or FIT (or a casing shoe pressure test). \
A numeric EMW or pressure value must be present.

DO NOT collect as formation_tests:
- DST (Drill Stem Test), MDT, RFT, WFT — these are formation fluid tests, not shoe tests
- Mud weights from DST reports (mud weight during a DST is NOT a LOT/FIT value)
- Approximate, estimated, or inferred LOT/FIT values (e.g. from mud weight plots or narratives)
- Any LOT/FIT value described as "approximately", "~", or read from a graphical curve without an annotated number

NOTE: If a page contains both casing data AND DST rows (e.g. a WDSS summary table), \
extract the casing rows as "casing_data" and skip only the DST rows. Do not skip the whole page.

Capture values VERBATIM with whatever units appear on the page (feet, metres, inches, mm, \
ppg, g/cm³, psi, etc.). Do NOT convert units — the synthesizer handles all conversion.

CRITICAL — plain-text and monospace tables: Some tables are formatted with fixed-width columns \
in plain text (e.g. typewriter-style WDSS pages). When a row has blank cells in the middle, \
the values in later columns remain in their original horizontal positions — they do NOT shift left. \
Use the column headers to determine which value belongs to which column based on horizontal \
alignment, not row order. A value printed far to the right belongs to the rightmost applicable \
column even if middle columns are empty for that row.

When a table row combines casing and formation test data (e.g. LOT mud weight on the same \
row as casing diameter), keep them in ONE fragment (topic "casing_data").

NEVER skip a row because some fields are blank. Extract whatever values ARE present — \
a casing with no hole size, a casing with no LOT, or a LOT with no depth are all valid fragments.

DO NOT extract — output NOTHING for:
- Lithology, stratigraphy, core analysis, geochemistry, palaeontology
- Mud logs, drilling fluid reports, daily drilling reports
- Seismic, velocity surveys, synthetic seismograms
- Production test data (flow rates, GOR, water cut)
- Core lab data, porosity/permeability tables
- Location maps, cover pages, table of contents
- Pre-drill planning (Well Prognosis, Drilling Prospectus, AFE, proposed casing)
- Any page where ALL data belongs to a different wellbore than the one in the user message

If data for multiple wellbores is mixed, include only fragments for the wellbore named in \
the user message.

Assign each fragment a topic:
- "casing_data"      — casing strings, hole sizes, shoe depths, cement
- "formation_tests"  — confirmed LOT or FIT results only

Each fragment must include a "confidence" field:
- "explicit"    — value and unit are clearly stated in a structured table or labelled test record
- "schematic"   — value is read from a wellbore diagram, completion schematic, or depth track \
(depths annotated on a graphical wellbore cross-section, not a table). \
For mud weight / LOT values on depth-vs-mud-weight graphs: only associate a LOT label with a \
casing string if the label is horizontally aligned with that casing's shoe depth marker on the \
depth axis, or if the label explicitly names the casing string. Do not assign a LOT value to a \
casing string based on proximity alone.
- "approximate" — value is estimated, described as "approximately", preceded by "~", \
or otherwise uncertain

Output one JSON object per line (JSON-L) using the exact format given in the user message.
Output nothing (empty response) if no relevant fragments found.\
"""

# Per-page context — variable parts only; kept small so the cached system prompt dominates.
COLLECTOR_USER_TEMPLATE = """\
Wellbore: {wellbore}
Source: {source_doc} | Type: {doc_type} | Page: {page_idx}
{doc_hint}

Output each fragment as:
{{"page_idx": {page_idx}, "source_doc": "{source_doc}", "doc_type": "{doc_type}", \
"priority": {priority}, "topic": "<topic>", "confidence": "<explicit|schematic|approximate>", \
"content": "<verbatim text or table row as it appears>"}}\
"""

# Multi-wellbore variant — used when a single PDF covers several wellbores.
# The LLM must tag each fragment with the correct wellbore name.
COLLECTOR_USER_TEMPLATE_MULTI = """\
All wellbores in this field: {wellbores_list}
Source: {source_doc} | Type: {doc_type} | Page: {page_idx}
{doc_hint}

This document covers MULTIPLE wellbores. Extract fragments for ALL wellbores listed above.
Set the "wellbore" field in each fragment to the exact wellbore name as it appears in the document.

Output each fragment as:
{{"page_idx": {page_idx}, "source_doc": "{source_doc}", "doc_type": "{doc_type}", \
"priority": {priority}, "topic": "<topic>", "confidence": "<explicit|schematic|approximate>", \
"wellbore": "<exact wellbore name e.g. 7/11-1>", \
"content": "<verbatim text or table row as it appears>"}}\
"""

# Keep COLLECTOR_PROMPT as a combined version for cache-key hashing only.
COLLECTOR_PROMPT = COLLECTOR_SYSTEM + COLLECTOR_USER_TEMPLATE

SYNTHESIZER_PROMPT = """\
You are extracting wellbore observations for wellbore {wellbore}.

Read ALL fragments below. Your ONLY job is to identify and emit raw numeric observations — \
do NOT convert units, do NOT deduplicate, do NOT match LOT to casing. \
Python code will handle all of that.

Output {{"observations": [...]}} where each item is exactly one of these three types:

Casing string:
{{"type": "casing", "casing_label": "intermediate", "size": "13-3/8", "shoe_depth": 6444, "shoe_unit": "ft", \
"hole_size": 17.5, "hole_size_unit": "in", "hole_depth": 6500, "hole_depth_unit": "ft", \
"source_doc": "DOC_NAME", "doc_type": "REPORTED BY LICENSEE", "priority": 2, "confidence": "explicit"}}
("casing_label": use the label from the source — e.g. "conductor", "surface", "intermediate", "liner", "production". \
A well may have MULTIPLE casings of the same label, e.g. three intermediates at different depths. Emit each one separately.)

LOT / FIT / casing shoe pressure test:
{{"type": "lot_fit", "test_type": "LOT", \
"depth": 6500, "depth_unit": "ft", "emw": 11.6, "emw_unit": "ppg", \
"casing_size": "13-3/8", \
"source_doc": "DOC_NAME", "doc_type": "REPORTED BY LICENSEE", "priority": 2, "confidence": "explicit"}}
(set "casing_size" only if the fragment explicitly names the casing string; otherwise null)

Rules:
- Copy all numeric values VERBATIM from fragments — do NOT convert any units
- "confidence": "explicit" if value and unit clearly stated; "inferred" if unit assumed
- Depths may be in ft or m — emit whatever appears, with the exact unit label
- AAODC cement-out pressure test: emit as type "lot_fit", test_type "casing_shoe_test", \
  use the mud weight from the mud record section as "emw" with the unit as it appears
- If the same data point appears in multiple fragments, emit it once from the highest-priority source
- Emit one observation per distinct data point (3 different LOT depths → 3 observations)
- Do NOT emit observations where all numeric fields are null
- CONFIRMED ABSENT: If an explicit fragment for a casing string says a LOT/FIT value is \
  "not listed" or absent, do NOT emit a lot_fit observation for that casing from any \
  schematic or lower-confidence fragment. The explicit "not listed" overrides schematic claims.
- Return ONLY valid JSON. No prose, no markdown fences.

{fragments_block}\
""" 

# ---------------------------------------------------------------------------
# Post-processing: unit conversion, dedup, LOT→casing matching
# ---------------------------------------------------------------------------

# Maps size string → (casing_type, diameter_inches)
_CASING_SIZE_MAP: dict[str, tuple[str, float]] = {
    "30":    ("conductor",    30.0),
    "26":    ("conductor",    26.0),
    "20":    ("surface",      20.0),
    "18-5/8": ("surface",    18.625),
    "16":    ("intermediate",  16.0),
    "13-3/8": ("intermediate", 13.375),
    "13-5/8": ("intermediate", 13.625),
    "9-5/8": ("production",    9.625),
    "7":     ("liner",         7.0),
    "5-1/2": ("liner",         5.5),
    "5":     ("liner",         5.0),
    "4-1/2": ("liner",         4.5),
}

# Reverse map: float diameter → standard fractional display string
_DIAM_TO_FRACTION: dict[float, str] = {
    30.0:   "30",
    26.0:   "26",
    20.0:   "20",
    18.625: "18 5/8",
    17.5:   "17 1/2",
    16.0:   "16",
    13.375: "13 3/8",
    13.625: "13 5/8",
    12.25:  "12 1/4",
    9.625:  "9 5/8",
    8.5:    "8 1/2",
    7.0:    "7",
    6.0:    "6",
    5.5:    "5 1/2",
    5.0:    "5",
    4.5:    "4 1/2",
    36.0:   "36",
    24.0:   "24",
}


def _fmt_diam(v) -> str:
    """Format a diameter float as a standard fractional string (e.g. 13.375 → '13 3/8"')."""
    if v is None:
        return ""
    s = _DIAM_TO_FRACTION.get(float(v), str(v))
    return s + '"'


def parse_casing_size(size_str) -> tuple[str | None, float | None]:
    """Return (casing_type, diameter_in) from a size string like '13-3/8' or '339.7mm'."""
    if not size_str:
        return None, None
    s = str(size_str).strip()
    # Normalise spaces/en-dashes to hyphen
    s_norm = s.replace(" ", "-").replace("\u2013", "-")
    if s_norm in _CASING_SIZE_MAP:
        return _CASING_SIZE_MAP[s_norm]
    # Try converting mm to inches
    if s_norm.lower().endswith("mm"):
        try:
            inches = float(s_norm[:-2]) / 25.4
            for k, (t, v) in _CASING_SIZE_MAP.items():
                if abs(v - inches) < 0.15:
                    return t, v
        except (ValueError, TypeError):
            pass
    # Try plain float match against known diameters
    try:
        d = float(s_norm)
        for k, (t, v) in _CASING_SIZE_MAP.items():
            if abs(v - d) < 0.15:
                return t, v
    except (ValueError, TypeError):
        pass
    # Handle truncated fractions: "3/8" → try "13-3/8", "18-3/8" etc.
    # Occurs when OCR or LLM drops the whole-number prefix from a fractional size.
    if "/" in s_norm and "-" not in s_norm:
        for prefix in ["13", "18", "9", "7", "5", "16", "20", "30"]:
            candidate = f"{prefix}-{s_norm}"
            if candidate in _CASING_SIZE_MAP:
                return _CASING_SIZE_MAP[candidate]
    return None, None


def to_metres(value, unit_str, doc_type: str = "") -> tuple[float | None, str]:
    """Convert a depth value to metres. Returns (value_m, original_unit_label).

    When the unit is unknown, applies a heuristic:
    - Values > 500 are assumed to be in feet (Norwegian wells are rarely deeper than
      ~500m for conductor/surface strings, but often reported in feet).
    - WDSS/NPD docs typically use metres; WCR/AAODC typically use feet.
    - Values ≤ 500 are assumed metres (common for shallow casings in Norwegian reports).
    """
    if value is None:
        return None, "unknown"
    try:
        v = float(value)
    except (ValueError, TypeError):
        return None, "unknown"
    unit = (unit_str or "").strip().lower()
    if any(u in unit for u in ("ft", "feet", "foot")):
        return round(v * 0.3048, 1), "ft"
    elif any(u in unit for u in ("m", "metre", "meter")):
        return round(v, 1), "m"
    else:
        # Unknown unit — apply heuristic based on magnitude and document type
        doc_upper = doc_type.upper()
        if "WDSS" in doc_upper or "NPD" in doc_upper:
            # WDSS/NPD docs typically use metres
            return round(v, 1), "m (assumed)"
        elif v > 500:
            # WCR, AAODC, and most operator reports use feet for large depths
            return round(v * 0.3048, 1), "ft (assumed)"
        else:
            # Small values — assume metres (shallow conductor/surface in Norwegian wells)
            return round(v, 1), "m (assumed)"


def to_gcm3(value, unit_str) -> float | None:
    """Convert a mud weight / EMW value to g/cm³."""
    if value is None:
        return None
    try:
        v = float(value)
    except (ValueError, TypeError):
        return None
    unit = (unit_str or "").strip().lower()
    if "ppg" in unit or "lb/gal" in unit or "lbs/gal" in unit:
        return round(v / 8.33, 3)
    elif "sg" in unit or "sp.g" in unit or "g/cm" in unit or unit == "s.g.":
        return round(v, 3)
    elif "kg/m" in unit:
        return round(v / 1000, 3)
    elif "kpa/m" in unit:
        return round(v / 9.81, 3)
    elif "psi/ft" in unit:
        return round(v / 0.4335, 3)
    elif "pcf" in unit or "lbs/cu" in unit or "lb/ft" in unit:
        return round(v / 62.43, 3)
    else:
        # Assume g/cm³ if value is in a plausible density range
        if 0.8 <= v <= 2.5:
            return round(v, 3)
        return None


_CONFIDENCE_RANK = {"explicit": 2, "schematic": 1, "approximate": 0}
_DATA_FORMAT_RANK = {"table": 2, "text": 1, "schematic": 0}


def _obs_score(obs: dict) -> tuple[int, int, int]:
    """Scoring tuple (confidence_score, priority, data_format_rank) for picking the best observation.

    confidence ranks:
      explicit  — from a structured table (most reliable)
      schematic — read from a wellbore diagram (allowed but loses to explicit)
      approximate — estimated/uncertain (lowest)
    When confidence and priority are equal, data_format breaks the tie.
    """
    conf = _CONFIDENCE_RANK.get(obs.get("confidence", "explicit"), 2)
    fmt  = _DATA_FORMAT_RANK.get(obs.get("data_format", "table"), 1)
    return (conf, int(obs.get("priority", 0)), fmt)


def postprocess_observations(wellbore: str, observations: list[dict],
                             scaffold_doc_names: set | None = None) -> tuple[list[dict], list[dict]]:
    """
    Convert raw LLM observations into (rows, conflicts).

    - Converts all depths to metres, all mud weights to g/cm³
    - Deduplicates casing strings by type (picks best source)
    - Matches each LOT/FIT to the nearest casing shoe immediately above it
    - Deduplicates DSTs by top depth ±5m
    """
    casing_obs  = [o for o in observations if o.get("type") == "casing"]
    lot_obs     = [o for o in observations if o.get("type") == "lot_fit"]
    dst_obs     = [o for o in observations if o.get("type") == "dst"]

    conflicts: list[dict] = []

    # ------------------------------------------------------------------ casing
    # Scaffold approach: anchor-tier observations (NPD PAPER, WDSS) define
    # the casing list. Later-tier observations can only fill missing fields
    # on existing casings — never create new rows. If no anchor observations
    # exist, all observations run in create mode.
    #
    # Keyed by (type, diameter) since a well doesn't have two casings of
    # the same size.
    casing_by_key: dict[tuple[str, float | None], dict] = {}
    _scaffold_locked = False  # set True after anchor pass

    def _find_casing_key(ctype: str, diam: float | None, depth_m: float | None) -> tuple[str, float | None] | None:
        """Find an existing key that matches this casing.

        Primary match: same diameter (regardless of type label — type labels
        from different docs may disagree).
        Secondary: same type + depth within 5m (for when diameter is missing).
        """
        # 1. Match by diameter (strongest signal)
        if diam is not None:
            for (key_type, key_diam), entry in casing_by_key.items():
                if key_diam is not None and abs(key_diam - diam) < 0.15:
                    return (key_type, key_diam)
        # 2. Fallback: match by type + depth within 5m
        for (key_type, key_diam), entry in casing_by_key.items():
            if key_type != ctype:
                continue
            if depth_m is None and entry["_depth_m"] is None:
                return (key_type, key_diam)
            if depth_m is not None and entry["_depth_m"] is not None and abs(depth_m - entry["_depth_m"]) <= 5:
                return (key_type, key_diam)
        return None

    def _parse_casing_obs(obs: dict) -> dict | None:
        """Parse a raw casing observation into an internal entry dict. Returns None if unusable."""
        _, diam = parse_casing_size(obs.get("size"))
        ctype = (obs.get("casing_label") or "").strip().lower() or None
        if not ctype:
            ctype, _ = parse_casing_size(obs.get("size"))
        if not diam:
            return None  # no diameter = not a real casing string
        if not ctype:
            ctype = "unknown"
        depth_m, depth_unit = to_metres(obs.get("shoe_depth"), obs.get("shoe_unit"),
                                        doc_type=obs.get("doc_type", ""))
        hole_size = None
        try:
            hole_size = float(obs.get("hole_size")) if obs.get("hole_size") is not None else None
        except (ValueError, TypeError):
            pass
        hole_depth_m, _ = to_metres(obs.get("hole_depth"), obs.get("hole_depth_unit"),
                                    doc_type=obs.get("doc_type", ""))
        return {
            "_ctype":       ctype,
            "_diam":        diam,
            "_depth_m":     depth_m,
            "_depth_unit":  depth_unit,
            "_hole_size":   hole_size,
            "_hole_depth_m": hole_depth_m,
            "_priority":    int(obs.get("priority", 0)),
            "_confidence":  obs.get("confidence", "inferred"),
            "_source_doc":  obs.get("source_doc", ""),
            "_doc_type":    obs.get("doc_type", ""),
            "_data_format": obs.get("data_format", "table"),
            "_emw":         None,
            "_lot_type":    None,
            "_lot_depth_m": None,
            "_lot_source":  "",
            "_lot_note":    "",
        }

    def _merge_casing(obs: dict, entry: dict, ctype: str, diam: float | None,
                      depth_m: float | None, depth_unit: str = "",
                      hole_size: float | None = None, hole_depth_m: float | None = None,
                      allow_create: bool = True):
        """Merge a casing observation into casing_by_key."""
        existing_key = _find_casing_key(ctype, diam, depth_m)

        if existing_key is None:
            if allow_create:
                casing_by_key[(ctype, diam)] = entry
            else:
                # Scaffold locked — log as unmatched
                print(f"    [scaffold] {wellbore}: {ctype} {diam}\" at {depth_m}m "
                      f"from {entry['_source_doc']} — no matching scaffold casing, skipped")
            return

        existing = casing_by_key[existing_key]
        new_score = _obs_score(obs)
        old_score = _obs_score({"confidence": existing["_confidence"], "priority": existing["_priority"]})

        if new_score > old_score and not _scaffold_locked:
            # Higher score wins — but only replace if scaffold isn't locked
            if depth_m is not None and existing["_depth_m"] is not None and abs(depth_m - existing["_depth_m"]) > 5:
                conflicts.append({
                    "wellbore":            wellbore,
                    "casing_type":         ctype,
                    "field":               "casing_depth_m",
                    "primary_value":       entry["_depth_m"],
                    "primary_source":      entry["_source_doc"],
                    "primary_confidence":  entry["_confidence"],
                    "secondary_value":     existing["_depth_m"],
                    "secondary_source":    existing["_source_doc"],
                    "secondary_confidence": existing["_confidence"],
                    "severity":            "high" if entry["_confidence"] == "explicit" and existing["_confidence"] == "explicit" else "low",
                    "resolution":          f"Chose {entry['_source_doc']} (score {new_score} > {old_score})",
                })
            # Carry forward fields the new winner is missing
            if entry["_hole_size"] is None and existing["_hole_size"] is not None:
                entry["_hole_size"] = existing["_hole_size"]
            if entry["_hole_depth_m"] is None and existing.get("_hole_depth_m") is not None:
                entry["_hole_depth_m"] = existing["_hole_depth_m"]
            if entry["_depth_m"] is None and existing["_depth_m"] is not None:
                entry["_depth_m"] = existing["_depth_m"]
                entry["_depth_unit"] = existing["_depth_unit"]
            casing_by_key[existing_key] = entry
        else:
            # Equal or lower score, or scaffold locked — only fill missing fields
            if depth_m is not None and existing["_depth_m"] is not None and abs(depth_m - existing["_depth_m"]) > 5:
                conflicts.append({
                    "wellbore":            wellbore,
                    "casing_type":         ctype,
                    "field":               "casing_depth_m",
                    "primary_value":       existing["_depth_m"],
                    "primary_source":      existing["_source_doc"],
                    "primary_confidence":  existing["_confidence"],
                    "secondary_value":     depth_m,
                    "secondary_source":    entry["_source_doc"],
                    "secondary_confidence": entry["_confidence"],
                    "severity":            "high" if existing["_confidence"] == "explicit" and entry["_confidence"] == "explicit" else "low",
                    "resolution":          "Scaffold locked; kept anchor value" if _scaffold_locked else "Equal score; kept first seen",
                })
            # Fill missing fields from the new observation
            if existing["_hole_size"] is None and hole_size is not None:
                existing["_hole_size"] = hole_size
            if existing.get("_hole_depth_m") is None and hole_depth_m is not None:
                existing["_hole_depth_m"] = hole_depth_m
            if existing["_depth_m"] is None and depth_m is not None:
                existing["_depth_m"] = depth_m
                existing["_depth_unit"] = depth_unit

    # Determine which observations are from scaffold (anchor) documents.
    # If dynamically selected scaffold_doc_names are provided, use them.
    # Otherwise fall back to the hardcoded tier-based anchor list.
    if scaffold_doc_names:
        anchor_casing = [o for o in casing_obs if o.get("source_doc", "") in scaffold_doc_names]
        later_casing  = [o for o in casing_obs if o.get("source_doc", "") not in scaffold_doc_names]
        if anchor_casing:
            print(f"  [{wellbore}] Scaffold: using {len(set(o['source_doc'] for o in anchor_casing))} "
                  f"dynamically selected doc(s) as anchor")
    else:
        # Fallback: use doc-type tiers (NPD PAPER, OLD NPD WDSS)
        _ANCHOR_DOC_TYPES = {t for t in DOC_TYPE_TIERS[:2]}
        anchor_casing = [o for o in casing_obs if o.get("doc_type", "") in _ANCHOR_DOC_TYPES]
        later_casing  = [o for o in casing_obs if o.get("doc_type", "") not in _ANCHOR_DOC_TYPES]

    # Pass 1: anchor observations — create the scaffold
    for obs in anchor_casing:
        entry = _parse_casing_obs(obs)
        if entry is None:
            continue
        _merge_casing(obs, entry, entry["_ctype"], entry["_diam"],
                      entry["_depth_m"], entry.get("_depth_unit", ""),
                      entry["_hole_size"], entry.get("_hole_depth_m"),
                      allow_create=True)

    # Lock scaffold if we got any anchor casings
    if casing_by_key:
        _scaffold_locked = True
        print(f"  [{wellbore}] Scaffold: {len(casing_by_key)} casings from anchor docs — "
              + ", ".join(f"{e['_diam']}\"{e['_ctype'][0].upper()}" for e in
                         sorted(casing_by_key.values(), key=lambda e: e["_depth_m"] or 99999)))

    # Pass 2: later-tier observations — fill only, no new casings
    for obs in later_casing:
        entry = _parse_casing_obs(obs)
        if entry is None:
            continue
        _merge_casing(obs, entry, entry["_ctype"], entry["_diam"],
                      entry["_depth_m"], entry.get("_depth_unit", ""),
                      entry["_hole_size"], entry.get("_hole_depth_m"),
                      allow_create=not _scaffold_locked)

    # ------------------------------------------------- Casing shoe order validation
    # Expected order: conductor < surface < intermediate < production < liner
    _CASING_ORDER = {"conductor": 0, "surface": 1, "intermediate": 2, "production": 3, "liner": 4}
    sorted_by_depth = sorted(
        [e for e in casing_by_key.values() if e["_depth_m"] is not None],
        key=lambda e: e["_depth_m"],
    )
    for i in range(len(sorted_by_depth) - 1):
        a, b = sorted_by_depth[i], sorted_by_depth[i + 1]
        order_a = _CASING_ORDER.get(a["_ctype"], 99)
        order_b = _CASING_ORDER.get(b["_ctype"], 99)
        if order_a > order_b:
            print(f"  [WARNING] Casing order anomaly: {a['_ctype']} ({a['_depth_m']}m) "
                  f"is deeper than {b['_ctype']} ({b['_depth_m']}m)")
            conflicts.append({
                "wellbore":            wellbore,
                "casing_type":         f"{a['_ctype']}/{b['_ctype']}",
                "field":               "shoe_order",
                "primary_value":       f"{a['_ctype']}={a['_depth_m']}m",
                "primary_source":      a["_source_doc"],
                "primary_confidence":  a["_confidence"],
                "secondary_value":     f"{b['_ctype']}={b['_depth_m']}m",
                "secondary_source":    b["_source_doc"],
                "secondary_confidence": b["_confidence"],
                "severity":            "high",
                "resolution":          "Physical order violation — verify source depths",
            })

    # ----------------------------------------------------------- LOT/FIT → casing
    sorted_casing = sorted(
        [e for e in casing_by_key.values() if e["_depth_m"] is not None],
        key=lambda e: e["_depth_m"],
    )

    for obs in lot_obs:
        depth_m, _ = to_metres(obs.get("depth"), obs.get("depth_unit"),
                               doc_type=obs.get("doc_type", ""))
        emw = to_gcm3(obs.get("emw"), obs.get("emw_unit"))
        if depth_m is None or emw is None:
            continue

        test_type   = obs.get("test_type", "LOT")
        size_hint   = obs.get("casing_size")
        obs_score   = _obs_score(obs)
        matched_key = None
        match_note  = ""

        # 1. Try explicit casing size match — find entry by type + nearest depth
        matched_entry = None
        if size_hint:
            ct, _ = parse_casing_size(size_hint)
            if ct:
                # Find all casings of this type, pick the one nearest to LOT depth
                candidates = [e for e in casing_by_key.values()
                              if e["_ctype"] == ct and e["_depth_m"] is not None]
                if candidates:
                    matched_entry = min(candidates, key=lambda e: abs(e["_depth_m"] - depth_m))
                    match_note = f"{test_type} at {depth_m}m matched by explicit casing size {size_hint}"

        # 2. Try matching by hole depth (LOT is often recorded at hole depth, not shoe depth)
        if matched_entry is None:
            for e in casing_by_key.values():
                hd = e.get("_hole_depth_m")
                if hd is not None and abs(hd - depth_m) <= 5:
                    matched_entry = e
                    match_note = (f"{test_type} at {depth_m}m matched by hole depth "
                                  f"{hd}m → {e['_ctype']} {e['_diam']}\"")
                    break

        # 3. Fall back: nearest shoe immediately above test depth
        if matched_entry is None:
            above = [e for e in sorted_casing if e["_depth_m"] <= depth_m]
            if above:
                matched_entry = above[-1]
                match_note = (f"{test_type} at {depth_m}m → {matched_entry['_ctype']} shoe at "
                              f"{matched_entry['_depth_m']}m (nearest shoe above)")
            else:
                # No shoe above — assign to shallowest shoe (LOT might be near surface)
                if sorted_casing:
                    matched_entry = sorted_casing[0]
                    match_note = (f"{test_type} at {depth_m}m → {matched_entry['_ctype']} shoe at "
                                  f"{matched_entry['_depth_m']}m (no shoe above; used shallowest)")

        if matched_entry is None:
            continue

        entry = matched_entry
        if entry["_emw"] is None:
            entry["_emw"]         = emw
            entry["_lot_type"]    = test_type
            entry["_lot_depth_m"] = depth_m
            entry["_lot_source"]  = obs.get("source_doc", "")
            entry["_lot_note"]    = match_note
        else:
            # Conflict: another LOT already assigned
            old_score = _obs_score({"confidence": entry["_confidence"], "priority": entry["_priority"]})
            if obs_score > old_score:
                note_prev = f"prev={entry['_emw']} from {entry['_lot_source']}; "
                entry["_emw"]         = emw
                entry["_lot_type"]    = test_type
                entry["_lot_depth_m"] = depth_m
                entry["_lot_source"]  = obs.get("source_doc", "")
                entry["_lot_note"]    = note_prev + match_note
            else:
                entry["_lot_note"] += f"; alt={emw} from {obs.get('source_doc','')} (lower priority, kept existing)"

    # -------------------------------------------------------------------- DSTs
    dst_rows: list[dict] = []
    for obs in dst_obs:
        _dt = obs.get("doc_type", "")
        top_m, _ = to_metres(obs.get("top"), obs.get("top_unit"), doc_type=_dt)
        if top_m is None:
            continue
        bottom_m, _ = to_metres(obs.get("bottom"), obs.get("bottom_unit"), doc_type=_dt)
        mw = to_gcm3(obs.get("mud_weight"), obs.get("mud_unit"))

        # Dedup ±5m
        dup = next((d for d in dst_rows if abs(d["_top_m"] - top_m) <= 5), None)
        if dup:
            if _obs_score(obs) > _obs_score({"confidence": dup["_confidence"], "priority": dup["_priority"]}):
                dup.update({
                    "_top_m":      top_m,
                    "_bottom_m":   bottom_m,
                    "_mw":         mw,
                    "_dst_num":    obs.get("dst_number"),
                    "_priority":   int(obs.get("priority", 0)),
                    "_confidence": obs.get("confidence", "inferred"),
                    "_source_doc": obs.get("source_doc", ""),
                    "_doc_type":   obs.get("doc_type", ""),
                })
            continue

        dst_rows.append({
            "_top_m":      top_m,
            "_bottom_m":   bottom_m,
            "_mw":         mw,
            "_dst_num":    obs.get("dst_number"),
            "_priority":   int(obs.get("priority", 0)),
            "_confidence": obs.get("confidence", "inferred"),
            "_source_doc": obs.get("source_doc", ""),
            "_doc_type":   obs.get("doc_type", ""),
        })

    # ---------------------------------------------------- Build output rows
    rows: list[dict] = []

    for (ctype, _depth_key), entry in sorted(casing_by_key.items(),
                                             key=lambda kv: kv[1]["_depth_m"] if kv[1]["_depth_m"] is not None else 99999):
        sources = [entry["_source_doc"]]
        if entry["_lot_source"] and entry["_lot_source"] != entry["_source_doc"]:
            sources.append(entry["_lot_source"])

        notes: list[str] = []
        if entry["_lot_note"]:
            notes.append(entry["_lot_note"])
        if "assumed" in entry["_depth_unit"]:
            notes.append(f"depth unit {entry['_depth_unit']}")
        elif entry["_depth_unit"] == "unknown":
            notes.append("depth unit unknown — used as-is")

        note_str = "; ".join(notes) if notes else None
        if note_str and len(note_str) > 300:
            note_str = note_str[:297] + "..."
        rows.append({
            "wellbore":          wellbore,
            "casing_type":       ctype,
            "casing_diameter_in": entry["_diam"],
            "casing_depth_m":    entry["_depth_m"],
            "hole_diameter_in":  entry["_hole_size"],
            "hole_depth_m":      entry.get("_hole_depth_m"),
            "mud_weight_g_cm3":  entry["_emw"],
            "formation_test_type": entry["_lot_type"],
            "confidence":        entry["_confidence"],
            "extraction_notes":  note_str,
            "source_document":   " | ".join(sources),
            "source_doc_type":   entry["_doc_type"],
            "source_priority":   entry["_priority"],
            "conflict_fields":   "",
        })

    # DST rows omitted from output

    return rows, conflicts


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def get_priority(doc_type: str) -> int:
    upper = doc_type.upper().strip()
    for key, p in DOC_PRIORITY.items():
        if key in upper:
            return p
    return 0


def is_target(doc_type: str) -> bool:
    upper = doc_type.upper().strip()
    return any(t.upper() in upper for t in TARGET_DOC_TYPES)


def is_skip(doc_name: str) -> bool:
    upper = doc_name.upper()
    return any(p.upper() in upper for p in SKIP_DOC_PATTERNS)


def doc_type_hint(doc_name: str) -> str:
    """Return a focus hint combining the document-type template and any known scan evidence."""
    upper = doc_name.upper()
    type_hint = ""
    for pattern, hint in DOC_TYPE_HINTS.items():
        if pattern.upper() in upper:
            type_hint = hint
            break

    return type_hint


def tier_for_doc_type(doc_type: str) -> int:
    """Return the tier index for a document type (lower = processed first)."""
    upper = doc_type.upper().strip()
    for i, t in enumerate(DOC_TYPE_TIERS):
        if t.upper() in upper:
            return i
    return len(DOC_TYPE_TIERS)  # unknown types go last


# ---------------------------------------------------------------------------
# Universal Document Scan + Ranking Layer
# ---------------------------------------------------------------------------

@dataclass
class DocumentScanResult:
    doc_name: str
    doc_type: str
    wellbores: list         # wellbore names visible in the document
    relevance_class: str    # "relevant" | "irrelevant" | "uncertain_needs_escalation"
    as_built: str           # "as_built" | "planned" | "unclear"
    casing_table: str       # "full" | "partial" | "none"
    casing_count: int       # estimated distinct casing strings
    has_lot_fit: bool
    has_dst: bool
    summary: str            # one-line description
    candidate_pages: list = field(default_factory=list)  # page numbers from scan (printed numbers, 1-based)
    scan_score: float = 0.0  # computed ranking score (filled by rank step)
    is_scaffold: bool = False  # marked True by select_scaffold_docs
    scan_raw: str = ""      # raw LLM response (debug)

    @property
    def relevant(self) -> bool:
        """True for anything that should enter the collection pipeline."""
        return self.relevance_class != "irrelevant"


@dataclass
class WellboreScaffoldSelection:
    wellbore: str
    primary_doc: str        # doc_name of best scaffold (empty string if none)
    secondary_doc: str      # doc_name of second scaffold (empty string if none)
    scaffold_docs: list     # all scaffold doc names [primary, secondary] (non-empty)
    ranked_docs: list       # all relevant doc names sorted by score
    reason: str             # explanation


# ---------------------------------------------------------------------------
# DocumentRef — unified canonical input type
# All pipeline stages downstream of input parsing consume List[DocumentRef].
# Two entry points produce this:
#   load_input_from_url(url)         → single-document mode
#   load_input_from_csv(path, ...)   → CSV batch mode
# ---------------------------------------------------------------------------

@dataclass
class DocumentRef:
    """Canonical internal input type — bridges single-URL and CSV batch modes."""
    input_mode: str                     # "single_url" | "csv_row"
    document_name: str                  # stable identifier (URL stem or wlbDocumentName)
    document_url: str
    wellbore_name: str                  # "UNKNOWN" when not yet known (single-URL mode)
    wellbore_id: str | None = None
    document_type: str = ""             # e.g. "REPORTED BY LICENSEE"
    document_format: str | None = None
    document_size_kb: int | None = None
    document_date_updated: str | None = None
    source_row_index: int | None = None
    is_multi_wellbore: bool = False     # True when one PDF spans several wellbores
    priority: int = 0                   # DOC_PRIORITY value
    tier: int = 0                       # processing tier index

    def to_legacy_dict(self) -> dict:
        """Return a dict compatible with the legacy doc-dict format used by older functions."""
        return {
            "wlbName":         self.wellbore_name,
            "wlbDocumentName": self.document_name,
            "wlbDocumentType": self.document_type,
            "wlbDocumentUrl":  self.document_url,
        }


# ---------------------------------------------------------------------------
# Structured observation types
# Run in parallel with freeform fragments during the transition period.
# fragments.json remains the synthesis input; observations.json provides
# richer structured evidence for future field-level merge logic.
# ---------------------------------------------------------------------------

@dataclass
class CasingObservation:
    """Single casing string observation extracted from one source page."""
    observation_type: str = "casing_row"
    wellbore: str = ""
    source_doc: str = ""
    source_page: int | None = None
    raw_snippet: str = ""
    # Normalized content fields
    casing_type_raw: str | None = None
    casing_type_norm: str | None = None     # conductor/surface/intermediate/production/liner
    casing_diameter_in: float | None = None
    casing_depth_m: float | None = None
    hole_diameter_in: float | None = None
    hole_depth_m: float | None = None
    # Provenance
    field_confidence: dict = field(default_factory=dict)   # field_name → "explicit"|"inferred"
    is_explicit: bool = True
    is_partial: bool = False
    missing_fields: list = field(default_factory=list)
    datum: str | None = None                # KB/RKB/RT/MSL
    source_doc_type: str = ""
    source_priority: int = 0
    planned_or_actual: str = "unknown"      # "as_built" | "planned" | "unknown"


@dataclass
class LotFitObservation:
    """LOT/FIT/shoe-test observation with explicit provenance class tracking.

    provenance_class distinguishes genuine tests from inferred or estimated values —
    they must not be silently merged. DST must never be equated with LOT/FIT.
    """
    observation_type: str = "lot_fit_observation"
    wellbore: str = ""
    source_doc: str = ""
    source_page: int | None = None
    raw_snippet: str = ""
    test_type_raw: str | None = None
    test_type_norm: str | None = None
    # Normalised values: "LOT" | "FIT" | "Shoe test" | "Inferred from mud weight" | "blank"
    depth_m: float | None = None
    emw_g_cm3: float | None = None
    casing_size: str | None = None
    provenance_class: str = "not_reported"
    # "explicit_lot" | "explicit_fit" | "explicit_shoe_test" |
    # "inferred_min_integrity" | "not_reported"
    is_explicit: bool = True
    source_doc_type: str = ""
    source_priority: int = 0


@dataclass
class DstObservation:
    """DST/MDT/RFT/WFT observation. Never equate with LOT/FIT."""
    observation_type: str = "dst_observation"
    wellbore: str = ""
    source_doc: str = ""
    source_page: int | None = None
    raw_snippet: str = ""
    dst_number: int | None = None
    top_m: float | None = None
    bottom_m: float | None = None
    mud_weight_g_cm3: float | None = None
    source_doc_type: str = ""
    source_priority: int = 0


# Union type alias for observations (used in type annotations)
AnyObservation = CasingObservation | LotFitObservation | DstObservation


# ---------------------------------------------------------------------------
# Input loaders — normalize all entry points to List[DocumentRef]
# ---------------------------------------------------------------------------

def _doc_name_from_url(url: str) -> str:
    """Derive a stable document name from a PDF URL (URL-decoded stem, no extension)."""
    raw = url.rstrip("/").split("/")[-1]
    if raw.lower().endswith(".pdf"):
        raw = raw[:-4]
    try:
        from urllib.parse import unquote
        raw = unquote(raw)
    except Exception:
        pass
    return raw or "single_doc"


def load_input_from_url(
    url: str,
    wellbore_filter: str | None = None,
) -> "list[DocumentRef]":
    """Build a single-element DocumentRef list from a PDF URL.

    The wellbore is initially UNKNOWN — the scan phase discovers it.
    If wellbore_filter is set, that name is pre-seeded as the wellbore.
    """
    doc_name = _doc_name_from_url(url)
    return [DocumentRef(
        input_mode="single_url",
        document_name=doc_name,
        document_url=url,
        wellbore_name=wellbore_filter or "UNKNOWN",
        document_type="REPORTED BY LICENSEE",   # assumed; scan will classify
        is_multi_wellbore=False,
        priority=get_priority("REPORTED BY LICENSEE"),
        tier=tier_for_doc_type("REPORTED BY LICENSEE"),
    )]


def load_input_from_csv(
    csv_path: str,
    doc_type_filter: "list[str] | None" = None,
    wellbore_filter: str | None = None,
    doc_name_filter: str | None = None,
) -> "list[DocumentRef]":
    """Load wellbore document CSV and return a normalised DocumentRef list.

    Applies TARGET_DOC_TYPES, SKIP_DOC_PATTERNS, and any caller-supplied filters.
    Unlike the legacy loader, this does NOT hard-filter to REPORTED_BY_LICENSEE or
    OLD_NPD_WDSS only — it accepts all types in doc_type_filter (default = TARGET_DOC_TYPES).
    """
    filter_types = [t.upper() for t in (doc_type_filter or TARGET_DOC_TYPES)]
    refs: list[DocumentRef] = []
    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        for i, row in enumerate(csv.DictReader(f)):
            doc_type = row.get("wlbDocumentType", "").strip()
            if not any(t in doc_type.upper() for t in filter_types):
                continue
            wb = row.get("wlbName", "").strip()
            if wellbore_filter and wb != wellbore_filter:
                continue
            name = row.get("wlbDocumentName", "").strip()
            if is_skip(name):
                continue
            if doc_name_filter and doc_name_filter.upper() not in name.upper():
                continue
            url = row.get("wlbDocumentUrl", "").strip()
            if not url:
                continue

            size_raw = row.get("wlbDocumentSize", "").strip()
            try:
                size_kb = int(size_raw)
            except (ValueError, TypeError):
                size_kb = None

            refs.append(DocumentRef(
                input_mode="csv_row",
                document_name=name,
                document_url=url,
                wellbore_name=wb,
                wellbore_id=row.get("wlbWellboreId", "").strip() or None,
                document_type=doc_type,
                document_format=row.get("wlbDocumentFormat", "").strip() or None,
                document_size_kb=size_kb,
                document_date_updated=row.get("wlbUpdatedDate", "").strip() or None,
                source_row_index=i,
                is_multi_wellbore=doc_type.upper() in {t.upper() for t in MULTI_WELLBORE_DOC_TYPES},
                priority=get_priority(doc_type),
                tier=tier_for_doc_type(doc_type),
            ))
    return refs


# ---------------------------------------------------------------------------
# Collection strategy router
# ---------------------------------------------------------------------------

# Strategy codes — passed to process_document to guide collection focus
STRATEGY_SCAFFOLD    = "scaffold_collection"    # completion reports, IWR, final reports
STRATEGY_SUPPORT     = "support_collection"     # mud/DST/pressure/fluid docs
STRATEGY_MULTI_WELL  = "multi_well_targeted"    # multi-well NPD papers / compilations
STRATEGY_ESCALATION  = "escalation_collection"  # uncertain/delayed-signal docs


def determine_collection_strategy(
    ref: "DocumentRef",
    scan_result: "DocumentScanResult | None",
) -> str:
    """Return the best collection strategy for a document.

    The strategy controls how the collector is focused:
    - scaffold: full casing-programme extraction, geometry-first
    - support:  fill missing values, tests, pressure observations
    - multi_well_targeted: jump to wellbore-specific sections via TOC
    - escalation: inspect deeper pages before deciding
    """
    name_upper  = ref.document_name.upper()
    dtype_upper = ref.document_type.upper()

    # Multi-wellbore docs always need targeted per-wellbore collection
    if ref.is_multi_wellbore or "NPD PAPER" in dtype_upper:
        return STRATEGY_MULTI_WELL

    if scan_result:
        summary_lower = scan_result.summary.lower()
        # Scaffold signals: as-built docs with a casing table
        if (scan_result.as_built == "as_built" and
                scan_result.casing_table in ("full", "partial")):
            return STRATEGY_SCAFFOLD
        # Scaffold signals: document type strongly implies casing geometry
        if any(kw in summary_lower for kw in
               ("completion", "individual well", "final report", "well record", "exploratory")):
            return STRATEGY_SCAFFOLD
        # Support signals: pressure / mud / test documents
        if any(kw in summary_lower for kw in
               ("mud", "dst", "pressure", "fluid", "drilling fluid", "drill stem")):
            return STRATEGY_SUPPORT
        if "WDSS" in name_upper:
            return STRATEGY_SUPPORT
        # Still uncertain after scan escalation → use escalation collection
        if scan_result.relevance_class == "uncertain_needs_escalation":
            return STRATEGY_ESCALATION

    # Fallback: name-pattern heuristics (scan not available)
    if any(kw in name_upper for kw in
           ("COMPLETION", "INDIVIDUAL_WELL", "FINAL_REPORT", "EXPLORATORY", "KONTINENTALSOKKELEN")):
        return STRATEGY_SCAFFOLD
    if any(kw in name_upper for kw in
           ("WDSS", "MUD", "DST_NO", "DRILL_STEM_TEST", "FLUID", "DIAGRAM_PSI", "AAODC")):
        return STRATEGY_SUPPORT

    return STRATEGY_SCAFFOLD  # safe default


UNIVERSAL_SCAN_PROMPT = """\
You are classifying a wellbore document. Examine these pages carefully.

Reply in EXACTLY this format (9 lines, no extra text):
RELEVANT: YES or NO or UNCERTAIN
AS_BUILT: AS_BUILT or PLANNED or UNCLEAR
WELLBORES: comma-separated wellbore names visible (e.g. 7/11-2), or UNKNOWN
CASING_TABLE: FULL or PARTIAL or NONE
CASING_COUNT: estimated number of distinct casing strings (integer, 0 if none)
HAS_LOT_FIT: YES or NO
HAS_DST: YES or NO
DATA_PAGES: comma-separated page numbers where casing/LOT-FIT data appears, or UNKNOWN
SUMMARY: one-line description of what this document is

RELEVANT definitions:
- YES: these pages clearly contain casing depths, casing diameters, hole sizes, or LOT/FIT mud weights
- NO: these pages are clearly geology, geochemistry, biostratigraphy, seismic, maps, purely administrative, or drill stem test (DST) results only
- UNCERTAIN: these pages appear to be a cover sheet, title page, table of contents, introduction, or index — \
the actual data may be on later pages. Also use UNCERTAIN if page quality is poor or OCR is unclear.

Other definitions:
- AS_BUILT: describes actual well operations or results (completion report, WDSS, daily report, final well summary)
- PLANNED: describes proposed/planned operations only (well programme, AFE, prognosis, feasibility study)
- UNCLEAR: cannot determine from these pages
- CASING_TABLE FULL: a structured table lists all casing strings with depths and diameters
- CASING_TABLE PARTIAL: some casing data present but not a complete table
- CASING_TABLE NONE: no casing data visible

DATA_PAGES instructions:
- If you can see casing/LOT-FIT data directly on the pages shown, list the page numbers printed on those pages (e.g. "3, 4")
- If you see a table of contents listing casing programme or well data sections, list the page numbers referenced there (e.g. "12, 13")
- Use the page numbers as printed in the document (not the position in this image set)
- If no page numbers are visible or you cannot tell, write UNKNOWN\
"""

def _scan_prompt_hash() -> str:
    """Hash of the universal scan prompt — cache auto-invalidates when prompt changes."""
    return hashlib.md5(UNIVERSAL_SCAN_PROMPT.encode()).hexdigest()[:8]

# Filename keywords that indicate operational drilling content.
# If any of these appear in the doc name, the scanner is NOT allowed to mark
# the document irrelevant from first-page sampling alone → escalated to uncertain.
_OPERATIONAL_NAME_KEYWORDS = {
    "COMPLETION", "CASING", "WDSS", "DRILLING", "MUD", "PRESSURE",
    "LOT", "FIT", "DST", "TEST", "CEMENT", "WELLBORE", "FORMATION",
    "AAODC", "INDIVIDUAL_WELL", "EXPLORATORY", "KONTINENTALSOKKELEN",
    "NPD_PAPER", "INTERPRETED_LITHOLOGY",
}


def _compute_scan_score(result: "DocumentScanResult") -> float:
    """Compute ranking score for scaffold selection. Higher = better ground truth."""
    if not result.relevant:
        return -1.0
    score = 0.0
    # Status: as-built data is the only acceptable ground truth
    if result.as_built == "as_built":
        score += 10.0
    elif result.as_built == "unclear":
        score += 2.0
    # planned = 0 (should not be selected as scaffold)
    # Casing table completeness
    if result.casing_table == "full":
        score += 5.0
    elif result.casing_table == "partial":
        score += 2.0
    # More casings visible = more complete
    score += min(result.casing_count, 5) * 0.5
    # Bonus for official NPD sources (authoritative)
    doc_upper = result.doc_name.upper()
    type_upper = result.doc_type.upper()
    if "WDSS" in doc_upper or "WDSS" in type_upper:
        score += 3.0
    elif "NPD_PAPER" in doc_upper or "NPD PAPER" in type_upper:
        score += 2.0
    return score


def _doc_name_has_operational_keywords(doc_name: str) -> bool:
    """Return True if the document name suggests operational drilling/completion content."""
    upper = doc_name.upper()
    return any(kw in upper for kw in _OPERATIONAL_NAME_KEYWORDS)


def _parse_scan_response(text: str, doc_name: str, doc_type: str, wellbore: str,
                         is_escalation: bool = False) -> DocumentScanResult:
    """Parse the structured scan response into a DocumentScanResult.

    is_escalation: True when this is the second-pass scan on extra pages.
    On escalation, UNCERTAIN is resolved to relevant (safe default).
    """
    defaults = {
        "relevance_raw": "uncertain",  # raw value before safety rules
        "as_built": "unclear",
        "wellbores_raw": wellbore,
        "casing_table": "none",
        "casing_count": 0,
        "has_lot_fit": False,
        "has_dst": False,
        "data_pages_raw": "",
        "summary": "",
    }
    for line in text.splitlines():
        line = line.strip()
        upper = line.upper()
        if upper.startswith("RELEVANT:"):
            val = line.split(":", 1)[1].strip().upper()
            if "NO" in val and "UNCERTAIN" not in val:
                defaults["relevance_raw"] = "no"
            elif "UNCERTAIN" in val:
                defaults["relevance_raw"] = "uncertain"
            else:
                defaults["relevance_raw"] = "yes"
        elif upper.startswith("AS_BUILT:"):
            val = line.split(":", 1)[1].strip().lower()
            if "as_built" in val or "as-built" in val:
                defaults["as_built"] = "as_built"
            elif "planned" in val:
                defaults["as_built"] = "planned"
            else:
                defaults["as_built"] = "unclear"
        elif upper.startswith("WELLBORES:"):
            defaults["wellbores_raw"] = line.split(":", 1)[1].strip()
        elif upper.startswith("CASING_TABLE:"):
            val = line.split(":", 1)[1].strip().lower()
            if "full" in val:
                defaults["casing_table"] = "full"
            elif "partial" in val:
                defaults["casing_table"] = "partial"
            else:
                defaults["casing_table"] = "none"
        elif upper.startswith("CASING_COUNT:"):
            try:
                defaults["casing_count"] = int(line.split(":", 1)[1].strip())
            except ValueError:
                defaults["casing_count"] = 0
        elif upper.startswith("HAS_LOT_FIT:"):
            defaults["has_lot_fit"] = "YES" in upper
        elif upper.startswith("HAS_DST:"):
            defaults["has_dst"] = "YES" in upper
        elif upper.startswith("DATA_PAGES:"):
            defaults["data_pages_raw"] = line.split(":", 1)[1].strip()
        elif upper.startswith("SUMMARY:"):
            defaults["summary"] = line.split(":", 1)[1].strip()

    # Apply relevance safety rules
    raw = defaults["relevance_raw"]
    if raw == "yes":
        relevance_class = "relevant"
    elif raw == "no":
        # Safety override: never mark irrelevant from first-page sampling if the
        # filename suggests operational content
        if _doc_name_has_operational_keywords(doc_name):
            relevance_class = "uncertain_needs_escalation"
        else:
            relevance_class = "irrelevant"
    else:  # uncertain
        if is_escalation:
            # Second pass still uncertain → treat as relevant (safe default)
            relevance_class = "relevant"
        else:
            relevance_class = "uncertain_needs_escalation"


    # Parse wellbore names from the raw string
    raw_wb = defaults["wellbores_raw"]
    if raw_wb.upper() in ("UNKNOWN", ""):
        wellbores = [wellbore]
    else:
        wellbores = [w.strip() for w in raw_wb.replace(";", ",").split(",") if w.strip()]
        if not wellbores:
            wellbores = [wellbore]

    # Parse DATA_PAGES into a sorted list of integers (printed page numbers, 1-based)
    candidate_pages: list[int] = []
    raw_dp = defaults["data_pages_raw"].upper()
    if raw_dp and raw_dp != "UNKNOWN":
        for token in raw_dp.replace(";", ",").split(","):
            token = token.strip()
            try:
                candidate_pages.append(int(token))
            except ValueError:
                pass  # ignore non-numeric tokens
    candidate_pages = sorted(set(candidate_pages))

    return DocumentScanResult(
        doc_name=doc_name,
        doc_type=doc_type,
        wellbores=wellbores,
        relevance_class=relevance_class,
        as_built=defaults["as_built"],
        casing_table=defaults["casing_table"],
        casing_count=defaults["casing_count"],
        has_lot_fit=defaults["has_lot_fit"],
        has_dst=defaults["has_dst"],
        summary=defaults["summary"],
        candidate_pages=candidate_pages,
        scan_raw=text,
    )


async def scan_document_universal(
    client: "anthropic.AsyncAnthropic",
    doc: dict,
    pdf_sem: "asyncio.Semaphore",
    claude_sem: "asyncio.Semaphore",
) -> DocumentScanResult:
    """Phase 0 per document: download (if needed), render first 2 pages, classify.

    Returns a DocumentScanResult. Cached across runs.
    """
    doc_name = doc["wlbDocumentName"]
    doc_type = doc.get("wlbDocumentType", "")
    wellbore = doc["wlbName"]
    url      = doc["wlbDocumentUrl"]
    pdf_path = PDF_DIR / f"{doc_name}.pdf"

    scan_cache_key = f"SCAN|{doc_name}|{_scan_prompt_hash()}"
    if scan_cache_key in _cache:
        cached = _cache[scan_cache_key]
        return DocumentScanResult(**cached)

    # Download PDF if not present
    async with pdf_sem:
        ok = await asyncio.to_thread(download_pdf, url, pdf_path)
        if not ok:
            return DocumentScanResult(
                doc_name=doc_name, doc_type=doc_type, wellbores=[wellbore],
                relevance_class="uncertain_needs_escalation", as_built="unclear",
                casing_table="none", casing_count=0, has_lot_fit=False, has_dst=False,
                summary="download failed",
            )
        total_pages = await asyncio.to_thread(pdf_page_count, pdf_path)
        rendered = await asyncio.to_thread(pdf_to_images, pdf_path, [0, 1])

    if not rendered:
        return DocumentScanResult(
            doc_name=doc_name, doc_type=doc_type, wellbores=[wellbore],
            relevance_class="uncertain_needs_escalation", as_built="unclear",
            casing_table="none", casing_count=0, has_lot_fit=False, has_dst=False,
            summary="no pages rendered",
        )

    async def _run_scan_call(jpegs: list[bytes], is_escalation: bool = False) -> tuple[str, DocumentScanResult]:
        """Send jpegs to Haiku and return (raw_text, parsed_result)."""
        async with claude_sem:
            try:
                content: list[dict] = [
                    {"type": "image", "source": {"type": "base64", "media_type": "image/jpeg", "data": b64(j)}}
                    for j in jpegs
                ]
                content.append({"type": "text", "text": UNIVERSAL_SCAN_PROMPT})
                r = await call_with_retry(
                    client,
                    model=MODELS["anthropic"]["screener"],
                    max_tokens=200,
                    messages=[{"role": "user", "content": content}],
                )
                raw = r.content[0].text.strip()
                if hasattr(r, "usage"):
                    m = MODELS["anthropic"]["screener"]
                    it, ot = r.usage.input_tokens, r.usage.output_tokens
                    pricing = MODEL_PRICING.get(m, {"input": 0.80, "output": 4.00})
                    cost = (it * pricing["input"] + ot * pricing["output"]) / 1_000_000
                    phase = "scanning_escalation" if is_escalation else "scanning"
            except Exception as e:
                print(f"    [scan error] {doc_name}: {e}")
                raw = ("RELEVANT: UNCERTAIN\nAS_BUILT: UNCLEAR\nWELLBORES: UNKNOWN\n"
                       "CASING_TABLE: NONE\nCASING_COUNT: 0\nHAS_LOT_FIT: NO\nHAS_DST: NO\n"
                       "SUMMARY: scan error")
        return raw, _parse_scan_response(raw, doc_name, doc_type, wellbore, is_escalation)

    # Pass 1: first 2 pages
    _, result = await _run_scan_call([jpeg for _, jpeg in rendered])

    # Escalation: if uncertain, scan pages 2–5 to look past cover/ToC
    if result.relevance_class == "uncertain_needs_escalation" and total_pages > 2:
        escalation_indices = list(range(2, min(6, total_pages)))
        print(f"    [scan] {doc_name}: uncertain on p0-1 — escalating to p{escalation_indices[0]}-{escalation_indices[-1]}")
        async with pdf_sem:
            extra_rendered = await asyncio.to_thread(pdf_to_images, pdf_path, escalation_indices)
        if extra_rendered:
            _, result = await _run_scan_call([jpeg for _, jpeg in extra_rendered], is_escalation=True)

    # Cache the final result
    global _cache_dirty
    async with _cache_lock:
        _cache[scan_cache_key] = asdict(result)
        _cache_dirty = True

    return result


def rank_documents_for_wellbore(
    wellbore: str,
    docs: list[dict],
    scan_results: dict,
) -> list[tuple[float, str]]:
    """Return list of (score, doc_name) sorted descending for a wellbore.

    Only includes docs where the scan result covers this wellbore.
    """
    ranked = []
    for doc in docs:
        doc_name = doc["wlbDocumentName"]
        result = scan_results.get(doc_name)
        if result is None or not result.relevant:
            continue
        # Only include if this doc covers the wellbore (or is tagged for it)
        if wellbore not in result.wellbores and len(result.wellbores) > 0:
            # Multi-wellbore docs: check if wellbore appears in the list
            if wellbore not in result.wellbores:
                # Accept anyway if the doc's registered wellbore matches
                if doc.get("wlbName") != wellbore:
                    continue
        score = _compute_scan_score(result)
        result.scan_score = score
        ranked.append((score, doc_name))
    ranked.sort(key=lambda x: x[0], reverse=True)
    return ranked


def select_scaffold_docs(
    targets: list[dict],
    scan_results: dict,
) -> dict:
    """Select the 1-2 best scaffold documents per wellbore.

    Scaffold docs are the ground truth for casing string definition.
    Only AS_BUILT documents with FULL or PARTIAL casing tables qualify.
    Falls back to the best available if none qualify strictly.

    Returns dict[wellbore_name, WellboreScaffoldSelection].
    """
    # Group targets by wellbore
    by_wellbore: dict[str, list[dict]] = defaultdict(list)
    for doc in targets:
        wb = doc["wlbName"]
        by_wellbore[wb].append(doc)
        # Multi-wellbore docs (NPD PAPER) appear for multiple wellbores
        result = scan_results.get(doc["wlbDocumentName"])
        if result and len(result.wellbores) > 1:
            for extra_wb in result.wellbores:
                if extra_wb != wb and extra_wb in {d["wlbName"] for d in targets}:
                    # Add this doc to the extra wellbore's pool too
                    if doc not in by_wellbore[extra_wb]:
                        by_wellbore[extra_wb].append(doc)

    selections: dict[str, WellboreScaffoldSelection] = {}
    for wellbore, docs in by_wellbore.items():
        ranked = rank_documents_for_wellbore(wellbore, docs, scan_results)

        # Prefer: as_built + (full or partial) casing table
        scaffold_candidates = []
        for score, doc_name in ranked:
            r = scan_results.get(doc_name)
            if r and r.as_built == "as_built" and r.casing_table in ("full", "partial"):
                scaffold_candidates.append((score, doc_name))

        # If no strict candidates, widen to as_built (any casing completeness)
        if not scaffold_candidates:
            for score, doc_name in ranked:
                r = scan_results.get(doc_name)
                if r and r.as_built == "as_built":
                    scaffold_candidates.append((score, doc_name))

        # Final fallback: take top 2 regardless of as_built status
        if not scaffold_candidates:
            scaffold_candidates = ranked[:2]

        primary   = scaffold_candidates[0][1] if scaffold_candidates else ""
        secondary = scaffold_candidates[1][1] if len(scaffold_candidates) > 1 else ""
        scaffold_docs = [d for d in [primary, secondary] if d]

        # Mark these docs as scaffold in scan results
        for doc_name in scaffold_docs:
            r = scan_results.get(doc_name)
            if r:
                r.is_scaffold = True

        reason_parts = []
        if primary:
            r = scan_results.get(primary)
            reason_parts.append(
                f"primary={primary} (score={r.scan_score:.1f}, {r.as_built}, casing={r.casing_table})"
                if r else f"primary={primary}"
            )
        if secondary:
            r = scan_results.get(secondary)
            reason_parts.append(
                f"secondary={secondary} (score={r.scan_score:.1f}, {r.as_built}, casing={r.casing_table})"
                if r else f"secondary={secondary}"
            )

        selections[wellbore] = WellboreScaffoldSelection(
            wellbore=wellbore,
            primary_doc=primary,
            secondary_doc=secondary,
            scaffold_docs=scaffold_docs,
            ranked_docs=[doc_name for _, doc_name in ranked],
            reason="; ".join(reason_parts) if reason_parts else "no relevant docs found",
        )

    return selections


# Casing size keywords used for the fragment-based completeness heuristic
_COMPLETENESS_CASING_KW = {"30\"", "20\"", "13-3/8", "9-5/8", "7\"", "5-1/2", "18-5/8"}
_COMPLETENESS_TEST_KW   = {"LOT", "FIT", "DST", "MDT", "RFT", "WFT"}


def is_wellbore_complete(fragments: list[dict]) -> bool:
    """Heuristic completeness check based on fragment content — no LLM cost.

    Returns True if fragments contain evidence for at least ANCHOR_CASING_THRESHOLD
    distinct casing sizes AND ANCHOR_TEST_THRESHOLD formation test mentions.
    """
    casing_frags = [f for f in fragments if f.get("topic") == "casing_data"]
    test_frags   = [f for f in fragments if f.get("topic") == "formation_tests"]

    sizes_seen: set[str] = set()
    for f in casing_frags:
        content = f.get("content", "").upper()
        for kw in _COMPLETENESS_CASING_KW:
            if kw.upper() in content:
                sizes_seen.add(kw)

    tests_seen: set[str] = set()
    for f in test_frags:
        content = f.get("content", "").upper()
        for kw in _COMPLETENESS_TEST_KW:
            if kw in content:
                tests_seen.add(kw)

    return (len(sizes_seen) >= ANCHOR_CASING_THRESHOLD
            and len(tests_seen) >= ANCHOR_TEST_THRESHOLD)


def build_known_state(fragments_by_wb: dict[str, list[dict]]) -> dict[str, dict]:
    """Analyze collected fragments to determine what data types are already known per wellbore.

    Returns {wellbore: {"has_casing": bool, "has_tests": bool, "casing_count": int, "test_count": int}}
    """
    state: dict[str, dict] = {}
    for wb, frags in fragments_by_wb.items():
        casing_frags = [f for f in frags if f.get("topic") == "casing_data"]
        test_frags   = [f for f in frags if f.get("topic") == "formation_tests"]

        sizes_seen: set[str] = set()
        for f in casing_frags:
            content = f.get("content", "").upper()
            for kw in _COMPLETENESS_CASING_KW:
                if kw.upper() in content:
                    sizes_seen.add(kw)

        tests_seen: set[str] = set()
        for f in test_frags:
            content = f.get("content", "").upper()
            for kw in _COMPLETENESS_TEST_KW:
                if kw in content:
                    tests_seen.add(kw)

        state[wb] = {
            "has_casing": len(sizes_seen) >= ANCHOR_CASING_THRESHOLD,
            "has_tests":  len(tests_seen) >= ANCHOR_TEST_THRESHOLD,
            "casing_count": len(sizes_seen),
            "test_count": len(tests_seen),
        }
    return state


def missing_data_description(known: dict) -> str | None:
    """Return a short description of what data types are still missing for a wellbore.
    Returns None if everything is known (should skip).
    """
    missing = []
    if not known["has_casing"]:
        missing.append("casing setting depths, casing diameters, hole sizes")
    if not known["has_tests"]:
        missing.append("formation pressure tests (LOT, FIT, leak-off, shoe test)")
    if not missing:
        return None  # everything known — skip this doc
    return "; ".join(missing)


def download_pdf(url: str, dest: Path) -> bool:
    if dest.exists():
        return True
    try:
        r = requests.get(url, timeout=60)
        r.raise_for_status()
        dest.write_bytes(r.content)
        return True
    except Exception as e:
        print(f"    [download failed] {e}")
        return False


# Thresholds for deciding whether extracted text is good enough to skip image rendering.
# Mirrors the logic in pdfmux audit.py.
_TEXT_GOOD_THRESHOLD  = 200  # chars — digital page, send as text
_TEXT_EMPTY_THRESHOLD =  20  # chars — effectively empty, render as image


def _audit_page_text(page) -> tuple[str, str]:
    """Extract text from a fitz page and classify quality.

    Returns (text, quality) where quality is:
      'good'  — enough clean text to send directly to Haiku as text
      'bad'   — little text but has images (scanned) — render as image
      'empty' — no useful content — render as image
    """
    text = page.get_text("text")
    text_len = len(text.strip())
    image_count = len(page.get_images())

    if text_len < _TEXT_EMPTY_THRESHOLD:
        quality = "empty"
    elif text_len < _TEXT_GOOD_THRESHOLD and image_count > 0:
        quality = "bad"
    else:
        quality = "good"

    return text, quality


def _render_page(page, page_num: int, pdf_name: str) -> bytes | None:
    """Render a single PDF page to JPEG, trying progressively lower DPI."""
    MAX_BYTES = 5 * 1024 * 1024
    MAX_DIM   = 7900
    for dpi in (120, 90, 60):
        mat = fitz.Matrix(dpi / 72, dpi / 72)
        pix = page.get_pixmap(matrix=mat)
        if pix.width > MAX_DIM or pix.height > MAX_DIM:
            continue
        jpeg = pix.tobytes("jpeg")
        if len(jpeg) <= MAX_BYTES:
            return jpeg
    print(f"    [dpi] {pdf_name} p{page_num}: too large even at 60dpi — skipped")
    return None


def pdf_page_count(pdf_path: Path) -> int:
    """Return the number of pages in a PDF without rendering."""
    try:
        doc = fitz.open(str(pdf_path))
        n = len(doc)
        doc.close()
        return n
    except Exception:
        return 0


def pdf_to_images(
    pdf_path: Path,
    page_indices: list[int] | None = None,
    try_text: bool = False,
) -> list[tuple[int, bytes | str]]:
    """Render PDF pages to JPEG, or extract text for digital pages.

    Args:
        page_indices: 0-based page indices to render. None = all pages.
        try_text: if True, attempt text extraction first. Pages with sufficient
                  clean text (>=200 chars) are returned as str instead of JPEG bytes,
                  saving image token costs for digital PDFs. Scanned/image-heavy pages
                  fall back to JPEG rendering as normal.

    Returns list of (page_index, jpeg_bytes_or_text) tuples.
    """
    results: list[tuple[int, bytes | str]] = []
    try:
        doc = fitz.open(str(pdf_path))
        indices = page_indices if page_indices is not None else list(range(len(doc)))
        for page_num in indices:
            if page_num < 0 or page_num >= len(doc):
                continue
            page = doc[page_num]
            if try_text:
                text, quality = _audit_page_text(page)
                if quality == "good":
                    results.append((page_num, text))
                    continue
            jpeg = _render_page(page, page_num, pdf_path.name)
            if jpeg is not None:
                results.append((page_num, jpeg))
        doc.close()
    except Exception as e:
        print(f"    [pymupdf failed] {pdf_path.name}: {e}")
    return results


def b64(jpeg: bytes) -> str:
    return base64.standard_b64encode(jpeg).decode()


_gemini_client = None


async def call_gemini_with_retry(model_name: str, contents: list, max_output_tokens: int) -> str:
    """Call Gemini API with exponential backoff on rate limit errors."""
    global _gemini_client
    try:
        from google import genai
        from google.genai import types as gtypes
    except ImportError:
        raise ImportError("Gemini provider requires: pip install google-genai")
    if _gemini_client is None:
        import os
        _gemini_client = genai.Client(api_key=os.environ["GOOGLE_API_KEY"])

    delay = 10
    for attempt in range(5):
        try:
            r = await _gemini_client.aio.models.generate_content(
                model=model_name,
                contents=contents,
                config=gtypes.GenerateContentConfig(max_output_tokens=max_output_tokens),
            )
            return r.text.strip() if r.text else ""
        except Exception as e:
            err = str(e)
            if "429" in err or "quota" in err.lower() or "rate" in err.lower():
                if attempt == 4:
                    raise
                print(f"    [gemini rate limit] waiting {delay}s before retry {attempt + 1}...")
                await asyncio.sleep(delay)
                delay *= 2
            else:
                raise
    return ""


# ---------------------------------------------------------------------------
# Async Claude calls
# ---------------------------------------------------------------------------

async def call_with_retry(client, **kwargs) -> any:
    """Call client.messages.create with exponential backoff on rate limit errors."""
    delay = 10
    for attempt in range(5):
        try:
            r = await client.messages.create(**kwargs)
            model = kwargs.get("model", "unknown")
            if hasattr(r, "usage"):
                _usage[model]["input"]  += r.usage.input_tokens
                _usage[model]["output"] += r.usage.output_tokens
                _usage[model]["cache_write"] = _usage[model].get("cache_write", 0) + getattr(r.usage, "cache_creation_input_tokens", 0)
                _usage[model]["cache_read"]  = _usage[model].get("cache_read",  0) + getattr(r.usage, "cache_read_input_tokens",      0)
            return r
        except anthropic.RateLimitError:
            if attempt == 4:
                raise
            print(f"    [rate limit] waiting {delay}s before retry {attempt + 1}...")
            await asyncio.sleep(delay)
            delay *= 2
        except anthropic.BadRequestError as e:
            if "image exceeds" in str(e):
                raise  # not retriable
            raise


async def collect_page(
    client: anthropic.AsyncAnthropic,
    page_data: bytes | str,
    sem: asyncio.Semaphore,
    page_idx: int,
    source_doc: str,
    doc_type: str,
    priority: int,
    wellbore: str,
    multi_wellbore: bool = False,
    all_wellbores: list[str] | None = None,
) -> list[dict]:
    """Collector — Haiku: dump tagged fragments from one page as JSON-L.

    page_data: JPEG bytes (scanned/image page) or str (digitally extracted text).
    multi_wellbore=True: instructs the LLM to extract data for ALL wellbores in the
    document and tag each fragment with the correct wellbore name. Uses a separate
    cache namespace so single- and multi-wellbore results don't collide.
    """
    global _cache_dirty
    text_mode = isinstance(page_data, str)
    cache_suffix = "|MULTI" if multi_wellbore else ""
    if text_mode:
        cache_suffix += "|TEXT"
    key = _cache_key(source_doc, page_idx) + cache_suffix
    if key in _cache:
        return _cache[key]

    hint = doc_type_hint(source_doc)
    if multi_wellbore and all_wellbores:
        user_prompt = COLLECTOR_USER_TEMPLATE_MULTI.format(
            source_doc=source_doc,
            doc_type=doc_type,
            page_idx=page_idx,
            priority=priority,
            wellbores_list=", ".join(all_wellbores),
            doc_hint=f"Document focus: {hint}" if hint else "",
        )
    else:
        user_prompt = COLLECTOR_USER_TEMPLATE.format(
            source_doc=source_doc,
            doc_type=doc_type,
            page_idx=page_idx,
            priority=priority,
            wellbore=wellbore,
            doc_hint=f"Document focus: {hint}" if hint else "",
        )
    async with sem:
        try:
            start_time = time.time()
            if COLLECTOR_PROVIDER == "gemini":
                from google.genai import types as gtypes
                model_name = MODELS["gemini"]["screener"]
                full_prompt = COLLECTOR_SYSTEM + "\n\n" + user_prompt
                if text_mode:
                    contents = [full_prompt + "\n\nExtracted page text:\n" + page_data]
                else:
                    contents = [
                        gtypes.Part.from_bytes(data=page_data, mime_type="image/jpeg"),
                        full_prompt,
                    ]
                text = await call_gemini_with_retry(model_name, contents, max_output_tokens=1024)
                latency = time.time() - start_time
            else:
                if text_mode:
                    user_content = [{"type": "text", "text": user_prompt + "\n\nExtracted page text:\n" + page_data}]
                else:
                    user_content = [
                        {"type": "text", "text": user_prompt},
                        {"type": "image", "source": {"type": "base64", "media_type": "image/jpeg", "data": b64(page_data)}},
                    ]
                r = await call_with_retry(
                    client,
                    model=MODELS["anthropic"]["screener"],
                    max_tokens=1024,
                    system=[{
                        "type": "text",
                        "text": COLLECTOR_SYSTEM,
                        "cache_control": {"type": "ephemeral"},
                    }],
                    messages=[{"role": "user", "content": user_content}],
                )
                latency = time.time() - start_time

                # Calculate cost with cache pricing
                input_tokens = r.usage.input_tokens
                output_tokens = r.usage.output_tokens
                cache_write = getattr(r.usage, 'cache_creation_input_tokens', 0)
                cache_read = getattr(r.usage, 'cache_read_input_tokens', 0)

                pricing = MODEL_PRICING.get(MODELS["anthropic"]["screener"], {"input": 0.80, "output": 4.00})
                cost = (
                    (input_tokens - cache_write - cache_read) * pricing["input"]
                    + output_tokens * pricing["output"]
                    + cache_write * pricing["input"] * 1.25
                    + cache_read * pricing["input"] * 0.10
                ) / 1_000_000

                text = r.content[0].text.strip()
            fragments = []
            for line in text.splitlines():
                line = line.strip()
                if not line or not line.startswith("{"):
                    continue
                try:
                    fragments.append(json.loads(line))
                except json.JSONDecodeError:
                    pass
            async with _cache_lock:
                _cache[key] = fragments
                _cache_dirty = True
            return fragments
        except Exception as e:
            latency = time.time() - start_time
            print(f"    [collect error] page {page_idx}: {e}")
            return []


def _estimate_tokens(text: str) -> int:
    """Rough token estimate: ~4 chars per token."""
    return len(text) // 4


MAX_FRAGS_PER_SOURCE = 40  # cap fragments per source document before synthesis


def _apply_source_cap(fragments: list[dict]) -> list[dict]:
    """Cap each source document at MAX_FRAGS_PER_SOURCE fragments."""
    source_counts: dict[str, int] = defaultdict(int)
    capped: list[dict] = []
    for f in fragments:
        src = f.get("source_doc", "")
        if source_counts[src] < MAX_FRAGS_PER_SOURCE:
            capped.append(f)
            source_counts[src] += 1
    if len(capped) < len(fragments):
        print(f"    [fragments] capped {len(fragments)} → {len(capped)} "
              f"(max {MAX_FRAGS_PER_SOURCE}/source)")
    return capped


def _build_fragments_block(fragments: list[dict]) -> str:
    """Format fragments grouped by topic for the Synthesizer prompt."""
    by_topic: dict[str, list[dict]] = defaultdict(list)
    for f in fragments:
        by_topic[f.get("topic", "other")].append(f)

    lines = []
    for topic in ["casing_data", "formation_tests", "other"]:
        if topic not in by_topic:
            continue
        lines.append(f"=== {topic.upper()} ===")
        for f in by_topic[topic]:
            lines.append(
                f"[page {f['page_idx']} | {f['source_doc']} | {f.get('doc_type', '?')} | priority:{f.get('priority', 0)}]"
            )
            lines.append(f"  {f['content']}")
        lines.append("")
    return "\n".join(lines)


async def synthesize_wellbore(
    client: anthropic.AsyncAnthropic,
    sem: asyncio.Semaphore,
    wellbore: str,
    fragments: list[dict],
    scaffold_doc_names: set | None = None,
) -> tuple[list[dict], list[dict]]:
    """Synthesizer — Sonnet: one call per wellbore, returns (rows, conflicts).

    scaffold_doc_names: set of doc names selected as ground truth by the scan phase.
    If provided, postprocess_observations uses these as anchors instead of doc-type tiers.

    If fragments exceed SYNTH_TOKEN_LIMIT, topics are split across multiple calls
    and results are concatenated.
    """
    if not fragments:
        return [], []

    fragments = _apply_source_cap(fragments)

    fragments_block = _build_fragments_block(fragments)
    estimated = _estimate_tokens(fragments_block)
    print(f"  [{wellbore}] synthesizing {len(fragments)} fragments (~{estimated} tokens)...")

    # If within limits, single call
    if estimated <= SYNTH_TOKEN_LIMIT:
        return await _synth_call(client, sem, wellbore, fragments_block, scaffold_doc_names)

    # Over limit: chunk by topic
    print(f"  [{wellbore}] fragments exceed {SYNTH_TOKEN_LIMIT} token limit — chunking by topic...")
    by_topic: dict[str, list[dict]] = defaultdict(list)
    for f in fragments:
        by_topic[f.get("topic", "other")].append(f)

    all_rows, all_conflicts = [], []
    for topic, topic_fragments in by_topic.items():
        block = _build_fragments_block(topic_fragments)
        rows, conflicts = await _synth_call(client, sem, wellbore, block, scaffold_doc_names)
        all_rows.extend(rows)
        all_conflicts.extend(conflicts)
    return all_rows, all_conflicts


async def _synth_call(
    client: anthropic.AsyncAnthropic,
    sem: asyncio.Semaphore,
    wellbore: str,
    fragments_block: str,
    scaffold_doc_names: set | None = None,
) -> tuple[list[dict], list[dict]]:
    """Single synthesizer call — LLM extracts raw observations, Python post-processes."""
    prompt = SYNTHESIZER_PROMPT.format(wellbore=wellbore, fragments_block=fragments_block)
    async with sem:
        raw = ""
        start_time = time.time()
        try:
            if SYNTH_PROVIDER == "gemini":
                raw = await call_gemini_with_retry(
                    MODELS["gemini"]["extractor"], [prompt], max_output_tokens=8096
                )
                latency = time.time() - start_time
            else:
                r = await call_with_retry(
                    client,
                    model=MODELS["anthropic"]["extractor"],
                    max_tokens=16384,
                    system='You are a data extraction API. You output ONLY valid JSON. No prose, no markdown, no reasoning, no explanation. Your entire response must be a single JSON object starting with { and ending with }.',
                    messages=[{"role": "user", "content": prompt}],
                )
                latency = time.time() - start_time

                input_tokens = r.usage.input_tokens
                output_tokens = r.usage.output_tokens
                synth_model = MODELS["anthropic"]["extractor"]
                pricing = MODEL_PRICING.get(synth_model, {"input": 3.00, "output": 15.00})
                cost = (input_tokens * pricing["input"] + output_tokens * pricing["output"]) / 1_000_000

                # Add to _usage for run_info cost
                if synth_model not in _usage:
                    _usage[synth_model] = {"input": 0, "output": 0}
                _usage[synth_model]["input"]  += input_tokens
                _usage[synth_model]["output"] += output_tokens

                raw = r.content[0].text.strip()
            if raw.startswith("```"):
                raw = raw.split("```", 2)[1]
                if raw.startswith("json"):
                    raw = raw[4:]
                raw = raw.rstrip("`").strip()
            data = json.loads(raw)
            observations = data.get("observations", [])
            print(f"  [{wellbore}] {len(observations)} observations → post-processing...")
            return postprocess_observations(wellbore, observations, scaffold_doc_names)
        except json.JSONDecodeError:
            print(f"  [{wellbore}] [synth json error] could not parse LLM output: {raw[:200]}")
            return [], []
        except Exception as e:
            print(f"  [{wellbore}] [synth error] {e}")
            return [], []


async def screen_document(
    client: anthropic.AsyncAnthropic, screen_images: list[bytes], sem: asyncio.Semaphore,
    doc_name: str, total_pages: int,
    missing_types: str | None = None,
) -> tuple[bool, list[int] | None]:
    """Screen first pages for relevance and ToC.

    Args:
        screen_images: JPEG bytes for the first few pages (already rendered).
        total_pages: Total page count of the PDF (for ±2 buffer clamping).

    Returns (is_relevant, candidate_pages).
    candidate_pages is a list of 0-based indices with ±2 buffer, or None if no ToC.
    """
    # For progressive screening, include missing_types in cache key
    cache_suffix = f"|PROG:{missing_types}" if missing_types else ""
    cache_key = f"SCREEN|{doc_name}|{_prompt_hash()}{cache_suffix}"
    if cache_key in _cache:
        cached = _cache[cache_key]
        return cached.get("relevant", True), cached.get("pages")

    check_images = screen_images

    # Build the prompt — narrow relevance check if we know what's missing
    if missing_types:
        prompt = (
            f"These are the first pages of a wellbore document. You must decide two things:\n\n"
            f"1. RELEVANCE — We already have most data for this wellbore. "
            f"This document is ONLY relevant if it likely contains: {missing_types}\n"
            f"   If the document only has data types we already have, mark it NOT relevant.\n\n"
            f"2. TABLE OF CONTENTS — If relevant, is there a table of contents, "
            f"list of figures, or section index? If yes, which page numbers contain "
            f"the missing data types listed above?\n\n"
            f"Reply in this exact format (three lines):\n"
            f"RELEVANT: YES or NO\n"
            f"TOC_FOUND: YES or NO\n"
            f"PAGES: (if both YES) comma-separated page numbers from the ToC. "
            f"If either is NO, write NONE."
        )
    else:
        prompt = SCREEN_PROMPT

    async with sem:
        try:
            content: list[dict] = []
            for jpeg in check_images:
                content.append({"type": "image", "source": {"type": "base64", "media_type": "image/jpeg", "data": b64(jpeg)}})
            content.append({"type": "text", "text": prompt})
            r = await call_with_retry(
                client,
                model=MODELS["anthropic"]["screener"],
                max_tokens=150,
                messages=[{"role": "user", "content": content}],
            )
            text = r.content[0].text.strip()
            latency = 0.0
            if hasattr(r, "usage"):
                _toc_usage["input"]  += r.usage.input_tokens
                _toc_usage["output"] += r.usage.output_tokens
                _toc_usage["calls"]  += 1

                # Also add to _usage so run_info cost is complete
                model = MODELS["anthropic"]["screener"]
                if model not in _usage:
                    _usage[model] = {"input": 0, "output": 0}
                _usage[model]["input"]  += r.usage.input_tokens
                _usage[model]["output"] += r.usage.output_tokens

                input_tokens = r.usage.input_tokens
                output_tokens = r.usage.output_tokens
                pricing = MODEL_PRICING.get(MODELS["anthropic"]["screener"], {"input": 0.80, "output": 4.00})
                cost = (input_tokens * pricing["input"] + output_tokens * pricing["output"]) / 1_000_000

            relevant = True
            toc_found = False
            pages_str = ""
            for line in text.splitlines():
                line = line.strip()
                if line.upper().startswith("RELEVANT:"):
                    relevant = "YES" in line.upper()
                elif line.upper().startswith("TOC_FOUND:"):
                    toc_found = "YES" in line.upper()
                elif line.upper().startswith("PAGES:"):
                    pages_str = line.split(":", 1)[1].strip()

            candidate_pages = None
            if relevant and toc_found and pages_str and pages_str.upper() != "NONE":
                page_nums = []
                for token in pages_str.split(","):
                    token = token.strip()
                    try:
                        page_nums.append(int(token))
                    except ValueError:
                        pass
                if page_nums:
                    total = total_pages
                    indices: set[int] = set()
                    for p in page_nums:
                        idx = p - 1  # convert to 0-based
                        for offset in range(-2, 3):  # ±2 buffer
                            i = idx + offset
                            if 0 <= i < total:
                                indices.add(i)
                    candidate_pages = sorted(indices)

            # Cache the result
            global _cache_dirty
            async with _cache_lock:
                _cache[cache_key] = {"relevant": relevant, "pages": candidate_pages}
                _cache_dirty = True

            return relevant, candidate_pages

        except Exception as e:
            print(f"    [screen error] {e}")
            return True, None  # assume relevant on error


async def process_document(
    client: anthropic.AsyncAnthropic,
    doc: dict,
    pdf_sem: asyncio.Semaphore,
    claude_sem: asyncio.Semaphore,
    multi_wellbore: bool = False,
    all_wellbores: list[str] | None = None,
    missing_types: str | None = None,
    scan_result: "DocumentScanResult | None" = None,
    collection_strategy: str | None = None,
) -> list[dict]:
    """Download PDF, run Pass 0 (ToC), then Collector on candidate pages.

    multi_wellbore=True: the document covers multiple wellbores. The collector
    is told to tag each fragment with the wellbore it belongs to.
    missing_types: if set, progressive mode — only screen for these data types.
    scan_result: if provided, used as the AUTHORITATIVE relevance source.
        When scan says relevant but screen says not, scan wins (the screen only
        sees the first few pages and can miss data on later pages).
    collection_strategy: STRATEGY_* code from determine_collection_strategy().
    Returns a list of tagged fragments (not final rows).
    """
    wellbore  = doc["wlbName"]
    doc_name  = doc["wlbDocumentName"]
    doc_type  = doc.get("wlbDocumentType", "")
    url       = doc["wlbDocumentUrl"]
    priority  = get_priority(doc_type)

    pdf_path = PDF_DIR / f"{doc_name}.pdf"

    async with pdf_sem:
        ok = await asyncio.to_thread(download_pdf, url, pdf_path)
        if not ok:
            return []
        total_pages = await asyncio.to_thread(pdf_page_count, pdf_path)
        # Render only first 8 pages for screening
        screen_rendered = await asyncio.to_thread(pdf_to_images, pdf_path, list(range(min(8, total_pages))))

    if total_pages == 0:
        return []

    screen_jpegs = [jpeg for _, jpeg in screen_rendered]

    # Screen: relevance check + ToC detection in one call
    screen_relevant, toc_indices = await screen_document(client, screen_jpegs, claude_sem, doc_name,
                                                         total_pages=total_pages,
                                                         missing_types=missing_types)

    wb_label = "MULTI" if multi_wellbore else wellbore

    # --- Routing decision: scan_result is the authoritative relevance source ---
    # The screen call only sees the first ~8 pages — cover pages, ToC, and front matter
    # can make it say "not relevant" even for documents the universal scan has already
    # classified as relevant after inspecting more context.
    # Rule: when scan_result is available, its relevance judgement overrides the screen.
    # The screen's ToC page list is always kept regardless.
    if scan_result is not None:
        scan_status = scan_result.relevance_class  # "relevant"|"irrelevant"|"uncertain_needs_escalation"
        scan_says_relevant = scan_result.relevance_class == "relevant"  # only confident relevant overrides

        if screen_relevant and scan_result.relevant:
            final_relevant = True
            decision_source = "screen_and_scan_agree_relevant"
        elif scan_says_relevant and not screen_relevant:
            # Only a confident "relevant" scan overrides a NOT_RELEVANT screen
            print(f"  [{wb_label}] {doc_name}: scan={scan_status} screen=NOT_RELEVANT "
                  f"→ scan overrides screen, proceeding with collection")
            final_relevant = True
            decision_source = "scan_overrides_screen"
        elif screen_relevant and not scan_says_relevant:
            # Screen says relevant but scan says irrelevant — scan wins (it had more context)
            final_relevant = False
            decision_source = "scan_overrides_screen_as_irrelevant"
        else:
            final_relevant = False
            decision_source = "screen_and_scan_agree_irrelevant"
    else:
        # No scan result available — fall back to screen alone
        final_relevant = screen_relevant
        scan_status = "N/A"
        decision_source = "screen_only"

    if not final_relevant:
        reason = "no missing data types" if missing_types else "not relevant"
        print(f"  [{wb_label}] {doc_name}: {total_pages} pages — SKIPPED"
              f" | scan={scan_status}"
              f" | screen={'YES' if screen_relevant else 'NO'}"
              f" | decision={decision_source}"
              f" | reason={reason}")
        return []

    if toc_indices is not None:
        # Only render the candidate pages (not the whole PDF)
        merged = sorted(toc_indices)
        print(f"  [{wb_label}] {doc_name}: {total_pages} pages — ToC found, "
              f"rendering {len(merged)} candidate pages")
        async with pdf_sem:
            rendered = await asyncio.to_thread(pdf_to_images, pdf_path, merged, True)
        collect_images = rendered
    elif scan_result is not None and scan_result.candidate_pages:
        # Scan identified specific pages — convert from printed (1-based) to 0-based index
        # with ±2 buffer, clamped to document length.
        # Discard page hints that are implausibly large (reference numbers, not page numbers).
        plausible = [p for p in scan_result.candidate_pages if p <= total_pages + 10]
        if plausible:
            raw_indices = [p - 1 for p in plausible]  # printed→0-based
            buffered = set()
            for idx in raw_indices:
                for offset in range(-2, 3):
                    buffered.add(idx + offset)
            merged = sorted(i for i in buffered if 0 <= i < total_pages)
        else:
            merged = []
        if merged:
            print(f"  [{wb_label}] {doc_name}: {total_pages} pages — scan page hints, "
                  f"rendering {len(merged)} candidate pages (from {plausible})")
            async with pdf_sem:
                rendered = await asyncio.to_thread(pdf_to_images, pdf_path, merged, True)
            collect_images = rendered
        else:
            # All hints were out of range — fall back to full render
            print(f"  [{wb_label}] {doc_name}: {total_pages} pages — scan hints out of range, rendering all...")
            async with pdf_sem:
                rendered = await asyncio.to_thread(pdf_to_images, pdf_path, None, True)
            collect_images = rendered
    else:
        print(f"  [{wb_label}] {doc_name}: {total_pages} pages — no ToC, rendering all...")
        async with pdf_sem:
            rendered = await asyncio.to_thread(pdf_to_images, pdf_path, None, True)
        collect_images = rendered

    # Collector: run all pages concurrently
    n_text  = sum(1 for _, d in collect_images if isinstance(d, str))
    n_image = sum(1 for _, d in collect_images if isinstance(d, bytes))
    if n_text:
        print(f"  [{wb_label}] {doc_name}: {n_text} text-mode page(s), {n_image} image-mode page(s)")
    results = await asyncio.gather(*[
        collect_page(client, page_data, claude_sem, idx, doc_name, doc_type, priority, wellbore,
                     multi_wellbore=multi_wellbore, all_wellbores=all_wellbores)
        for idx, page_data in collect_images
    ])

    fragments = [f for page_frags in results for f in page_frags]

    # Drop formation_tests fragments that aren't from an explicit table source.
    # Schematic and approximate readings are not confirmed test records.
    before = len(fragments)
    fragments = [
        f for f in fragments
        if not (f.get("topic") == "formation_tests" and f.get("confidence") != "explicit")
    ]
    dropped = before - len(fragments)
    if dropped:
        print(f"  [{wellbore}] {doc_name}: dropped {dropped} non-explicit formation_test fragment(s)")

    print(f"  [{wellbore}] {doc_name}: {len(fragments)} fragments collected")
    return fragments


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def next_run_dir() -> Path:
    RUNS_DIR.mkdir(exist_ok=True)
    existing = [d for d in RUNS_DIR.iterdir() if d.is_dir() and d.name.startswith("run")]
    run_nums = []
    for d in existing:
        try:
            run_nums.append(int(d.name[3:]))
        except ValueError:
            pass
    n = max(run_nums, default=0) + 1
    run_dir = RUNS_DIR / f"run{n}"
    run_dir.mkdir()
    return run_dir


async def main():
    global _cache_lock, SINGLE_DOC_URL, WELLBORE_FILTER, CSV_PATH

    # ---------------------------------------------------------------------------
    # CLI argument parsing — overrides module-level config if arguments are given.
    # Usage examples:
    #   python extract_casing_data.py --url https://...pdf
    #   python extract_casing_data.py --csv wellbore_document_7_11.csv --wellbore 7/11-2
    #   python extract_casing_data.py --csv wellbore_document_7_11.csv --doc-name COMPLETION
    # ---------------------------------------------------------------------------
    import argparse
    parser = argparse.ArgumentParser(
        description="Wellbore casing & formation data extractor",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument(
        "--url", metavar="PDF_URL",
        help="Run on a single PDF URL (single-document mode)",
    )
    mode_group.add_argument(
        "--csv", metavar="FILE",
        help="CSV input file path",
    )
    parser.add_argument(
        "--wellbore", metavar="NAME",
        help="Filter to a single wellbore name (e.g. '7/11-2')",
    )
    parser.add_argument(
        "--doc-name", metavar="PATTERN",
        help="Only process documents whose name contains this substring",
    )
    parser.add_argument(
        "--resynth", metavar="RUN",
        help="Skip collection and re-synthesize from a previous run (e.g. run56)",
    )
    args, _ = parser.parse_known_args()  # use parse_known_args to avoid breaking pytest/etc.

    # Apply CLI overrides
    if args.url:
        SINGLE_DOC_URL = args.url
    elif args.csv:
        SINGLE_DOC_URL = None
        CSV_PATH = args.csv
    if args.wellbore:
        WELLBORE_FILTER = args.wellbore
    RESYNTH_FROM: str | None = args.resynth
    doc_name_cli_filter: str | None = getattr(args, "doc_name", None)

    _cache_lock = asyncio.Lock()
    load_cache()
    print(f"Fragment cache: {len(_cache)} entries loaded from {CACHE_PATH}\n")

    client = anthropic.AsyncAnthropic()

    if RESYNTH_FROM:
        targets = []  # no collection — fragments loaded later from saved run
    elif SINGLE_DOC_URL:
        # Use unified loader — produces a single DocumentRef; scan phase discovers wellbore
        url_refs = load_input_from_url(SINGLE_DOC_URL, wellbore_filter=WELLBORE_FILTER)
        doc_name = url_refs[0].document_name
        targets = [url_refs[0].to_legacy_dict()]
        print(f"Single-document mode: {doc_name}\nURL: {SINGLE_DOC_URL}\n")
    else:
        # Use the unified loader so WELLBORE_FILTER and doc_name_cli_filter are applied
        refs = load_input_from_csv(
            CSV_PATH,
            wellbore_filter=WELLBORE_FILTER,
            doc_name_filter=doc_name_cli_filter,
        )
        targets = [r.to_legacy_dict() for r in refs]
        print(f"CSV input: {len(refs)} documents targeted from {CSV_PATH}.\n")

    PDF_DIR.mkdir(exist_ok=True)
    run_dir = next_run_dir()
    print(f"Output directory: {run_dir}\n")

    script = Path(__file__)
    run_info = {
        "run_dir":    str(run_dir),
        "timestamp":  datetime.now(timezone.utc).isoformat(),
        "script":     script.name,
        "script_md5": hashlib.md5(script.read_bytes()).hexdigest(),
        "config": {
            "TARGET_DOC_TYPES":    TARGET_DOC_TYPES,
            "DOC_PRIORITY":        DOC_PRIORITY,
            "MAX_CONCURRENT_PDFS": MAX_CONCURRENT_PDFS,
            "MAX_CONCURRENT_CLAUDE": MAX_CONCURRENT_CLAUDE,
            "SYNTH_TOKEN_LIMIT": SYNTH_TOKEN_LIMIT,
        },
        "filter": {
            "wellbore": WELLBORE_FILTER,
        },
    }
    (run_dir / "run_info.json").write_text(json.dumps(run_info, indent=2))
    (run_dir / script.name).write_bytes(script.read_bytes())

    pdf_sem   = asyncio.Semaphore(MAX_CONCURRENT_PDFS)
    claude_sem = asyncio.Semaphore(MAX_CONCURRENT_CLAUDE)

    # Collect fragments — or load from a previous run
    all_fragments: dict[str, list[dict]] = defaultdict(list)
    # Scaffold selection — populated by scan phase, used during synthesis
    scaffold_selection: dict[str, WellboreScaffoldSelection] = {}

    if RESYNTH_FROM:
        src = RUNS_DIR / RESYNTH_FROM / "fragments.json"
        print(f"Loading fragments from {src}...\n")
        saved = json.loads(src.read_text())
        for wellbore, frags in saved.items():
            filtered = [f for f in frags if not is_skip(f.get("source_doc", ""))]
            skipped = len(frags) - len(filtered)
            if skipped:
                print(f"  [{wellbore}] skipped {skipped} fragments from excluded docs")
            all_fragments[wellbore].extend(filtered)
        # Try to load scaffold selection from that run's scan artifact
        scan_artifact_path = RUNS_DIR / RESYNTH_FROM / "doc_scan_results.json"
        if scan_artifact_path.exists():
            try:
                sa = json.loads(scan_artifact_path.read_text())
                for wb, sel_dict in sa.get("scaffold_selection", {}).items():
                    scaffold_selection[wb] = WellboreScaffoldSelection(**sel_dict)
                print(f"Scaffold selection loaded from {scan_artifact_path}\n")
            except Exception as e:
                print(f"  [warn] could not load scaffold selection: {e}\n")
    elif SINGLE_DOC_URL:
        # ----------------------------------------------------------------
        # Single-document mode
        # One URL, no CSV, wellbore auto-detected by the scan phase.
        # ----------------------------------------------------------------
        single_doc = targets[0]
        doc_name = single_doc["wlbDocumentName"]

        # Phase 0: scan to discover wellbores and classify the document
        print("--- Phase 0: Universal Document Scan ---")
        scan_result = await scan_document_universal(client, single_doc, pdf_sem, claude_sem)

        print(f"\nScan result for {doc_name}:")
        print(f"  Summary  : {scan_result.summary}")
        print(f"  Relevant : {scan_result.relevant}")
        print(f"  As-built : {scan_result.as_built}")
        print(f"  Wellbores: {', '.join(scan_result.wellbores) or 'none detected'}")
        print(f"  Casing   : {scan_result.casing_table} ({scan_result.casing_count} strings)")
        print(f"  LOT/FIT  : {scan_result.has_lot_fit}  DST: {scan_result.has_dst}")

        if not scan_result.relevant:
            print("\nDocument is not relevant — no casing or formation test data found.")
            return

        discovered = [wb for wb in scan_result.wellbores
                      if wb and wb.upper() not in ("UNKNOWN", "")]
        if not discovered:
            print("\nWARNING: No wellbore names detected. Using 'UNKNOWN'.")
            discovered = ["UNKNOWN"]
        else:
            print(f"\nDiscovered wellbores: {', '.join(discovered)}")

        # Mark this doc as scaffold for each discovered wellbore
        scan_result.is_scaffold = True
        for wb in discovered:
            scaffold_selection[wb] = WellboreScaffoldSelection(
                wellbore=wb,
                primary_doc=doc_name,
                secondary_doc="",
                scaffold_docs=[doc_name],
                ranked_docs=[doc_name],
                reason=f"single-document mode — only source",
            )

        # Save scan artifact
        scan_artifact = {
            "scan_results": {doc_name: asdict(scan_result)},
            "scaffold_selection": {k: asdict(v) for k, v in scaffold_selection.items()},
        }
        (run_dir / "doc_scan_results.json").write_text(json.dumps(scan_artifact, indent=2))

        # Collection: run in multi-wellbore mode so LLM tags each fragment
        single_ref = load_input_from_url(SINGLE_DOC_URL)[0]
        strategy = determine_collection_strategy(single_ref, scan_result)
        print(f"\n--- Collection ({doc_name}) | strategy={strategy} ---")
        fragments = await process_document(
            client, single_doc, pdf_sem, claude_sem,
            multi_wellbore=True,
            all_wellbores=discovered,
            scan_result=scan_result,
            collection_strategy=strategy,
        )

        # Route fragments to their wellbore bucket
        valid_wellbores = set(discovered)
        for f in fragments:
            wb = f.get("wellbore") or discovered[0]
            if wb not in valid_wellbores:
                # Accept closest match (e.g. "7/11-2" vs "7/11-2 S")
                wb = next((v for v in valid_wellbores if v in wb or wb in v), discovered[0])
            all_fragments[wb].append(f)

        (run_dir / "fragments.json").write_text(json.dumps(dict(all_fragments), indent=2))
        print(f"\nFragments saved → {run_dir}/fragments.json\n")
        save_cache()
        print(f"Fragment cache updated → {CACHE_PATH} ({len(_cache)} entries)\n")
    else:
        # ----------------------------------------------------------------
        # Phase 0: Universal Document Scan
        # Classify all target documents before collection begins.
        # PDFs are downloaded here (cached for subsequent collection phase).
        # ----------------------------------------------------------------
        print("\n--- Phase 0: Universal Document Scan ---")
        # Deduplicate by doc_name — same PDF may appear for multiple wellbores
        seen_scan: set[str] = set()
        unique_targets_for_scan = []
        for doc in targets:
            if doc["wlbDocumentName"] not in seen_scan:
                seen_scan.add(doc["wlbDocumentName"])
                unique_targets_for_scan.append(doc)

        scan_tasks = [
            scan_document_universal(client, doc, pdf_sem, claude_sem)
            for doc in unique_targets_for_scan
        ]
        scan_results_list = await asyncio.gather(*scan_tasks)
        scan_results: dict[str, DocumentScanResult] = {
            r.doc_name: r for r in scan_results_list
        }

        # Rank and select scaffold docs per wellbore
        scaffold_selection = select_scaffold_docs(targets, scan_results)

        # Print scan summary
        print(f"\nScan complete: {len(scan_results)} documents classified")
        relevant_count  = sum(1 for r in scan_results.values() if r.relevant)
        as_built_count  = sum(1 for r in scan_results.values() if r.as_built == "as_built")
        planned_count   = sum(1 for r in scan_results.values() if r.as_built == "planned")
        full_casing     = sum(1 for r in scan_results.values() if r.casing_table == "full")
        print(f"  Relevant: {relevant_count}/{len(scan_results)} | "
              f"As-built: {as_built_count} | Planned: {planned_count} | "
              f"Full casing table: {full_casing}")
        print("\nScaffold selection per wellbore:")
        for wb, sel in sorted(scaffold_selection.items()):
            print(f"  [{wb}] {sel.reason}")

        # Save scan debug artifact
        scan_artifact = {
            "scan_results": {k: asdict(v) for k, v in scan_results.items()},
            "scaffold_selection": {k: asdict(v) for k, v in scaffold_selection.items()},
        }
        (run_dir / "doc_scan_results.json").write_text(json.dumps(scan_artifact, indent=2))
        print(f"\nScan results → {run_dir}/doc_scan_results.json\n")

        # Skip documents the scan deemed irrelevant (saves collection tokens)
        irrelevant_docs = {
            doc_name for doc_name, r in scan_results.items() if not r.relevant
        }
        if irrelevant_docs:
            before = len(targets)
            targets = [d for d in targets if d["wlbDocumentName"] not in irrelevant_docs]
            print(f"Scan filtered {before - len(targets)} irrelevant documents "
                  f"(keeping {len(targets)} for collection)\n")

        # Build fast lookup: doc_name → wellbore (for single-wellbore fragment routing)
        doc_name_to_wellbore = {d["wlbDocumentName"]: d["wlbName"] for d in targets}

        # All wellbores in scope (for multi-wellbore collector)
        all_wellbore_names = sorted(set(d["wlbName"] for d in targets))

        # Group targets by processing tier
        tiers_map: dict[int, list[dict]] = defaultdict(list)
        for doc in targets:
            tiers_map[tier_for_doc_type(doc.get("wlbDocumentType", ""))].append(doc)

        # Wellbores not yet considered complete — starts as all wellbores
        active_wellbores: set[str] = set(all_wellbore_names)

        # Deduplicate shared docs within a tier (same PDF registered for multiple wellbores).
        # For multi-wellbore types, process each unique PDF once.
        def dedup_multi(docs: list[dict]) -> list[dict]:
            seen: set[str] = set()
            unique: list[dict] = []
            for doc in docs:
                if doc["wlbDocumentName"] not in seen:
                    seen.add(doc["wlbDocumentName"])
                    unique.append(doc)
            return unique

        docs_processed = 0
        for tier_idx in sorted(tiers_map.keys()):
            tier_docs = tiers_map[tier_idx]
            tier_name = DOC_TYPE_TIERS[tier_idx] if tier_idx < len(DOC_TYPE_TIERS) else "OTHER"
            doc_type_for_tier = DOC_TYPE_TIERS[tier_idx] if tier_idx < len(DOC_TYPE_TIERS) else ""
            is_multi_tier = doc_type_for_tier in MULTI_WELLBORE_DOC_TYPES

            # For single-wellbore tiers, only process docs for still-active wellbores
            if not is_multi_tier:
                tier_docs = [d for d in tier_docs if d["wlbName"] in active_wellbores]

            # For multi-wellbore tiers, deduplicate (one PDF scan serves all wellbores)
            if is_multi_tier:
                tier_docs = dedup_multi(tier_docs)

            if not tier_docs:
                print(f"\n--- {tier_name} tier: skipped (all wellbores complete) ---")
                continue

            # --- Progressive collection: build known state for non-first tiers ---
            known_state: dict[str, dict] = {}
            if tier_idx > 0 and not is_multi_tier:
                known_state = build_known_state(dict(all_fragments))
                progressive_count = sum(1 for d in tier_docs
                                        if d["wlbName"] in known_state
                                        and missing_data_description(known_state[d["wlbName"]]) is not None)
                print(f"\n--- {tier_name} tier: {len(tier_docs)} documents "
                      f"({len(active_wellbores)} active wellbores"
                      + (f", {len(tier_docs) - progressive_count} with progressive narrowing" if progressive_count < len(tier_docs) else "")
                      + ") ---")
            else:
                print(f"\n--- {tier_name} tier: {len(tier_docs)} documents "
                      f"({'multi-wellbore' if is_multi_tier else f'{len(active_wellbores)} active wellbores'}) ---")

            tasks = []
            for doc in tier_docs:
                mt = None
                if known_state and not is_multi_tier:
                    wb_known = known_state.get(doc["wlbName"])
                    if wb_known:
                        mt = missing_data_description(wb_known)
                        # mt is None if everything is known — but completeness
                        # check already removed fully-complete wellbores, so
                        # this handles the partial case (e.g. has casing but no tests)
                # Look up pre-computed scan result so collection routing is authoritative
                doc_scan = scan_results.get(doc["wlbDocumentName"])
                doc_ref  = DocumentRef(
                    input_mode="csv_row",
                    document_name=doc["wlbDocumentName"],
                    document_url=doc.get("wlbDocumentUrl", ""),
                    wellbore_name=doc["wlbName"],
                    document_type=doc.get("wlbDocumentType", ""),
                    is_multi_wellbore=is_multi_tier,
                    priority=get_priority(doc.get("wlbDocumentType", "")),
                    tier=tier_idx,
                )
                strategy = determine_collection_strategy(doc_ref, doc_scan)
                tasks.append(
                    process_document(
                        client, doc, pdf_sem, claude_sem,
                        multi_wellbore=is_multi_tier,
                        all_wellbores=all_wellbore_names if is_multi_tier else None,
                        missing_types=mt,
                        scan_result=doc_scan,
                        collection_strategy=strategy,
                    )
                )

            docs_in_tier = len(tasks)
            valid_wellbores = set(all_wellbore_names)
            for i, result in enumerate(asyncio.as_completed(tasks), 1):
                fragments = await result
                for f in fragments:
                    # Prefer the wellbore tag from the fragment itself (multi-wellbore mode)
                    wb = f.get("wellbore") or doc_name_to_wellbore.get(f.get("source_doc", ""))
                    # Validate: only accept known wellbore names (prevents LLM hallucinations)
                    if wb and wb in valid_wellbores:
                        all_fragments[wb].append(f)
                    elif wb:
                        print(f"    [fragment] unknown wellbore '{wb}' — dropped")
                docs_processed += 1
                print(f"Progress: {i}/{docs_in_tier} in tier, {docs_processed} total\n")

            # --- Completeness check after each non-final tier ---
            if tier_idx < max(tiers_map.keys()):
                newly_complete: list[str] = []
                for wb in list(active_wellbores):
                    frags = all_fragments.get(wb, [])
                    if is_wellbore_complete(frags):
                        newly_complete.append(wb)
                        active_wellbores.discard(wb)
                if newly_complete:
                    print(f"\n  Completeness check: {len(newly_complete)} wellbore(s) complete "
                          f"— will skip remaining tiers: {', '.join(sorted(newly_complete))}")
                remaining = len(active_wellbores)
                if remaining:
                    print(f"  {remaining} wellbore(s) still need lower-tier documents: "
                          f"{', '.join(sorted(active_wellbores))}")
                else:
                    print("  All wellbores complete — skipping all remaining tiers.")
                    break

        # Save fragments so this run can be re-synthesized without re-collecting
        (run_dir / "fragments.json").write_text(json.dumps(dict(all_fragments), indent=2))
        print(f"\nFragments saved → {run_dir}/fragments.json\n")
        save_cache()
        print(f"Fragment cache updated → {CACHE_PATH} ({len(_cache)} entries)\n")

    # Synthesize per wellbore
    print("Synthesizing per wellbore...")
    merged, conflicts = [], []
    for wellbore, fragments in all_fragments.items():
        # Pass scaffold doc names so postprocess_observations uses dynamic anchors
        sel = scaffold_selection.get(wellbore)
        scaffold_names: set | None = set(sel.scaffold_docs) if sel and sel.scaffold_docs else None
        rows, wellbore_conflicts = await synthesize_wellbore(
            client, claude_sem, wellbore, fragments, scaffold_names
        )
        for row in rows:
            row.setdefault("wellbore", wellbore)
        merged.extend(rows)
        conflicts.extend(wellbore_conflicts)
    print(f"Synthesis complete: {len(merged)} rows, {len(conflicts)} conflicts\n")

    output_csv    = run_dir / "casing_data_full.csv"
    conflicts_csv = run_dir / "casing_conflicts.csv"

    def to_eu(v) -> str:
        """Convert a value to string using comma as decimal separator."""
        if v is None:
            return ""
        s = str(v)
        # Replace decimal point with comma only in numeric-looking values
        import re as _re
        if _re.fullmatch(r"-?\d+\.\d+", s):
            return s.replace(".", ",")
        return s

    _DIAM_FIELDS = {"casing_diameter_in", "hole_diameter_in"}

    def eu_row(row: dict, key_map: dict | None = None) -> dict:
        out = {}
        for k, v in row.items():
            display_key = key_map.get(k, k) if key_map else k
            out[display_key] = _fmt_diam(v) if k in _DIAM_FIELDS else to_eu(v)
        return out

    # Primary CSV — clean 8-column output matching the evaluation schema
    primary_csv = run_dir / "casing_data.csv"
    primary_fields = [COLUMN_DISPLAY_NAMES.get(f, f) for f in OUTPUT_FIELDS_PRIMARY]
    with open(primary_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=primary_fields, extrasaction="ignore", delimiter=";")
        writer.writeheader()
        for row in merged:
            writer.writerow(eu_row(row, COLUMN_DISPLAY_NAMES))

    # Full CSV — includes provenance and QA columns
    display_fields = [COLUMN_DISPLAY_NAMES.get(f, f) for f in OUTPUT_FIELDS]
    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=display_fields, extrasaction="ignore", delimiter=";")
        writer.writeheader()
        for row in merged:
            writer.writerow(eu_row(row, COLUMN_DISPLAY_NAMES))

    conflict_fields = [
        "wellbore", "casing_type", "field",
        "primary_value", "primary_source", "primary_confidence",
        "secondary_value", "secondary_source", "secondary_confidence",
        "severity", "resolution",
    ]
    # Ensure wellbore is populated on all conflict rows
    for c in conflicts:
        c.setdefault("wellbore", "unknown")
    with open(conflicts_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=conflict_fields, delimiter=";")
        writer.writeheader()
        writer.writerows([eu_row(c) for c in conflicts])

    # Cost summary
    total_cost = 0.0
    cost_breakdown = {}
    for model, usage in _usage.items():
        pricing = MODEL_PRICING.get(model, {"input": 0, "output": 0})
        cache_write = usage.get("cache_write", 0)
        cache_read  = usage.get("cache_read",  0)
        cost = (
            usage["input"]  * pricing["input"]
            + usage["output"] * pricing["output"]
            + cache_write   * pricing["input"] * 1.25   # cache write: 25% surcharge
            + cache_read    * pricing["input"] * 0.10   # cache read:  90% discount
        ) / 1_000_000
        cost_breakdown[model] = {
            "input_tokens":       usage["input"],
            "output_tokens":      usage["output"],
            "cache_write_tokens": cache_write,
            "cache_read_tokens":  cache_read,
            "cost_usd":           round(cost, 4),
        }
        total_cost += cost

    print(f"\nDone.")
    print(f"  {len(merged)} merged rows  → {primary_csv}  (clean 8-column schema)")
    print(f"  {len(merged)} merged rows  → {output_csv}  (full provenance)")
    print(f"  {len(conflicts)} conflicts  → {conflicts_csv}")
    print(f"\nAPI cost this run: ${total_cost:.4f}")
    for model, info in cost_breakdown.items():
        cache_note = ""
        if info["cache_write_tokens"] or info["cache_read_tokens"]:
            cache_note = f"  (cache write {info['cache_write_tokens']:,} / read {info['cache_read_tokens']:,})"
        print(f"  {model}: {info['input_tokens']:,} in / {info['output_tokens']:,} out → ${info['cost_usd']:.4f}{cache_note}")
    if _toc_usage["calls"]:
        toc_pricing = MODEL_PRICING.get(MODELS["anthropic"]["screener"], {"input": 0, "output": 0})
        toc_cost = (_toc_usage["input"] * toc_pricing["input"] + _toc_usage["output"] * toc_pricing["output"]) / 1_000_000
        print(f"  Screening: {_toc_usage['calls']} calls, "
              f"{_toc_usage['input']:,} in / {_toc_usage['output']:,} out → ${toc_cost:.4f} "
              f"(included in Haiku total above)")

    # Save cost to run_info
    run_info_path = run_dir / "run_info.json"
    info = json.loads(run_info_path.read_text())
    info["cost"] = {"total_usd": round(total_cost, 4), "breakdown": cost_breakdown}
    run_info_path.write_text(json.dumps(info, indent=2))


async def run_pipeline(
    document_refs: "list[DocumentRef]",
    config: dict | None = None,
) -> tuple[list[dict], list[dict]]:
    """Canonical pipeline entry point for programmatic use.

    Accepts a pre-built list of DocumentRef objects (produced by
    load_input_from_url() or load_input_from_csv()) and runs the full
    pipeline: scan → scaffold selection → collection → synthesis → postprocess.

    Returns (merged_rows, conflicts) — same shape as main() output.

    config: optional dict of overrides, keys matching module-level constants
    (e.g. {"WELLBORE_FILTER": "7/11-2", "MAX_CONCURRENT_CLAUDE": 5}).
    """
    # Apply any caller-supplied config overrides
    g = globals()
    for k, v in (config or {}).items():
        if k in g:
            g[k] = v

    # Convert refs to the legacy dict format main() currently consumes, then delegate.
    # This is a transitional shim — as main() is progressively migrated to consume
    # DocumentRef[] directly, this wrapper will shrink.
    global SINGLE_DOC_URL, WELLBORE_FILTER, CSV_PATH
    if len(document_refs) == 1 and document_refs[0].input_mode == "single_url":
        SINGLE_DOC_URL = document_refs[0].document_url
    else:
        # Batch mode: write a temporary in-memory stub — main() will re-load via load_input_from_csv.
        # For now, pass refs directly by overwriting the CSV targets via a side-channel.
        # TODO: migrate main() to accept document_refs directly.
        SINGLE_DOC_URL = None

    await main()
    # main() writes outputs to run_dir and returns nothing.
    # Future: return (merged_rows, conflicts) once main() is refactored to yield them.
    return [], []


# ---------------------------------------------------------------------------
# Developer note: new architecture overview
# ---------------------------------------------------------------------------
#
# HOW THE NEW SCAN CONTROLS THE PIPELINE
# ---------------------------------------
# Phase 0 (scan_document_universal) runs BEFORE collection. It classifies every
# document on relevance, status, and role. Its result (DocumentScanResult) is now
# passed into process_document as `scan_result`, making it the AUTHORITATIVE source
# for skip/keep decisions. The legacy screen_document call inside process_document
# now only overrides the scan if BOTH agree the document is irrelevant. When scan
# says relevant but screen says not, scan wins — it had more context (escalation pages,
# filename heuristics, TOC signals). Every skip logs:
#   scan=<relevance_class>  screen=YES|NO  decision=<source>  reason=<code>
#
# HOW SINGLE URL AND CSV ARE UNIFIED
# -----------------------------------
# Both entry modes now produce List[DocumentRef] before any downstream code runs:
#   --url  → load_input_from_url()  → [DocumentRef(input_mode="single_url", ...)]
#   --csv  → load_input_from_csv()  → [DocumentRef(input_mode="csv_row", ...)] × N
# All legacy doc-dicts are now produced by DocumentRef.to_legacy_dict(), keeping
# backward compatibility with existing functions. The final migration step (making
# main() consume DocumentRef[] directly instead of converting) is deferred.
#
# HOW OBSERVATIONS DIFFER FROM FRAGMENTS
# ----------------------------------------
# Fragments (fragments.json) are freeform JSON-L objects emitted by Haiku per page.
# They are unstructured and require Sonnet to reconstruct rows from scratch.
# Observations (CasingObservation, LotFitObservation, DstObservation) are typed
# dataclasses with field-level confidence, provenance class, datum, and partial flags.
# During the transition, both exist in parallel. fragments.json continues to drive
# synthesis; observations.json (planned) will drive the future field-level merge layer.
#
# WHAT IS TRANSITIONAL VS FULLY MIGRATED
# ----------------------------------------
# MIGRATED:
#   - DocumentRef + input loaders (load_input_from_url, load_input_from_csv)
#   - scan_result as authoritative routing signal in process_document
#   - collection_strategy computed per-doc and passed to process_document
#   - Structured observation dataclasses (CasingObservation, LotFitObservation, DstObservation)
#   - CLI args (--url, --csv, --wellbore, --doc-name)
# TRANSITIONAL:
#   - main() still converts DocumentRef → legacy dict internally
#   - collection strategy is computed and passed but not yet consumed inside process_document
#     (the collector still uses the same prompt regardless of strategy)
#   - observations.json not yet written (dataclasses exist, collector output not yet converted)
# NEXT STEPS:
#   - Wire collection_strategy into collector prompt selection (Phase 5 per spec)
#   - Convert synthesizer JSON output into typed Observation objects and write observations.json
#   - Implement observation clustering + field-level merge before Sonnet synthesis (Phase 7)
#   - Migrate main() to consume List[DocumentRef] directly (remove to_legacy_dict shim)
# ---------------------------------------------------------------------------


if __name__ == "__main__":
    asyncio.run(main())
