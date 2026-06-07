"""
NASA APOD ETL Pipeline — Week 3
Myleah Jones
================================
MSBA 692: Pipeline to Insights

Full ETL pipeline that extracts data from the NASA Astronomy Picture of the
Day (APOD) REST API, applies transformation and data quality operations, runs
validation checks, loads into Supabase PostgreSQL using an incremental
strategy, and exports analytics-ready CSV files for Plotly Dash.

Stages:
    1. Extraction   — NASA APOD API with pagination, auth, and error handling
    2. Transformation & Cleaning — normalize, clean, and derive metrics
    3. Validation   — null checks, duplicates, schema, range, referential integrity
    4. Incremental Load — append-only new records; no full reload each run
    5. Analytics Export — CSV snapshots ready for Plotly Dash
    6. Logging & Error Handling — throughout all stages

Required packages:
    pip install pandas sqlalchemy psycopg2-binary python-dotenv requests

.env values expected:
    DB_PASSWORD=your_supabase_database_password
    DB_REF=your_supabase_project_ref
    NASA_API_KEY=your_nasa_api_key
    SUPABASE_DB_URL=postgresql+psycopg2://...  (optional override)

Optional flags:
    RESET_TABLES=false   -- set to true to drop and recreate tables (full load)
    START_DATE=YYYY-MM-DD
    END_DATE=YYYY-MM-DD
"""

from __future__ import annotations

import logging
import os
import time
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.types import Date, Integer, Text


# ── Logging configuration ──────────────────────────────────────────────────────
# Set up logging to both the console and a persistent log file so every
# pipeline run is recorded for auditing and debugging.

LOG_DIR = Path(__file__).resolve().parent.parent / "logs"
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_DIR / "etl_pipeline.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)


# ── Constants ──────────────────────────────────────────────────────────────────

BASE_DIR    = Path(__file__).resolve().parent.parent
EXPORT_DIR  = BASE_DIR / "data" / "analytics"
EXPORT_DIR.mkdir(parents=True, exist_ok=True)

APOD_URL    = "https://api.nasa.gov/planetary/apod"
BATCH_SIZE  = 30       # records per API request
REQUEST_DELAY = 2      # seconds between batch requests

# Seed data for media_type lookup table
MEDIA_TYPES = [
    {"media_type_id": 1, "media_type_name": "image"},
    {"media_type_id": 2, "media_type_name": "video"},
]

# Validation thresholds
MAX_NULL_PCT      = 0.05   # allow up to 5% nulls in non-nullable fields
MIN_WORD_COUNT    = 1      # explanation must have at least 1 word
MAX_WORD_COUNT    = 2000   # sanity cap — no explanation should exceed this
MIN_CHAR_COUNT    = 10     # explanation must be at least 10 characters


# ── Environment & Connection ───────────────────────────────────────────────────

def get_database_url() -> str:
    """
    Build the Supabase PostgreSQL connection URL from .env file.
    SUPABASE_DB_URL overrides the DB_PASSWORD + DB_REF combination.
    """
    load_dotenv()

    direct_url = os.getenv("SUPABASE_DB_URL")
    if direct_url:
        return direct_url

    password = os.getenv("DB_PASSWORD")
    db_ref   = os.getenv("DB_REF")

    if not password or not db_ref:
        raise RuntimeError(
            "Missing credentials. Set SUPABASE_DB_URL, "
            "or set both DB_PASSWORD and DB_REF in your .env file."
        )

    return (
        "postgresql+psycopg2://"
        f"postgres:{password}"
        f"@db.{db_ref}.supabase.co:5432/postgres"
    )


def table_reset_enabled() -> bool:
    """
    Check RESET_TABLES flag. Defaults to False for Week 3 — we use
    incremental loading by default and only do a full reset when explicitly
    requested. Set RESET_TABLES=true in .env to force a clean reload.
    """
    return os.getenv("RESET_TABLES", "false").strip().lower() in {"1", "true", "yes", "y"}


# ── Schema Management ──────────────────────────────────────────────────────────

def create_schema(engine) -> None:
    """
    Create the database schema if it does not already exist.
    Uses CREATE TABLE IF NOT EXISTS so re-running is safe.
    Only drops tables when RESET_TABLES=true (full load mode).

    Tables:
        media_type  — lookup: image | video
        apod_entry  — one row per APOD entry
    """
    log.info("Initializing database schema...")

    drop_sql = """
    DROP TABLE IF EXISTS public.apod_entry CASCADE;
    DROP TABLE IF EXISTS public.media_type CASCADE;
    """

    create_sql = """
    CREATE TABLE IF NOT EXISTS public.media_type (
        media_type_id   INTEGER      PRIMARY KEY,
        media_type_name VARCHAR(10)  NOT NULL UNIQUE
    );

    CREATE TABLE IF NOT EXISTS public.apod_entry (
        entry_id        BIGSERIAL    PRIMARY KEY,
        entry_date      DATE         NOT NULL UNIQUE,
        media_type_id   INTEGER      NOT NULL
                        REFERENCES public.media_type(media_type_id),
        title           TEXT         NOT NULL,
        explanation     TEXT         NOT NULL,
        url             TEXT         NOT NULL,
        hdurl           TEXT,
        copyright       TEXT,
        word_count      INTEGER      NOT NULL DEFAULT 0,
        char_count      INTEGER      NOT NULL DEFAULT 0
    );

    CREATE INDEX IF NOT EXISTS idx_apod_entry_media_type
        ON public.apod_entry(media_type_id);
    """

    with engine.begin() as conn:
        if table_reset_enabled():
            log.warning("RESET_TABLES=true — dropping and recreating all tables (full load).")
            conn.execute(text(drop_sql))
        conn.execute(text(create_sql))

    log.info("Schema ready.")


# ── Stage 1: Extraction ────────────────────────────────────────────────────────

def build_date_ranges(start: date, end: date, batch_days: int = BATCH_SIZE) -> list[tuple[date, date]]:
    """
    Split the full date range into smaller batches to stay within NASA API
    rate limits (DEMO_KEY: 30 req/hour; registered key: 1,000 req/day).
    Returns a list of (batch_start, batch_end) tuples.
    """
    ranges = []
    current = start
    while current <= end:
        batch_end = min(current + timedelta(days=batch_days - 1), end)
        ranges.append((current, batch_end))
        current = batch_end + timedelta(days=1)
    return ranges


def fetch_apod_batch(api_key: str, start: date, end: date, retries: int = 3) -> list[dict]:
    """
    Call the NASA APOD API for one date-range batch.
    Implements retry logic with exponential backoff for transient errors (5xx).
    Returns a list of entry dicts, or an empty list on persistent failure.
    """
    params = {
        "api_key":    api_key,
        "start_date": start.isoformat(),
        "end_date":   end.isoformat(),
    }

    for attempt in range(1, retries + 1):
        try:
            response = requests.get(APOD_URL, params=params, timeout=15)

            # Validate HTTP response — raise immediately for 4xx errors,
            # retry for 5xx server errors
            if response.status_code >= 500:
                raise requests.exceptions.HTTPError(
                    f"{response.status_code} Server Error", response=response
                )
            response.raise_for_status()

            entries = response.json()
            log.info(f"  Fetched {len(entries)} entries: {start} → {end}")
            return entries

        except requests.exceptions.HTTPError as e:
            if attempt < retries:
                wait = 2 ** attempt
                log.warning(f"  HTTP error attempt {attempt}/{retries} for {start}→{end}: {e}. Retrying in {wait}s...")
                time.sleep(wait)
            else:
                log.error(f"  Failed after {retries} attempts for {start}→{end}: {e}")
                return []

        except requests.exceptions.RequestException as e:
            log.error(f"  Request failed for {start}→{end}: {e}")
            return []

    return []


def get_existing_dates(engine) -> set[date]:
    """
    Query the database for all entry_date values already loaded.
    Used by the incremental load strategy to skip already-loaded records.
    Returns an empty set if the table does not exist yet.
    """
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT entry_date FROM public.apod_entry"))
            return {row[0] for row in result}
    except Exception:
        return set()


def extract_apod_data(engine) -> pd.DataFrame:
    """
    Pull APOD entries from the NASA API for the configured date range.
    Implements incremental loading: only fetches dates not already in the DB.

    Incremental loading strategy:
        1. Query the DB for all dates already loaded (get_existing_dates)
        2. For each API batch, skip entries whose dates are already present
        3. Only new records are returned for transformation and loading
        4. On RESET_TABLES=true (full load), all existing dates are ignored
           and the full range is re-fetched

    This approach prevents duplicate loads and keeps the pipeline efficient
    for recurring runs — each execution only processes net-new data.
    """
    load_dotenv()

    api_key    = os.getenv("NASA_API_KEY", "DEMO_KEY")
    start_date = date.fromisoformat(os.getenv("START_DATE", "2020-01-01"))
    end_date   = date.fromisoformat(os.getenv("END_DATE", date.today().isoformat()))

    log.info("=" * 55)
    log.info("STAGE 1: EXTRACTION")
    log.info(f"  Date range : {start_date} → {end_date}")
    log.info(f"  API key    : {'DEMO_KEY (limited)' if api_key == 'DEMO_KEY' else 'registered key'}")

    # Incremental: get dates already in DB (empty set on full load / first run)
    if table_reset_enabled():
        existing_dates: set[date] = set()
        log.info("  Mode: FULL LOAD — ignoring existing records")
    else:
        existing_dates = get_existing_dates(engine)
        log.info(f"  Mode: INCREMENTAL — {len(existing_dates)} dates already loaded, skipping those")

    date_ranges  = build_date_ranges(start_date, end_date)
    all_entries: list[dict] = []

    for i, (batch_start, batch_end) in enumerate(date_ranges):
        batch = fetch_apod_batch(api_key, batch_start, batch_end)

        # Filter out already-loaded dates (incremental strategy)
        new_entries = [
            e for e in batch
            if date.fromisoformat(e["date"]) not in existing_dates
        ]

        if len(new_entries) < len(batch):
            skipped = len(batch) - len(new_entries)
            log.info(f"    Skipped {skipped} already-loaded entries in this batch")

        all_entries.extend(new_entries)

        if i < len(date_ranges) - 1:
            time.sleep(REQUEST_DELAY)

    log.info(f"Extraction complete: {len(all_entries)} new entries to process.")
    return pd.DataFrame(all_entries) if all_entries else pd.DataFrame()


# ── Stage 2: Transformation & Cleaning ────────────────────────────────────────

def build_media_type_df() -> pd.DataFrame:
    """Return the seed DataFrame for the media_type lookup table."""
    return pd.DataFrame(MEDIA_TYPES)


def build_apod_entry_df(raw_df: pd.DataFrame, media_type_df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and transform the raw API response DataFrame into a shape that
    matches the apod_entry table schema.

    Transformation steps:
        1. Drop API fields not stored in schema
        2. Rename 'date' → 'entry_date'
        3. Cast entry_date to Python date object
        4. Strip whitespace from all text fields
        5. Fill missing nullable fields (hdurl, copyright) with None → SQL NULL
        6. Handle unexpected media_type values — default to 'image' (ID 1)
        7. Map media_type string → integer ID via lookup join
        8. Compute derived metrics: word_count, char_count
        9. Deduplicate by entry_date (keep first)
       10. Select and order final columns
    """
    log.info("=" * 55)
    log.info("STAGE 2: TRANSFORMATION & CLEANING")

    df = raw_df.copy()
    initial_rows = len(df)
    log.info(f"  Input rows: {initial_rows}")

    # 1. Drop API-only fields not stored in the schema
    drop_cols = [c for c in ["service_version", "concepts", "thumbnail_url"] if c in df.columns]
    if drop_cols:
        df = df.drop(columns=drop_cols)
        log.info(f"  Dropped non-schema API columns: {drop_cols}")

    # 2. Rename date → entry_date
    df = df.rename(columns={"date": "entry_date"})

    # 3. Normalize entry_date to Python date object
    df["entry_date"] = pd.to_datetime(df["entry_date"]).dt.date

    # 4. Strip whitespace from all text fields
    for col in ["title", "explanation", "url", "media_type"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()

    # 5. Fill missing nullable fields with None (maps to SQL NULL)
    for col in ["hdurl", "copyright"]:
        if col not in df.columns:
            df[col] = None
        else:
            df[col] = df[col].where(df[col].notna() & (df[col] != "nan"), other=None)

    # 6. Handle unexpected media_type values
    known_types = set(media_type_df["media_type_name"])
    unexpected  = df[~df["media_type"].isin(known_types)]["media_type"].unique()
    if len(unexpected) > 0:
        log.warning(f"  Unknown media_type values found: {unexpected} — defaulting to 'image'")
        df["media_type"] = df["media_type"].apply(
            lambda x: x if x in known_types else "image"
        )

    # 7. Map media_type string → integer ID
    media_map = dict(zip(media_type_df["media_type_name"], media_type_df["media_type_id"]))
    df["media_type_id"] = df["media_type"].map(media_map).fillna(1).astype("Int64")

    # 8. Compute derived metrics from explanation text
    df["word_count"] = df["explanation"].str.split().str.len().fillna(0).astype(int)
    df["char_count"] = df["explanation"].str.strip().str.len().fillna(0).astype(int)

    # 9. Deduplicate by entry_date
    dupes_before = df.duplicated(subset=["entry_date"]).sum()
    if dupes_before > 0:
        log.warning(f"  Removed {dupes_before} duplicate entry_date rows")
        df = df.drop_duplicates(subset=["entry_date"], keep="first")

    # 10. Select final columns in schema order
    final_cols = [
        "entry_date", "media_type_id", "title", "explanation",
        "url", "hdurl", "copyright", "word_count", "char_count"
    ]
    df = df[final_cols].reset_index(drop=True)

    log.info(f"  Output rows after cleaning: {len(df)}")
    log.info(f"  Rows removed during cleaning: {initial_rows - len(df)}")
    log.info("Transformation complete.")
    return df


# ── Stage 3: Validation & Quality Checks ──────────────────────────────────────

def run_validation(df: pd.DataFrame, media_type_df: pd.DataFrame) -> bool:
    """
    Run a suite of data quality checks on the transformed DataFrame before
    loading to the database. Logs PASS/FAIL for each check.

    Checks:
        1.  Row count — must have at least one record
        2.  Required columns — all schema columns must be present
        3.  Null check — entry_date, media_type_id, title, url must not be null
        4.  Duplicate detection — entry_date must be unique
        5.  Media type referential integrity — all IDs must exist in lookup
        6.  word_count range — must be within MIN_WORD_COUNT to MAX_WORD_COUNT
        7.  char_count range — must be >= MIN_CHAR_COUNT
        8.  entry_date format — all dates must be valid date objects
        9.  URL format — url must start with http
        10. word_count / char_count consistency — word_count > 0 iff char_count > 0

    Returns True if all checks pass, False if any critical check fails.
    """
    log.info("=" * 55)
    log.info("STAGE 3: VALIDATION & QUALITY CHECKS")

    all_passed   = True
    check_number = 0

    def check(name: str, passed: bool, detail: str = "") -> None:
        nonlocal all_passed, check_number
        check_number += 1
        status = "PASS" if passed else "FAIL"
        msg    = f"  [{status}] Check {check_number:02d}: {name}"
        if detail:
            msg += f" — {detail}"
        if passed:
            log.info(msg)
        else:
            log.error(msg)
            all_passed = False

    # 1. Row count
    check("Row count > 0", len(df) > 0, f"{len(df)} rows")

    if len(df) == 0:
        log.error("No records to validate. Aborting validation.")
        return False

    # 2. Required columns present
    required_cols = ["entry_date", "media_type_id", "title", "explanation",
                     "url", "hdurl", "copyright", "word_count", "char_count"]
    missing_cols  = [c for c in required_cols if c not in df.columns]
    check("Required columns present", len(missing_cols) == 0,
          f"Missing: {missing_cols}" if missing_cols else "All present")

    # 3. Null checks on non-nullable columns
    non_nullable = ["entry_date", "media_type_id", "title", "url"]
    for col in non_nullable:
        null_count = df[col].isnull().sum()
        null_pct   = null_count / len(df)
        check(f"Null check: {col}",
              null_pct <= MAX_NULL_PCT,
              f"{null_count} nulls ({null_pct:.1%})")

    # 4. Duplicate detection on entry_date
    dupe_count = df.duplicated(subset=["entry_date"]).sum()
    check("No duplicate entry_date", dupe_count == 0,
          f"{dupe_count} duplicates found" if dupe_count > 0 else "All unique")

    # 5. Referential integrity — media_type_id must exist in lookup
    valid_ids     = set(media_type_df["media_type_id"])
    invalid_ids   = df[~df["media_type_id"].isin(valid_ids)]["media_type_id"].unique()
    check("Referential integrity: media_type_id",
          len(invalid_ids) == 0,
          f"Invalid IDs: {invalid_ids}" if len(invalid_ids) > 0 else "All valid")

    # 6. word_count range
    out_of_range = df[
        (df["word_count"] < MIN_WORD_COUNT) | (df["word_count"] > MAX_WORD_COUNT)
    ]
    check("word_count range",
          len(out_of_range) == 0,
          f"{len(out_of_range)} rows outside [{MIN_WORD_COUNT}, {MAX_WORD_COUNT}]")

    # 7. char_count minimum
    low_char = df[df["char_count"] < MIN_CHAR_COUNT]
    check("char_count minimum",
          len(low_char) == 0,
          f"{len(low_char)} rows below {MIN_CHAR_COUNT} chars")

    # 8. entry_date type validation
    invalid_dates = df[df["entry_date"].apply(lambda x: not isinstance(x, date))]
    check("entry_date type valid",
          len(invalid_dates) == 0,
          f"{len(invalid_dates)} invalid date values")

    # 9. URL format check
    invalid_urls = df[~df["url"].str.startswith("http", na=False)]
    check("URL format valid",
          len(invalid_urls) == 0,
          f"{len(invalid_urls)} URLs don't start with http")

    # 10. word_count / char_count consistency
    inconsistent = df[
        ((df["word_count"] > 0) & (df["char_count"] == 0)) |
        ((df["word_count"] == 0) & (df["char_count"] > 0))
    ]
    check("word_count / char_count consistency",
          len(inconsistent) == 0,
          f"{len(inconsistent)} inconsistent rows")

    log.info(f"Validation complete: {check_number} checks run.")
    if all_passed:
        log.info("All validation checks PASSED. Proceeding to load.")
    else:
        log.error("One or more validation checks FAILED. Review errors above before loading.")

    return all_passed


# ── Stage 4: Database Loading ──────────────────────────────────────────────────

def write_table(df: pd.DataFrame, table_name: str, engine, dtype: dict) -> None:
    """
    Write a DataFrame to the named PostgreSQL table using SQLAlchemy.
    Appends to existing data (schema created separately).
    Loads in chunks of 500 for reliability with large payloads.
    """
    log.info(f"  Loading {table_name} ({len(df)} rows)...")
    try:
        df.to_sql(
            table_name,
            engine,
            schema="public",
            if_exists="append",
            index=False,
            method="multi",
            chunksize=500,
            dtype=dtype,
        )
        log.info(f"  {table_name}: {len(df)} rows loaded successfully.")
    except Exception as e:
        log.error(f"  Failed to load {table_name}: {e}")
        raise


def load_tables(engine, media_type_df: pd.DataFrame, apod_entry_df: pd.DataFrame) -> None:
    """
    Load all transformed DataFrames into the database.
    media_type must load first — apod_entry has a foreign key on it.
    On incremental runs, media_type seed data is only loaded if the table
    is empty (avoids unique constraint violations on re-runs).
    """
    log.info("=" * 55)
    log.info("STAGE 4: DATABASE LOADING")

    # Only seed media_type if it's empty (safe for incremental runs)
    try:
        with engine.connect() as conn:
            count = conn.execute(text("SELECT COUNT(*) FROM public.media_type")).scalar()
        if count == 0:
            write_table(
                media_type_df, "media_type", engine,
                {"media_type_id": Integer(), "media_type_name": Text()}
            )
        else:
            log.info(f"  media_type already seeded ({count} rows) — skipping.")
    except Exception:
        # Table may not exist yet on very first run; write it
        write_table(
            media_type_df, "media_type", engine,
            {"media_type_id": Integer(), "media_type_name": Text()}
        )

    # Load apod_entry — incremental, only new records
    if len(apod_entry_df) > 0:
        write_table(
            apod_entry_df, "apod_entry", engine,
            {
                "entry_date":    Date(),
                "media_type_id": Integer(),
                "title":         Text(),
                "explanation":   Text(),
                "url":           Text(),
                "hdurl":         Text(),
                "copyright":     Text(),
                "word_count":    Integer(),
                "char_count":    Integer(),
            }
        )
    else:
        log.info("  No new records to load into apod_entry.")

    log.info("Database loading complete.")


# ── Stage 5: Analytics Export ──────────────────────────────────────────────────

def export_analytics_datasets(engine) -> None:
    """
    Read the full apod_entry table from the database and export
    analytics-ready CSV files for use with Plotly Dash.

    Exports:
        apod_full.csv         — all entries with media type name joined
        apod_by_year.csv      — entry count and avg word count per year
        apod_media_by_year.csv — image vs video counts per year
        apod_top_words.csv    — top 50 most frequent words in titles
    """
    log.info("=" * 55)
    log.info("STAGE 5: ANALYTICS EXPORT")

    try:
        # Load full joined dataset
        query = """
            SELECT
                ae.entry_id,
                ae.entry_date,
                mt.media_type_name,
                ae.title,
                ae.explanation,
                ae.url,
                ae.hdurl,
                ae.copyright,
                ae.word_count,
                ae.char_count,
                EXTRACT(YEAR FROM ae.entry_date)::INTEGER  AS entry_year,
                EXTRACT(MONTH FROM ae.entry_date)::INTEGER AS entry_month
            FROM public.apod_entry ae
            JOIN public.media_type mt ON ae.media_type_id = mt.media_type_id
            ORDER BY ae.entry_date
        """
        df = pd.read_sql(query, engine)
        log.info(f"  Loaded {len(df)} rows from database for export.")

        # ── Export 1: Full dataset ─────────────────────────────────────────
        full_path = EXPORT_DIR / "apod_full.csv"
        df.to_csv(full_path, index=False)
        log.info(f"  Exported: {full_path.name} ({len(df)} rows)")

        # ── Export 2: Entries per year with avg word count ─────────────────
        by_year = (
            df.groupby("entry_year")
            .agg(
                entry_count=("entry_id", "count"),
                avg_word_count=("word_count", "mean"),
                avg_char_count=("char_count", "mean"),
            )
            .round(1)
            .reset_index()
        )
        by_year_path = EXPORT_DIR / "apod_by_year.csv"
        by_year.to_csv(by_year_path, index=False)
        log.info(f"  Exported: {by_year_path.name} ({len(by_year)} rows)")

        # ── Export 3: Image vs video counts per year ───────────────────────
        media_by_year = (
            df.groupby(["entry_year", "media_type_name"])
            .size()
            .reset_index(name="count")
        )
        media_path = EXPORT_DIR / "apod_media_by_year.csv"
        media_by_year.to_csv(media_path, index=False)
        log.info(f"  Exported: {media_path.name} ({len(media_by_year)} rows)")

        # ── Export 4: Top 50 words in APOD titles (stopwords removed) ─────
        stopwords = {
            "a", "an", "the", "and", "or", "of", "in", "to", "is", "it",
            "its", "on", "at", "for", "with", "as", "by", "from", "this",
            "that", "be", "was", "are", "were", "has", "have", "had", "not",
            "but", "so", "do", "if", "than", "then", "into", "over", "our",
            "your", "we", "they", "he", "she", "i", "you", "s", "de",
        }
        all_words = (
            df["title"]
            .str.lower()
            .str.replace(r"[^a-z\s]", "", regex=True)
            .str.split()
            .explode()
            .dropna()
        )
        top_words = (
            all_words[~all_words.isin(stopwords)]
            .value_counts()
            .head(50)
            .reset_index()
        )
        top_words.columns = ["word", "count"]
        words_path = EXPORT_DIR / "apod_top_words.csv"
        top_words.to_csv(words_path, index=False)
        log.info(f"  Exported: {words_path.name} (top {len(top_words)} words)")

        log.info("Analytics export complete.")

    except Exception as e:
        log.error(f"Analytics export failed: {e}")
        raise


# ── Row count verification ─────────────────────────────────────────────────────

def verify_row_counts(engine, expected_new: int) -> None:
    """
    After loading, query the database to verify row counts are as expected.
    Logs the total rows in each table as a final quality check.
    """
    log.info("=" * 55)
    log.info("POST-LOAD VERIFICATION")

    try:
        with engine.connect() as conn:
            total_entries = conn.execute(
                text("SELECT COUNT(*) FROM public.apod_entry")
            ).scalar()
            total_media_types = conn.execute(
                text("SELECT COUNT(*) FROM public.media_type")
            ).scalar()

        log.info(f"  media_type rows  : {total_media_types}")
        log.info(f"  apod_entry rows  : {total_entries}")
        log.info(f"  New rows added   : {expected_new}")

        if expected_new > 0 and total_entries >= expected_new:
            log.info("  Row count verification PASSED.")
        elif expected_new == 0:
            log.info("  No new rows expected — database is up to date.")
        else:
            log.warning("  Row count may be lower than expected — review load logs.")

    except Exception as e:
        log.error(f"Row count verification failed: {e}")


# ── Main Orchestration ─────────────────────────────────────────────────────────

def main() -> None:
    """
    Full ETL pipeline orchestration for MSBA 692 Week 3.

    Execution order:
        1. Connect to Supabase PostgreSQL
        2. Create schema (incremental-safe)
        3. Extract new entries from NASA APOD API
        4. Transform and clean raw data
        5. Validate quality before loading
        6. Load new records into database (incremental)
        7. Export analytics-ready CSVs for Plotly Dash
        8. Verify final row counts
    """
    log.info("=" * 55)
    log.info("NASA APOD ETL PIPELINE — MSBA 692 Week 3")
    log.info("=" * 55)

    # ── Connect ────────────────────────────────────────────────────────────
    log.info("Connecting to Supabase PostgreSQL...")
    try:
        engine = create_engine(get_database_url())
        # Test connection immediately
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        log.info("Database connection established.")
    except Exception as e:
        log.critical(f"Cannot connect to database: {e}")
        return

    # ── Schema ────────────────────────────────────────────────────────────
    try:
        create_schema(engine)
    except Exception as e:
        log.critical(f"Schema creation failed: {e}")
        return

    # ── Extract ───────────────────────────────────────────────────────────
    try:
        raw_df = extract_apod_data(engine)
    except Exception as e:
        log.critical(f"Extraction failed: {e}")
        return

    if raw_df.empty:
        log.info("No new records to process. Database is already up to date.")
        # Still export analytics CSVs from existing data
        try:
            export_analytics_datasets(engine)
        except Exception as e:
            log.error(f"Analytics export failed: {e}")
        log.info("Pipeline complete — no new data.")
        return

    # ── Transform ─────────────────────────────────────────────────────────
    try:
        media_type_df = build_media_type_df()
        apod_entry_df = build_apod_entry_df(raw_df, media_type_df)
    except Exception as e:
        log.critical(f"Transformation failed: {e}")
        return

    # ── Validate ──────────────────────────────────────────────────────────
    try:
        validation_passed = run_validation(apod_entry_df, media_type_df)
    except Exception as e:
        log.critical(f"Validation stage error: {e}")
        return

    if not validation_passed:
        log.critical("Validation failed. Aborting load to prevent bad data entering the database.")
        return

    # ── Load ──────────────────────────────────────────────────────────────
    new_record_count = len(apod_entry_df)
    try:
        load_tables(engine, media_type_df, apod_entry_df)
    except Exception as e:
        log.critical(f"Database load failed: {e}")
        return

    # ── Export Analytics CSVs ─────────────────────────────────────────────
    try:
        export_analytics_datasets(engine)
    except Exception as e:
        log.error(f"Analytics export failed: {e}")

    # ── Verify ────────────────────────────────────────────────────────────
    verify_row_counts(engine, new_record_count)

    log.info("=" * 55)
    log.info("ETL PIPELINE COMPLETE")
    log.info(f"  New records loaded : {new_record_count}")
    log.info(f"  Analytics CSVs     : {EXPORT_DIR}")
    log.info(f"  Log file           : {LOG_DIR / 'etl_pipeline.log'}")
    log.info("=" * 55)


if __name__ == "__main__":
    main()
