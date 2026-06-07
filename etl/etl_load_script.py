"""
NASA APOD Supabase PostgreSQL ETL Loader
-----------------------------------------
Pulls data from the NASA Astronomy Picture of the Day (APOD) API,
transforms the JSON responses using pandas, and loads the cleaned
records into a Supabase PostgreSQL database.

Tables created:
    public.media_type   -- lookup: image | video
    public.apod_entry   -- one row per APOD entry

Required packages:
    pip install pandas sqlalchemy psycopg2-binary python-dotenv requests

.env values expected:
    DB_PASSWORD=your_supabase_database_password
    DB_REF=your_supabase_project_ref

Optional .env override:
    SUPABASE_DB_URL=postgresql+psycopg2://postgres:PASSWORD@db.REF.supabase.co:5432/postgres

Optional flags:
    RESET_TABLES=true    -- drop and recreate tables before loading (default: true)
    NASA_API_KEY=...     -- your NASA API key (default: DEMO_KEY, limited to 30 req/hour)
    START_DATE=YYYY-MM-DD  -- earliest date to pull (default: 2020-01-01)
    END_DATE=YYYY-MM-DD    -- latest date to pull   (default: today)
"""

from __future__ import annotations

import os
import time
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.types import BigInteger, Date, Integer, Text


# ── Constants ──────────────────────────────────────────────────────────────────

BASE_DIR = Path(__file__).resolve().parent

# NASA APOD API endpoint
APOD_URL = "https://api.nasa.gov/planetary/apod"

# Max entries the APOD API returns per date-range request
BATCH_SIZE = 30

# Seconds to wait between batch requests (avoids rate limit on DEMO_KEY)
REQUEST_DELAY = 2

# Seed values for media_type lookup table
MEDIA_TYPES = [
    {"media_type_id": 1, "media_type_name": "image"},
    {"media_type_id": 2, "media_type_name": "video"},
]


# ── Connection ─────────────────────────────────────────────────────────────────

def get_database_url() -> str:
    """
    Build the Supabase PostgreSQL connection URL from .env file.
    Supports a direct SUPABASE_DB_URL override for flexibility.
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
    Check whether to drop and recreate tables before loading.
    Reads RESET_TABLES from .env; defaults to true for a clean load.
    """
    return os.getenv("RESET_TABLES", "true").strip().lower() in {"1", "true", "yes", "y"}


# ── Schema ─────────────────────────────────────────────────────────────────────

def create_schema(engine) -> None:
    """
    Drop existing tables (if RESET_TABLES=true) and create the schema.
    Always drops in dependency order (child before parent).
    """
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
            print("Dropping existing tables...")
            conn.execute(text(drop_sql))
        print("Creating schema...")
        conn.execute(text(create_sql))


# ── Extraction ─────────────────────────────────────────────────────────────────

def build_date_ranges(start: date, end: date, batch_days: int = BATCH_SIZE) -> list[tuple[date, date]]:
    """
    Split the full date range into smaller batches to stay within
    NASA API limits (max 100 per request; DEMO_KEY 30 req/hour).
    Returns a list of (batch_start, batch_end) tuples.
    """
    ranges = []
    current = start
    while current <= end:
        batch_end = min(current + timedelta(days=batch_days - 1), end)
        ranges.append((current, batch_end))
        current = batch_end + timedelta(days=1)
    return ranges


def fetch_apod_batch(api_key: str, start: date, end: date) -> list[dict]:
    """
    Call the NASA APOD API for one date-range batch.
    Returns a list of entry dictionaries, or an empty list on failure.
    """
    params = {
        "api_key":    api_key,
        "start_date": start.isoformat(),
        "end_date":   end.isoformat(),
    }

    try:
        response = requests.get(APOD_URL, params=params, timeout=15)
        response.raise_for_status()
        entries = response.json()
        print(f"  Fetched {len(entries)} entries: {start} → {end}")
        return entries

    except requests.exceptions.HTTPError as e:
        print(f"  HTTP error for {start} → {end}: {e}")
        return []

    except requests.exceptions.RequestException as e:
        print(f"  Request failed for {start} → {end}: {e}")
        return []


def extract_apod_data() -> pd.DataFrame:
    """
    Pull all APOD entries within the configured date range.
    Iterates through date-range batches with a delay between requests.
    Returns a raw DataFrame of all fetched entries.
    """
    load_dotenv()

    api_key    = os.getenv("NASA_API_KEY", "DEMO_KEY")
    start_date = date.fromisoformat(os.getenv("START_DATE", "2020-01-01"))
    end_date   = date.fromisoformat(os.getenv("END_DATE",   date.today().isoformat()))

    print(f"Extracting APOD entries: {start_date} → {end_date}")
    print(f"API key: {'DEMO_KEY (limited)' if api_key == 'DEMO_KEY' else 'registered key'}")

    date_ranges = build_date_ranges(start_date, end_date)
    all_entries: list[dict] = []

    for i, (batch_start, batch_end) in enumerate(date_ranges):
        batch = fetch_apod_batch(api_key, batch_start, batch_end)
        all_entries.extend(batch)

        # Pause between requests to avoid hitting the rate limit
        if i < len(date_ranges) - 1:
            time.sleep(REQUEST_DELAY)

    print(f"Extraction complete: {len(all_entries)} total entries fetched.")
    return pd.DataFrame(all_entries)


# ── Transformation ─────────────────────────────────────────────────────────────

def build_media_type_df() -> pd.DataFrame:
    """
    Return the seed DataFrame for the media_type lookup table.
    Only two values exist: image (1) and video (2).
    """
    return pd.DataFrame(MEDIA_TYPES)


def build_apod_entry_df(raw_df: pd.DataFrame, media_type_df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and transform the raw API response DataFrame into a shape
    that matches the apod_entry table schema.

    Steps:
        1. Drop unsupported or irrelevant columns returned by the API
        2. Rename columns to match DB schema
        3. Normalize date column to Python date objects
        4. Strip whitespace from text fields
        5. Fill missing nullable fields (hdurl, copyright) with None
        6. Map media_type string → media_type_id integer via lookup join
        7. Compute derived columns: word_count, char_count
        8. Drop any duplicate entries by entry_date
        9. Select and order final columns
    """
    df = raw_df.copy()

    # Remove columns the API sometimes returns that we don't store
    drop_cols = [c for c in ["service_version", "concepts", "thumbnail_url"] if c in df.columns]
    if drop_cols:
        df = df.drop(columns=drop_cols)

    # Rename API field names to match DB column names
    df = df.rename(columns={"date": "entry_date"})

    # Normalize entry_date to Python date object
    df["entry_date"] = pd.to_datetime(df["entry_date"]).dt.date

    # Strip leading/trailing whitespace from all string fields
    for col in ["title", "explanation", "url"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()

    # Ensure nullable fields exist and replace NaN with None (maps to SQL NULL)
    for col in ["hdurl", "copyright"]:
        if col not in df.columns:
            df[col] = None
        else:
            df[col] = df[col].where(df[col].notna(), other=None)

    # Map media_type string to integer ID via the lookup table
    media_map = dict(zip(media_type_df["media_type_name"], media_type_df["media_type_id"]))
    df["media_type_id"] = df["media_type"].map(media_map).fillna(1).astype("Int64")

    # Compute derived columns from explanation text
    df["word_count"] = df["explanation"].str.split().str.len().fillna(0).astype(int)
    df["char_count"] = df["explanation"].str.strip().str.len().fillna(0).astype(int)

    # Remove any duplicate entries by entry_date (keep first occurrence)
    df = df.drop_duplicates(subset=["entry_date"], keep="first")

    # Select only the columns that map to DB columns, in the correct order
    final_cols = [
        "entry_date",
        "media_type_id",
        "title",
        "explanation",
        "url",
        "hdurl",
        "copyright",
        "word_count",
        "char_count",
    ]
    df = df[final_cols].reset_index(drop=True)

    print(f"Transformation complete: {len(df)} records ready for load.")
    return df


# ── Loading ────────────────────────────────────────────────────────────────────

def write_table(df: pd.DataFrame, table_name: str, engine, dtype: dict) -> None:
    """
    Write a DataFrame to the named PostgreSQL table using SQLAlchemy.
    Uses append mode (schema is already created by create_schema).
    Loads in chunks of 500 rows for reliability with large datasets.
    """
    print(f"Loading {table_name} ({len(df)} rows)...")
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


def load_tables(engine, media_type_df: pd.DataFrame, apod_entry_df: pd.DataFrame) -> None:
    """
    Load all transformed DataFrames into the database.
    media_type must be loaded first — apod_entry has a foreign key on it.
    """
    write_table(
        media_type_df,
        "media_type",
        engine,
        {
            "media_type_id":   Integer(),
            "media_type_name": Text(),
        },
    )

    write_table(
        apod_entry_df,
        "apod_entry",
        engine,
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
        },
    )


# ── Orchestration ──────────────────────────────────────────────────────────────

def main() -> None:
    """
    Full ETL orchestration:
        1. Connect to Supabase PostgreSQL
        2. Extract raw APOD entries from NASA API
        3. Transform data using pandas
        4. Create schema in the database
        5. Load transformed data into tables
    """
    print("Connecting to Supabase PostgreSQL...")
    engine = create_engine(get_database_url())

    # Extract
    raw_df = extract_apod_data()
    if raw_df.empty:
        print("No data extracted. Check API key and date range. Exiting.")
        return

    # Transform
    media_type_df  = build_media_type_df()
    apod_entry_df  = build_apod_entry_df(raw_df, media_type_df)

    # Create schema
    create_schema(engine)

    # Load
    load_tables(engine, media_type_df, apod_entry_df)

    print("=" * 45)
    print("ETL LOAD COMPLETE")
    print(f"  media_type rows : {len(media_type_df)}")
    print(f"  apod_entry rows : {len(apod_entry_df)}")
    print("=" * 45)


if __name__ == "__main__":
    main()
