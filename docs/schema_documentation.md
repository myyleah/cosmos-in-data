# Database Schema Documentation

## NASA Astronomy Picture of the Day (APOD) Data Pipeline

This database stores NASA Astronomy Picture of the Day entry data loaded into a PostgreSQL database hosted in Supabase.

The schema is normalized to approximately Third Normal Form (3NF) by:

- separating media type classifications into a dedicated lookup table
- avoiding repeated string values for media type across all entry records
- using foreign keys to enforce referential integrity between tables

---

### ER Diagram

![APOD ER Diagram](<APOD_ERD.png>)

## Database Overview

The database contains two tables:

- `media_type`
- `apod_entry`

### Entity Relationship Summary

| Table | Purpose |
| --- | --- |
| `media_type` | Lookup table storing the two possible NASA APOD media types: image and video |
| `apod_entry` | Stores one record per daily NASA APOD entry including title, explanation, URL, and derived metrics |

---

## Table Documentation

### 1. `media_type`

**Purpose**

Stores the two possible media type values returned by the NASA APOD API. The API returns either `image` or `video` for each daily entry. Storing these as a lookup table avoids repeating string values across every row in `apod_entry` and allows clean filtering and grouping in dashboard queries.

**Examples**

- ID `1` = image
- ID `2` = video

**Primary Key**

- `media_type_id`

**Relationships**

- One `media_type` can classify many `apod_entry` records.
- Referenced by `apod_entry.media_type_id`.

**Table Structure**

| Column Name | Data Type | Key | Description |
| --- | --- | --- | --- |
| `media_type_id` | `INTEGER` | Primary Key | Unique integer identifier for the media type |
| `media_type_name` | `VARCHAR(10)` | Unique | Human-readable media type value: `image` or `video` |

---

### 2. `apod_entry`

**Purpose**

Stores daily NASA Astronomy Picture of the Day entries retrieved from the NASA APOD REST API.

**Entry data includes**

- entry date and title
- full astronomer explanation text
- standard and high-definition media URLs
- copyright attribution
- derived word and character counts from the explanation field

**Primary Key**

- `entry_id`

**Foreign Keys**

- `media_type_id` → `media_type.media_type_id`

**Relationships**

- Many entries can reference one media type.
- Each entry date is unique — NASA publishes exactly one entry per calendar day.

**Table Structure**

| Column Name | Data Type | Key | Description |
| --- | --- | --- | --- |
| `entry_id` | `BIGSERIAL` | Primary Key | Auto-generated surrogate record ID |
| `entry_date` | `DATE` | Unique | Calendar date of the APOD entry (YYYY-MM-DD); natural key |
| `media_type_id` | `INTEGER` | Foreign Key | References `media_type.media_type_id`; classifies entry as image or video |
| `title` | `TEXT` | | Title of the APOD image or video as returned by the API |
| `explanation` | `TEXT` | | Full plain-language explanation written by a professional astronomer |
| `url` | `TEXT` | | Standard-resolution URL for the media asset |
| `hdurl` | `TEXT` | | High-definition image URL; NULL for video entries |
| `copyright` | `TEXT` | | Copyright attribution; NULL when entry is in the public domain |
| `word_count` | `INTEGER` | | Number of words in the explanation field; computed during ETL transform |
| `char_count` | `INTEGER` | | Number of characters in the explanation field; computed during ETL transform |

---

## Cardinality Relationships

| Parent Table | Child Table | Relationship Type |
| --- | --- | --- |
| `media_type` | `apod_entry` | One-to-Many |

---

## Normalization Notes (3NF)

This schema is normalized to approximately Third Normal Form:

- Media type string values are separated into `media_type` to avoid repetition across all entry rows.
- Derived columns (`word_count`, `char_count`) depend only on the primary key of `apod_entry` and are computed once during ETL rather than at query time.
- Non-key columns depend only on each table's primary key.
- No transitive dependencies exist between non-key columns.

---

## Data Sources

### APOD Data Source

Entry data is sourced from the NASA Astronomy Picture of the Day REST API.

The API provides:

- daily entry date and title
- full astronomer explanation text
- standard and high-definition media URLs
- media type classification (image or video)
- optional copyright attribution

API endpoint: `https://api.nasa.gov/planetary/apod`

Date-range queries use `start_date` and `end_date` parameters and return up to 100 entries per request. A registered NASA API key allows up to 1,000 requests per day.

---

## Example Relationship Flow

- `apod_entry.media_type_id = 1`
- `media_type.media_type_id = 1`
- `media_type_name` returned: `image`

This design prevents the string `image` or `video` from being stored repeatedly across every row in `apod_entry`.

---

## SQL DDL

```sql
-- Drop existing tables (safe re-run)
DROP TABLE IF EXISTS public.apod_entry CASCADE;
DROP TABLE IF EXISTS public.media_type CASCADE;

-- Lookup table: media type
CREATE TABLE IF NOT EXISTS public.media_type (
    media_type_id   INTEGER      PRIMARY KEY,
    media_type_name VARCHAR(10)  NOT NULL UNIQUE
);

-- Main fact table: one row per APOD entry
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
```
