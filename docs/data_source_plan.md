# Data Source Plan

## Project: Cosmos in Data — NASA APOD ETL Pipeline
**MSBA 692: Pipeline to Insights**

---

## Primary Data Source

**NASA Astronomy Picture of the Day (APOD) API**

| Detail | Value |
|---|---|
| API Endpoint | `https://api.nasa.gov/planetary/apod` |
| Provider | NASA (National Aeronautics and Space Administration) |
| Access | Free with a registered API key |
| Registration | https://api.nasa.gov — instant, no cost, no credit card |
| Rate Limit | 1,000 requests per day with a registered key |
| Format | JSON |
| Documentation | https://api.nasa.gov/#apod |

---

## Authentication

The API uses a simple API key passed as a query parameter:

```
https://api.nasa.gov/planetary/apod?api_key=YOUR_KEY&date=2024-01-01
```

The key is stored in a `.env` file and loaded at runtime using `python-dotenv`. It is never hardcoded in the source code and is excluded from version control via `.gitignore`.

---

## Query Parameters

| Parameter | Type | Description |
|---|---|---|
| `api_key` | string | Required. Your registered NASA API key |
| `date` | string | Single date in YYYY-MM-DD format |
| `start_date` | string | Start of a date range (returns array of entries) |
| `end_date` | string | End of a date range (used with start_date) |

Date-range queries return a JSON array of up to 100 entries per request. The pipeline uses 30-day batches to stay within limits and allow per-batch error recovery.

---

## Fields Returned

| Field | Type | Nullable | Description |
|---|---|---|---|
| `date` | string | No | Entry date in YYYY-MM-DD format |
| `title` | string | No | Title of the astronomy image or video |
| `explanation` | string | No | Plain-language description by a professional astronomer |
| `url` | string | No | Standard-resolution URL for the media asset |
| `hdurl` | string | Yes | High-definition image URL — null for video entries |
| `media_type` | string | No | Either `image` or `video` |
| `copyright` | string | Yes | Copyright attribution — null for public domain entries |
| `service_version` | string | No | API version string — not stored in the database |

---

## Date Range Extracted

| Setting | Value |
|---|---|
| Start date | 2020-01-01 |
| End date | Current date (dynamic — updates on each pipeline run) |
| Total entries | ~1,355 (limited by API timeouts on some batches) |
| Full archive | Available from 1995-06-16 to present |

The `START_DATE` and `END_DATE` values are configurable in the `.env` file. The pipeline defaults to 2020-01-01 as the start date to keep the initial load manageable within course constraints.

---

## Data Quality Characteristics

- **Consistency:** The API returns well-structured JSON with consistent field names across all entries
- **Completeness:** `hdurl` and `copyright` are intentionally nullable — video entries do not have an `hdurl`, and many entries are in the public domain
- **Unexpected values:** At least one entry (October 2024) returned an unrecognized `media_type` value. The pipeline handles this with a fallback default during the transform stage
- **API reliability:** NASA's APOD API occasionally returns 503 errors under load. The pipeline implements retry logic with exponential backoff to handle transient failures gracefully

---

## Why This Data Source

- Free and publicly accessible — no cost, no approval process
- Stable and well-maintained — NASA has operated APOD since 1995
- Clean, structured JSON — minimal preprocessing required
- Rich enough for meaningful analysis — title, explanation, media type, and date support multiple visualization dimensions
- Unique gap in the ecosystem — no pre-built structured dataset exists for the APOD archive, making this pipeline genuinely useful
