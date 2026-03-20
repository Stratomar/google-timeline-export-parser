# Google Timeline Export Parser

Turns a Google Maps Timeline JSON export (from Google Takeout) into a SQLite database you can actually query.

## Setup

```bash
uv sync
```

## Usage

### Parsing

From the project directory:

```bash
uv run timelineparser parse --input /path/to/Timeline.json
```

This creates `timeline.db` next to the input file. Use `--db` to put it somewhere else.

### Enriching places

The raw export only has Google Place IDs — not very useful on their own. The `enrich` command hits the Google Places API to resolve them into actual names and addresses.

You'll need a Google Places API key:

1. Enable the Places API (New): https://console.cloud.google.com/marketplace/product/google/places.googleapis.com
2. Grab your API key from: https://console.cloud.google.com/google/maps-apis/credentials
3. Make sure the key doesn't have restrictions blocking the Places API (New)

(Steps accurate as of early 2026.)

Copy `.env.example` to `.env` and add your key:

```bash
cp .env.example .env
```

Then run:

```bash
uv run timelineparser enrich --db /path/to/timeline.db --batch-size 50
```

## What's in the database

| Table | What it stores |
|-------|----------------|
| `places` | Unique places — coords, semantic type (home/work/etc), enriched name and address |
| `visits` | Each time you visited a place, with timestamps and probability scores |
| `activities` | Movement between places — walking, driving, flying, etc. |
| `timeline_paths` | GPS breadcrumb trail segments |
| `timeline_points` | Individual points within those segments |
| `raw_signals` | Raw GPS position readings |
| `trips` | Trip summaries from Timeline Memories |


