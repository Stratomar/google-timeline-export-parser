import json
import logging
import time
from datetime import datetime, timezone
from urllib.parse import quote

import requests
from tqdm import tqdm

from timelineparser.db import Database

logger = logging.getLogger(__name__)

PLACES_API_BASE = "https://places.googleapis.com/v1/places/"
FIELD_MASK = "displayName,formattedAddress,types"


def _mark_enriched(db: Database, place_id: str) -> None:
    now = datetime.now(timezone.utc).isoformat()
    db.conn.execute(
        "UPDATE places SET enriched_at = ? WHERE place_id = ?",
        (now, place_id),
    )
    db.commit()


def enrich_places(
    db: Database,
    api_key: str,
    batch_size: int = 50,
    delay: float = 0.1,
) -> dict:
    rows = db.conn.execute(
        "SELECT place_id FROM places WHERE enriched_at IS NULL LIMIT ?",
        (batch_size,),
    ).fetchall()

    stats = {"enriched": 0, "failed": 0, "skipped": 0}

    for row in tqdm(rows, desc="Enriching places"):
        place_id = row["place_id"]
        try:
            url = PLACES_API_BASE + quote(place_id, safe="")
            resp = requests.get(
                url,
                headers={
                    "X-Goog-Api-Key": api_key,
                    "X-Goog-FieldMask": FIELD_MASK,
                },
                timeout=10,
            )
            if resp.status_code != 200:
                logger.warning("API error for %s: %d", place_id, resp.status_code)
                _mark_enriched(db, place_id)
                stats["failed"] += 1
                time.sleep(delay)
                continue

            data = resp.json()
            name = data.get("displayName", {}).get("text")
            address = data.get("formattedAddress")
            types = json.dumps(data.get("types", []))
            now = datetime.now(timezone.utc).isoformat()

            db.conn.execute(
                "UPDATE places SET name = ?, address = ?, types = ?, enriched_at = ? "
                "WHERE place_id = ?",
                (name, address, types, now, place_id),
            )
            db.commit()
            stats["enriched"] += 1

        except requests.RequestException:
            logger.warning("Error enriching %s", place_id, exc_info=True)
            _mark_enriched(db, place_id)
            stats["failed"] += 1

        time.sleep(delay)

    return stats
