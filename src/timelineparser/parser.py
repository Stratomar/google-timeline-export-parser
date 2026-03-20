import json
import logging
from pathlib import Path

from tqdm import tqdm

from timelineparser.db import Database
from timelineparser.extractors import (
    extract_activity,
    extract_raw_signal,
    extract_timeline_path,
    extract_trip,
    extract_visit,
    parse_latlng,
)

logger = logging.getLogger(__name__)

BATCH_SIZE = 1000


def parse_timeline(json_path: str | Path, db: Database) -> dict:
    logger.info("Loading %s ...", json_path)
    with open(json_path) as f:
        data = json.load(f)

    stats = {
        "visits": 0,
        "activities": 0,
        "paths": 0,
        "trips": 0,
        "raw_signals": 0,
        "frequent_places": 0,
        "errors": 0,
    }

    # Extract frequent places from user profile
    profile = data.get("userLocationProfile", {})
    for fp in profile.get("frequentPlaces", []):
        place_id = fp.get("placeId")
        if not place_id:
            continue
        coords = parse_latlng(fp.get("placeLocation"))
        lat, lng = coords if coords else (None, None)
        db.insert_place(place_id, lat, lng, fp.get("label"))
        stats["frequent_places"] += 1
    db.commit()

    # Parse semantic segments
    segments = data.get("semanticSegments", [])
    for i, segment in enumerate(tqdm(segments, desc="Segments")):
        try:
            if "visit" in segment:
                v = extract_visit(segment)
                if v:
                    db.insert_place(
                        v["place_id"], v["lat"], v["lng"], v["semantic_type"]
                    )
                    db.insert_visit(
                        v["place_id"],
                        v["start_time"],
                        v["end_time"],
                        v["start_tz_offset_min"],
                        v["end_tz_offset_min"],
                        v["hierarchy_level"],
                        v["probability"],
                        v["place_probability"],
                    )
                    stats["visits"] += 1

            elif "activity" in segment:
                a = extract_activity(segment)
                if a:
                    db.insert_activity(
                        a["start_time"],
                        a["end_time"],
                        a["start_tz_offset_min"],
                        a["end_tz_offset_min"],
                        a["start_lat"],
                        a["start_lng"],
                        a["end_lat"],
                        a["end_lng"],
                        a["distance_meters"],
                        a["activity_type"],
                        a["probability"],
                    )
                    stats["activities"] += 1

            elif "timelinePath" in segment:
                p = extract_timeline_path(segment)
                if p:
                    db.insert_timeline_path(
                        p["segment_start_time"],
                        p["segment_end_time"],
                        p["points"],
                    )
                    stats["paths"] += 1

            elif "timelineMemory" in segment:
                t = extract_trip(segment)
                if t:
                    for pid in t["destination_place_ids"]:
                        db.insert_place(pid)
                    db.insert_trip(
                        t["start_time"],
                        t["end_time"],
                        t["start_tz_offset_min"],
                        t["end_tz_offset_min"],
                        t["distance_from_origin_kms"],
                        t["destination_place_ids"],
                    )
                    stats["trips"] += 1

        except (KeyError, ValueError, TypeError):
            logger.warning("Error processing segment %d", i, exc_info=True)
            stats["errors"] += 1

        if (i + 1) % BATCH_SIZE == 0:
            db.commit()

    db.commit()

    # Parse raw signals
    raw_signals = data.get("rawSignals", [])
    for i, signal in enumerate(tqdm(raw_signals, desc="Raw signals")):
        try:
            rs = extract_raw_signal(signal)
            if rs:
                db.insert_raw_signal(
                    rs["lat"],
                    rs["lng"],
                    rs["timestamp"],
                    rs["accuracy_meters"],
                    rs["source"],
                )
                stats["raw_signals"] += 1
        except (KeyError, ValueError, TypeError):
            logger.warning("Error processing raw signal %d", i, exc_info=True)
            stats["errors"] += 1

        if (i + 1) % BATCH_SIZE == 0:
            db.commit()

    db.commit()
    return stats
