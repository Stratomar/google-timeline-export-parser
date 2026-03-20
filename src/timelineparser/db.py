import json
import sqlite3
from pathlib import Path

SCHEMA = """
CREATE TABLE IF NOT EXISTS places (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    place_id TEXT UNIQUE,
    lat REAL,
    lng REAL,
    semantic_type TEXT,
    name TEXT,
    address TEXT,
    types TEXT,
    enriched_at TEXT,
    created_at TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_places_semantic_type ON places(semantic_type);

CREATE TABLE IF NOT EXISTS visits (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    place_id TEXT,
    start_time TEXT,
    end_time TEXT,
    start_tz_offset_min INTEGER,
    end_tz_offset_min INTEGER,
    hierarchy_level INTEGER,
    probability REAL,
    place_probability REAL,
    created_at TEXT DEFAULT (datetime('now')),
    FOREIGN KEY (place_id) REFERENCES places(place_id),
    UNIQUE (place_id, start_time)
);
CREATE INDEX IF NOT EXISTS idx_visits_place_id ON visits(place_id);
CREATE INDEX IF NOT EXISTS idx_visits_start_time ON visits(start_time);

CREATE TABLE IF NOT EXISTS activities (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    start_time TEXT,
    end_time TEXT,
    start_tz_offset_min INTEGER,
    end_tz_offset_min INTEGER,
    start_lat REAL,
    start_lng REAL,
    end_lat REAL,
    end_lng REAL,
    distance_meters REAL,
    activity_type TEXT,
    probability REAL,
    created_at TEXT DEFAULT (datetime('now')),
    UNIQUE (start_time, end_time, activity_type)
);
CREATE INDEX IF NOT EXISTS idx_activities_type ON activities(activity_type);
CREATE INDEX IF NOT EXISTS idx_activities_start_time ON activities(start_time);

CREATE TABLE IF NOT EXISTS timeline_paths (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    segment_start_time TEXT,
    segment_end_time TEXT,
    created_at TEXT DEFAULT (datetime('now')),
    UNIQUE (segment_start_time, segment_end_time)
);

CREATE TABLE IF NOT EXISTS timeline_points (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    path_id INTEGER NOT NULL,
    lat REAL,
    lng REAL,
    timestamp TEXT,
    FOREIGN KEY (path_id) REFERENCES timeline_paths(id)
);
CREATE INDEX IF NOT EXISTS idx_timeline_points_path_id ON timeline_points(path_id);

CREATE TABLE IF NOT EXISTS raw_signals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    lat REAL,
    lng REAL,
    timestamp TEXT UNIQUE,
    accuracy_meters REAL,
    source TEXT
);
CREATE INDEX IF NOT EXISTS idx_raw_signals_timestamp ON raw_signals(timestamp);

CREATE TABLE IF NOT EXISTS trips (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    start_time TEXT,
    end_time TEXT,
    start_tz_offset_min INTEGER,
    end_tz_offset_min INTEGER,
    distance_from_origin_kms REAL,
    destination_place_ids TEXT,
    created_at TEXT DEFAULT (datetime('now')),
    UNIQUE (start_time, end_time)
);
CREATE INDEX IF NOT EXISTS idx_trips_start_time ON trips(start_time);
"""

SEMANTIC_TYPE_PRIORITY = {
    "HOME": 4,
    "WORK": 3,
    "SEARCHED_ADDRESS": 2,
    "UNKNOWN": 1,
}


class Database:
    def __init__(self, db_path: str | Path):
        self.conn = sqlite3.connect(str(db_path))
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")

    def create_tables(self) -> None:
        self.conn.executescript(SCHEMA)

    def __enter__(self) -> "Database":
        return self

    def __exit__(self, exc_type: type | None, *exc: object) -> None:
        if exc_type is None:
            self.conn.commit()
        self.close()

    def close(self) -> None:
        self.conn.close()

    def commit(self) -> None:
        self.conn.commit()

    def insert_place(
        self,
        place_id: str,
        lat: float | None = None,
        lng: float | None = None,
        semantic_type: str | None = None,
    ) -> None:
        self.conn.execute(
            "INSERT OR IGNORE INTO places (place_id, lat, lng, semantic_type) "
            "VALUES (?, ?, ?, ?)",
            (place_id, lat, lng, semantic_type),
        )
        row = self.conn.execute(
            "SELECT lat, lng, semantic_type FROM places WHERE place_id = ?",
            (place_id,),
        ).fetchone()
        if not row:
            return
        updates: list[str] = []
        params: list[object] = []
        if lat is not None and row["lat"] is None:
            updates.append("lat = ?")
            params.append(lat)
        if lng is not None and row["lng"] is None:
            updates.append("lng = ?")
            params.append(lng)
        if semantic_type:
            new_priority = SEMANTIC_TYPE_PRIORITY.get(semantic_type, 0)
            old_priority = SEMANTIC_TYPE_PRIORITY.get(row["semantic_type"] or "", 0)
            if new_priority > old_priority:
                updates.append("semantic_type = ?")
                params.append(semantic_type)
        if updates:
            params.append(place_id)
            self.conn.execute(
                f"UPDATE places SET {', '.join(updates)} WHERE place_id = ?",  # noqa: S608
                params,
            )

    def insert_visit(
        self,
        place_id: str | None,
        start_time: str | None,
        end_time: str | None,
        start_tz_offset_min: int | None,
        end_tz_offset_min: int | None,
        hierarchy_level: int | None,
        probability: float | None,
        place_probability: float | None,
    ) -> None:
        self.conn.execute(
            "INSERT OR IGNORE INTO visits (place_id, start_time, end_time, "
            "start_tz_offset_min, end_tz_offset_min, hierarchy_level, "
            "probability, place_probability) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                place_id,
                start_time,
                end_time,
                start_tz_offset_min,
                end_tz_offset_min,
                hierarchy_level,
                probability,
                place_probability,
            ),
        )

    def insert_activity(
        self,
        start_time: str | None,
        end_time: str | None,
        start_tz_offset_min: int | None,
        end_tz_offset_min: int | None,
        start_lat: float | None,
        start_lng: float | None,
        end_lat: float | None,
        end_lng: float | None,
        distance_meters: float | None,
        activity_type: str | None,
        probability: float | None,
    ) -> None:
        self.conn.execute(
            "INSERT OR IGNORE INTO activities (start_time, end_time, start_tz_offset_min, "
            "end_tz_offset_min, start_lat, start_lng, end_lat, end_lng, "
            "distance_meters, activity_type, probability) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                start_time,
                end_time,
                start_tz_offset_min,
                end_tz_offset_min,
                start_lat,
                start_lng,
                end_lat,
                end_lng,
                distance_meters,
                activity_type,
                probability,
            ),
        )

    def insert_timeline_path(
        self,
        segment_start_time: str | None,
        segment_end_time: str | None,
        points: list[tuple[float, float, str]],
    ) -> None:
        cursor = self.conn.execute(
            "INSERT OR IGNORE INTO timeline_paths (segment_start_time, segment_end_time) "
            "VALUES (?, ?)",
            (segment_start_time, segment_end_time),
        )
        if cursor.rowcount == 0:
            return
        path_id = cursor.lastrowid
        self.conn.executemany(
            "INSERT INTO timeline_points (path_id, lat, lng, timestamp) "
            "VALUES (?, ?, ?, ?)",
            [(path_id, lat, lng, ts) for lat, lng, ts in points],
        )

    def insert_raw_signal(
        self,
        lat: float | None,
        lng: float | None,
        timestamp: str | None,
        accuracy_meters: float | None,
        source: str | None,
    ) -> None:
        self.conn.execute(
            "INSERT OR IGNORE INTO raw_signals (lat, lng, timestamp, accuracy_meters, source) "
            "VALUES (?, ?, ?, ?, ?)",
            (lat, lng, timestamp, accuracy_meters, source),
        )

    def insert_trip(
        self,
        start_time: str | None,
        end_time: str | None,
        start_tz_offset_min: int | None,
        end_tz_offset_min: int | None,
        distance_from_origin_kms: float | None,
        destination_place_ids: list[str],
    ) -> None:
        self.conn.execute(
            "INSERT OR IGNORE INTO trips (start_time, end_time, start_tz_offset_min, "
            "end_tz_offset_min, distance_from_origin_kms, destination_place_ids) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (
                start_time,
                end_time,
                start_tz_offset_min,
                end_tz_offset_min,
                distance_from_origin_kms,
                json.dumps(destination_place_ids),
            ),
        )
