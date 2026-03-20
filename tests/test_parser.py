import json

from timelineparser.db import Database
from timelineparser.parser import parse_timeline


def make_timeline_json(tmp_path):
    data = {
        "semanticSegments": [
            {
                "startTime": "2024-01-01T10:00:00.000+01:00",
                "endTime": "2024-01-01T11:00:00.000+01:00",
                "startTimeTimezoneUtcOffsetMinutes": 60,
                "endTimeTimezoneUtcOffsetMinutes": 60,
                "visit": {
                    "hierarchyLevel": 0,
                    "probability": 0.8,
                    "topCandidate": {
                        "placeId": "ChIJ_test_place",
                        "semanticType": "HOME",
                        "probability": 0.95,
                        "placeLocation": {"latLng": "45.0°, 5.0°"},
                    },
                },
            },
            {
                "startTime": "2024-01-01T11:00:00.000+01:00",
                "endTime": "2024-01-01T11:30:00.000+01:00",
                "startTimeTimezoneUtcOffsetMinutes": 60,
                "endTimeTimezoneUtcOffsetMinutes": 60,
                "activity": {
                    "start": {"latLng": "45.0°, 5.0°"},
                    "end": {"latLng": "45.1°, 5.1°"},
                    "distanceMeters": 500.0,
                    "topCandidate": {"type": "WALKING", "probability": 0.9},
                },
            },
            {
                "startTime": "2024-01-01T09:00:00.000+01:00",
                "endTime": "2024-01-01T10:00:00.000+01:00",
                "timelinePath": [
                    {"point": "45.0°, 5.0°", "time": "2024-01-01T09:10:00.000+01:00"},
                    {"point": "45.1°, 5.1°", "time": "2024-01-01T09:20:00.000+01:00"},
                ],
            },
            {
                "startTime": "2024-01-02T00:00:00.000+01:00",
                "endTime": "2024-01-03T00:00:00.000+01:00",
                "startTimeTimezoneUtcOffsetMinutes": 60,
                "endTimeTimezoneUtcOffsetMinutes": 60,
                "timelineMemory": {
                    "trip": {
                        "distanceFromOriginKms": 100,
                        "destinations": [{"identifier": {"placeId": "ChIJ_trip_dest"}}],
                    }
                },
            },
        ],
        "rawSignals": [
            {
                "position": {
                    "LatLng": "45.0°, 5.0°",
                    "accuracyMeters": 10,
                    "source": "GPS",
                    "timestamp": "2024-01-01T09:00:00.000+01:00",
                }
            }
        ],
        "userLocationProfile": {
            "frequentPlaces": [
                {
                    "placeId": "ChIJ_freq_place",
                    "placeLocation": "45.0°, 5.0°",
                    "label": "HOME",
                }
            ],
            "persona": {"travelModeAffinities": []},
        },
    }
    path = tmp_path / "Timeline.json"
    path.write_text(json.dumps(data))
    return path


class TestParser:
    def test_parse_full(self, tmp_path):
        json_path = make_timeline_json(tmp_path)
        db = Database(":memory:")
        db.create_tables()
        stats = parse_timeline(json_path, db)
        assert stats["visits"] == 1
        assert stats["activities"] == 1
        assert stats["paths"] == 1
        assert stats["trips"] == 1
        assert stats["raw_signals"] == 1
        assert stats["frequent_places"] == 1
        assert stats["errors"] == 0
        # Check places dedup (freq place + visit place + trip dest = 3 unique)
        count = db.conn.execute("SELECT COUNT(*) as c FROM places").fetchone()
        assert count["c"] == 3
        db.close()

    def test_malformed_segment(self, tmp_path):
        data = {
            "semanticSegments": [
                {
                    "visit": {
                        "topCandidate": {
                            "placeId": "ChIJ_ok",
                            "placeLocation": {"latLng": "45.0°, 5.0°"},
                        },
                        "probability": 0.5,
                    },
                    "startTime": "2024-01-01T10:00:00.000+01:00",
                    "endTime": "2024-01-01T11:00:00.000+01:00",
                },
                {"visit": {"topCandidate": {}}},
            ],
            "rawSignals": [],
            "userLocationProfile": {"frequentPlaces": []},
        }
        path = tmp_path / "Timeline.json"
        path.write_text(json.dumps(data))
        db = Database(":memory:")
        db.create_tables()
        stats = parse_timeline(path, db)
        assert stats["visits"] == 1
        assert stats["errors"] == 0
        db.close()
