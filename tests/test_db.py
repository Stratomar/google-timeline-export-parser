from timelineparser.db import Database


def make_db():
    db = Database(":memory:")
    db.create_tables()
    return db


class TestDatabase:
    def test_create_tables(self):
        db = make_db()
        tables = db.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        ).fetchall()
        names = [t["name"] for t in tables]
        assert "places" in names
        assert "visits" in names
        assert "activities" in names
        assert "timeline_paths" in names
        assert "timeline_points" in names
        assert "raw_signals" in names
        assert "trips" in names
        db.close()

    def test_insert_place_dedup(self):
        db = make_db()
        db.insert_place("abc123", 45.0, 5.0, "UNKNOWN")
        db.insert_place("abc123", 45.0, 5.0, "HOME")
        db.commit()
        row = db.conn.execute(
            "SELECT * FROM places WHERE place_id = 'abc123'"
        ).fetchone()
        assert row["semantic_type"] == "HOME"
        count = db.conn.execute("SELECT COUNT(*) as c FROM places").fetchone()
        assert count["c"] == 1
        db.close()

    def test_insert_visit(self):
        db = make_db()
        db.insert_place("p1", 45.0, 5.0, "WORK")
        db.insert_visit(
            "p1", "2024-01-01T00:00:00", "2024-01-01T01:00:00", 60, 60, 0, 0.8, 0.9
        )
        db.commit()
        count = db.conn.execute("SELECT COUNT(*) as c FROM visits").fetchone()
        assert count["c"] == 1
        db.close()

    def test_insert_activity(self):
        db = make_db()
        db.insert_activity(
            "2024-01-01T00:00:00",
            "2024-01-01T01:00:00",
            60,
            60,
            45.0,
            5.0,
            45.1,
            5.1,
            1000.0,
            "WALKING",
            0.5,
        )
        db.commit()
        count = db.conn.execute("SELECT COUNT(*) as c FROM activities").fetchone()
        assert count["c"] == 1
        db.close()

    def test_insert_timeline_path(self):
        db = make_db()
        db.insert_timeline_path(
            "2024-01-01T00:00:00",
            "2024-01-01T01:00:00",
            [(45.0, 5.0, "2024-01-01T00:10:00"), (45.1, 5.1, "2024-01-01T00:20:00")],
        )
        db.commit()
        paths = db.conn.execute("SELECT COUNT(*) as c FROM timeline_paths").fetchone()
        points = db.conn.execute("SELECT COUNT(*) as c FROM timeline_points").fetchone()
        assert paths["c"] == 1
        assert points["c"] == 2
        db.close()

    def test_insert_raw_signal(self):
        db = make_db()
        db.insert_raw_signal(45.0, 5.0, "2024-01-01T00:00:00", 10.0, "GPS")
        db.commit()
        count = db.conn.execute("SELECT COUNT(*) as c FROM raw_signals").fetchone()
        assert count["c"] == 1
        db.close()

    def test_insert_trip(self):
        db = make_db()
        db.insert_place("dest1")
        db.insert_trip(
            "2024-01-01",
            "2024-01-02",
            60,
            60,
            445.0,
            ["dest1"],
        )
        db.commit()
        count = db.conn.execute("SELECT COUNT(*) as c FROM trips").fetchone()
        assert count["c"] == 1
        db.close()
