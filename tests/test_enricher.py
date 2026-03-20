from unittest.mock import MagicMock, patch

from timelineparser.db import Database
from timelineparser.enricher import enrich_places


def make_db_with_places():
    db = Database(":memory:")
    db.create_tables()
    db.insert_place("ChIJ_place1", 45.0, 5.0, "HOME")
    db.insert_place("ChIJ_place2", 45.1, 5.1, "WORK")
    db.commit()
    return db


class TestEnricher:
    @patch("timelineparser.enricher.requests.get")
    @patch("timelineparser.enricher.time.sleep")
    def test_enriches_places(self, mock_sleep, mock_get):
        db = make_db_with_places()
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "displayName": {"text": "My Home"},
            "formattedAddress": "123 Main St",
            "types": ["premise"],
        }
        mock_get.return_value = mock_resp

        stats = enrich_places(db, "fake-api-key", batch_size=50, delay=0)
        assert stats["enriched"] == 2
        assert stats["failed"] == 0

        row = db.conn.execute(
            "SELECT name, address FROM places WHERE place_id = 'ChIJ_place1'"
        ).fetchone()
        assert row["name"] == "My Home"
        assert row["address"] == "123 Main St"
        db.close()

    @patch("timelineparser.enricher.requests.get")
    @patch("timelineparser.enricher.time.sleep")
    def test_handles_api_error(self, mock_sleep, mock_get):
        db = make_db_with_places()
        mock_resp = MagicMock()
        mock_resp.status_code = 404
        mock_resp.text = "Not Found"
        mock_get.return_value = mock_resp

        stats = enrich_places(db, "fake-api-key", batch_size=50, delay=0)
        assert stats["failed"] == 2
        assert stats["enriched"] == 0
        # Failed places should be marked so they aren't retried
        unenriched = db.conn.execute(
            "SELECT COUNT(*) as c FROM places WHERE enriched_at IS NULL"
        ).fetchone()
        assert unenriched["c"] == 0
        db.close()

    @patch("timelineparser.enricher.requests.get")
    @patch("timelineparser.enricher.time.sleep")
    def test_batch_size_limits(self, mock_sleep, mock_get):
        db = make_db_with_places()
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "displayName": {"text": "Place"},
            "formattedAddress": "Addr",
            "types": [],
        }
        mock_get.return_value = mock_resp

        stats = enrich_places(db, "fake-api-key", batch_size=1, delay=0)
        assert stats["enriched"] == 1
        db.close()
