from timelineparser.extractors import (
    extract_activity,
    extract_raw_signal,
    extract_timeline_path,
    extract_trip,
    extract_visit,
    parse_latlng,
)


class TestParseLatlng:
    def test_degree_sign_format(self):
        assert parse_latlng("45.7212835°, 5.0787821°") == (45.7212835, 5.0787821)

    def test_no_degree_sign(self):
        assert parse_latlng("45.7212835, 5.0787821") == (45.7212835, 5.0787821)

    def test_dict_with_latlng(self):
        result = parse_latlng({"latLng": "45.7222288°, 5.0764892°"})
        assert result == (45.7222288, 5.0764892)

    def test_dict_with_capital_latlng(self):
        result = parse_latlng({"LatLng": "43.7108483°, 7.2934124°"})
        assert result == (43.7108483, 7.2934124)

    def test_negative_coords(self):
        assert parse_latlng("-33.8688, 151.2093") == (-33.8688, 151.2093)

    def test_none(self):
        assert parse_latlng(None) is None

    def test_empty_dict(self):
        assert parse_latlng({}) is None

    def test_malformed(self):
        assert parse_latlng("not a coordinate") is None

    def test_trailing_garbage_rejected(self):
        assert parse_latlng("45.0°, 5.0° extra") is None


class TestExtractVisit:
    def test_basic_visit(self):
        segment = {
            "startTime": "2015-12-15T18:26:19.000-03:00",
            "endTime": "2015-12-15T18:54:16.000-03:00",
            "startTimeTimezoneUtcOffsetMinutes": 60,
            "endTimeTimezoneUtcOffsetMinutes": 60,
            "visit": {
                "hierarchyLevel": 0,
                "probability": 0.77,
                "topCandidate": {
                    "placeId": "ChIJRQW25tbI9EcRnfQU46wS64k",
                    "semanticType": "WORK",
                    "probability": 0.947,
                    "placeLocation": {"latLng": "45.7222288°, 5.0764892°"},
                },
            },
        }
        result = extract_visit(segment)
        assert result is not None
        assert result["place_id"] == "ChIJRQW25tbI9EcRnfQU46wS64k"
        assert result["semantic_type"] == "WORK"
        assert result["lat"] == 45.7222288
        assert result["lng"] == 5.0764892
        assert result["hierarchy_level"] == 0

    def test_missing_place_id(self):
        segment = {"visit": {"topCandidate": {}}}
        assert extract_visit(segment) is None

    def test_no_visit_key(self):
        assert extract_visit({"activity": {}}) is None


class TestExtractActivity:
    def test_basic_activity(self):
        segment = {
            "startTime": "2015-12-15T18:54:16.000-03:00",
            "endTime": "2015-12-15T19:07:00.000-03:00",
            "startTimeTimezoneUtcOffsetMinutes": 60,
            "endTimeTimezoneUtcOffsetMinutes": 60,
            "activity": {
                "start": {"latLng": "45.7222288°, 5.0764892°"},
                "end": {"latLng": "45.7214703°, 5.0618908°"},
                "distanceMeters": 1136.41,
                "topCandidate": {"type": "WALKING", "probability": 0.0},
            },
        }
        result = extract_activity(segment)
        assert result is not None
        assert result["activity_type"] == "WALKING"
        assert result["distance_meters"] == 1136.41
        assert result["start_lat"] == 45.7222288

    def test_no_activity_key(self):
        assert extract_activity({"visit": {}}) is None


class TestExtractTimelinePath:
    def test_basic_path(self):
        segment = {
            "startTime": "2015-12-15T17:00:00.000-03:00",
            "endTime": "2015-12-15T19:00:00.000-03:00",
            "timelinePath": [
                {
                    "point": "45.7212835°, 5.0787821°",
                    "time": "2015-12-15T18:26:00.000-03:00",
                },
                {
                    "point": "45.7189881°, 5.0787063°",
                    "time": "2015-12-15T18:38:00.000-03:00",
                },
            ],
        }
        result = extract_timeline_path(segment)
        assert result is not None
        assert len(result["points"]) == 2
        assert result["points"][0] == (
            45.7212835,
            5.0787821,
            "2015-12-15T18:26:00.000-03:00",
        )

    def test_empty_path(self):
        assert extract_timeline_path({"timelinePath": []}) is None

    def test_no_path_key(self):
        assert extract_timeline_path({"visit": {}}) is None


class TestExtractTrip:
    def test_basic_trip(self):
        segment = {
            "startTime": "2016-10-17T01:36:03.000-03:00",
            "endTime": "2016-10-18T09:29:10.000-03:00",
            "startTimeTimezoneUtcOffsetMinutes": 120,
            "endTimeTimezoneUtcOffsetMinutes": 120,
            "timelineMemory": {
                "trip": {
                    "distanceFromOriginKms": 445,
                    "destinations": [
                        {"identifier": {"placeId": "ChIJtyOAutvZVA0Rdh6B3PW9RFY"}}
                    ],
                }
            },
        }
        result = extract_trip(segment)
        assert result is not None
        assert result["distance_from_origin_kms"] == 445
        assert result["destination_place_ids"] == ["ChIJtyOAutvZVA0Rdh6B3PW9RFY"]

    def test_no_memory_key(self):
        assert extract_trip({"visit": {}}) is None


class TestExtractRawSignal:
    def test_basic_signal(self):
        signal = {
            "position": {
                "LatLng": "43.7108483°, 7.2934124°",
                "accuracyMeters": 100,
                "source": "UNKNOWN",
                "timestamp": "2026-01-31T21:48:11.000-03:00",
            }
        }
        result = extract_raw_signal(signal)
        assert result is not None
        assert result["lat"] == 43.7108483
        assert result["lng"] == 7.2934124
        assert result["accuracy_meters"] == 100

    def test_no_position(self):
        assert extract_raw_signal({}) is None
