import re

_COORD_RE = re.compile(r"(-?\d+\.?\d*)[°]?,\s*(-?\d+\.?\d*)[°]?$")


def parse_latlng(coord: str | dict | None) -> tuple[float, float] | None:
    if coord is None:
        return None
    if isinstance(coord, dict):
        coord = coord.get("latLng") or coord.get("LatLng")
        if coord is None:
            return None
    m = _COORD_RE.match(str(coord))
    if not m:
        return None
    try:
        lat, lng = float(m.group(1)), float(m.group(2))
    except ValueError:
        return None
    if not (-90 <= lat <= 90 and -180 <= lng <= 180):
        return None
    return lat, lng


def extract_visit(segment: dict) -> dict | None:
    visit = segment.get("visit")
    if not visit:
        return None
    top = visit.get("topCandidate", {})
    place_id = top.get("placeId")
    if not place_id:
        return None
    coords = parse_latlng(top.get("placeLocation"))
    lat, lng = coords if coords else (None, None)
    return {
        "place_id": place_id,
        "lat": lat,
        "lng": lng,
        "semantic_type": top.get("semanticType"),
        "start_time": segment.get("startTime"),
        "end_time": segment.get("endTime"),
        "start_tz_offset_min": segment.get("startTimeTimezoneUtcOffsetMinutes"),
        "end_tz_offset_min": segment.get("endTimeTimezoneUtcOffsetMinutes"),
        "hierarchy_level": visit.get("hierarchyLevel"),
        "probability": visit.get("probability"),
        "place_probability": top.get("probability"),
    }


def extract_activity(segment: dict) -> dict | None:
    activity = segment.get("activity")
    if not activity:
        return None
    top = activity.get("topCandidate", {})
    start_coords = parse_latlng(activity.get("start"))
    end_coords = parse_latlng(activity.get("end"))
    start_lat, start_lng = start_coords if start_coords else (None, None)
    end_lat, end_lng = end_coords if end_coords else (None, None)
    return {
        "start_time": segment.get("startTime"),
        "end_time": segment.get("endTime"),
        "start_tz_offset_min": segment.get("startTimeTimezoneUtcOffsetMinutes"),
        "end_tz_offset_min": segment.get("endTimeTimezoneUtcOffsetMinutes"),
        "start_lat": start_lat,
        "start_lng": start_lng,
        "end_lat": end_lat,
        "end_lng": end_lng,
        "distance_meters": activity.get("distanceMeters"),
        "activity_type": top.get("type"),
        "probability": top.get("probability"),
    }


def extract_timeline_path(
    segment: dict,
) -> dict | None:
    path = segment.get("timelinePath")
    if not path:
        return None
    points = []
    for pt in path:
        coords = parse_latlng(pt.get("point"))
        if coords:
            points.append((coords[0], coords[1], pt.get("time")))
    if not points:
        return None
    return {
        "segment_start_time": segment.get("startTime"),
        "segment_end_time": segment.get("endTime"),
        "points": points,
    }


def extract_trip(segment: dict) -> dict | None:
    memory = segment.get("timelineMemory")
    if not memory:
        return None
    trip = memory.get("trip")
    if not trip:
        return None
    destinations = []
    for dest in trip.get("destinations", []):
        pid = dest.get("identifier", {}).get("placeId")
        if pid:
            destinations.append(pid)
    return {
        "start_time": segment.get("startTime"),
        "end_time": segment.get("endTime"),
        "start_tz_offset_min": segment.get("startTimeTimezoneUtcOffsetMinutes"),
        "end_tz_offset_min": segment.get("endTimeTimezoneUtcOffsetMinutes"),
        "distance_from_origin_kms": trip.get("distanceFromOriginKms"),
        "destination_place_ids": destinations,
    }


def extract_raw_signal(signal: dict) -> dict | None:
    pos = signal.get("position")
    if not pos:
        return None
    coords = parse_latlng(pos.get("LatLng") or pos.get("latLng"))
    if not coords:
        return None
    return {
        "lat": coords[0],
        "lng": coords[1],
        "timestamp": pos.get("timestamp"),
        "accuracy_meters": pos.get("accuracyMeters"),
        "source": pos.get("source"),
    }
