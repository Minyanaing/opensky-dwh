"""Small, shared helpers for tagging and deduplicating raw OpenSky flight records."""

from datetime import datetime, timezone


def utc_now_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def tag_record(flight, queried_airport, movement_type, fetched_at):
    """Attach ingestion metadata to a raw OpenSky Flight record, without altering its fields."""
    tagged = dict(flight)
    tagged["queried_airport"] = queried_airport
    tagged["movement_type"] = movement_type
    tagged["fetched_at"] = fetched_at
    return tagged


def _dedup_key(record):
    return (
        record.get("icao24"),
        record.get("movement_type"),
        record.get("queried_airport"),
        record.get("firstSeen"),
    )


def dedupe(records):
    """Drop exact repeats caused by overlapping API call windows, keeping first-seen order."""
    seen = set()
    deduped = []
    for record in records:
        key = _dedup_key(record)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(record)
    return deduped
