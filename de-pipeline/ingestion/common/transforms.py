"""Small, shared helpers for tagging and deduplicating raw OpenSky flight records."""

import csv
from datetime import datetime, timezone
from pathlib import Path


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


def distinct_callsigns(records):
    """Distinct, whitespace-stripped callsigns seen across a batch of tagged flight records."""
    callsigns = {record["callsign"].strip() for record in records if record.get("callsign", "").strip()}
    return sorted(callsigns)


def distinct_airports(records):
    """Distinct, whitespace-stripped ICAO codes from both ends of every flight, not just the
    queried airport."""
    airports = set()
    for record in records:
        for field in ("estDepartureAirport", "estArrivalAirport"):
            value = (record.get(field) or "").strip()
            if value:
                airports.add(value)
    return sorted(airports)


def read_column(path, column):
    """Reads a single-column CSV's values. Tolerant of a missing header row (treats the first
    line as data unless it's literally the column name) - a DictReader-based read would
    otherwise silently treat that first value as the header and return nothing at all."""
    path = Path(path)
    if not path.is_file():
        return []
    with open(path, newline="", encoding="utf-8") as f:
        rows = [row[0].strip() for row in csv.reader(f) if row]
    if rows and rows[0] == column:
        rows = rows[1:]
    return rows


def append_column(source_path, dest_path, column):
    """Append source_path's values onto dest_path, creating dest_path (with header) if needed."""
    values = read_column(source_path, column)
    if not values:
        return
    dest_path = Path(dest_path)
    write_header = not dest_path.is_file()
    with open(dest_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if write_header:
            writer.writerow([column])
        for value in values:
            writer.writerow([value])


def write_column_csv(file_path, header, values):
    with open(file_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        for value in values:
            writer.writerow([value])


def export_new_only(current_values, old_path, out_path, column):
    """Compare current_values against everything already in old_path; overwrite out_path with
    only the ones not already known."""
    existing = set(read_column(old_path, column))
    new_values = sorted({v for v in current_values if v} - existing)
    write_column_csv(out_path, [column], new_values)
    return new_values
