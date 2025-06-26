import re
from datetime import datetime, timezone
from typing import Dict

import pytz


def parse_payload_data(raw: str) -> Dict:
    if not raw or not isinstance(raw, str):
        return _get_base_dict("invalid", raw)

    raw = re.sub(r"\s+", " ", raw.strip())

    # Case 1: startup message
    match = re.match(r"^([\S]*?)(\d{6}) (\d{1,2}):(\d{2}) LTE Setup Done", raw)
    if match:
        return _parse_startup_message(raw, match)

    # Case 2: error message
    match = re.match(r"^([\S]*?)(\d{6}) (\d{1,2}):(\d{2}) (E\d+)$", raw)
    if match:
        return _parse_error_message(raw, match)

    # Case 3: sensor data (starts with a comma and has 50+ values)
    parts = [part.strip() for part in raw.split(",")]
    parts = parts[1:] if raw.startswith(",") else parts
    if len(parts) >= 9 and all(re.match(r'^\d*$', p) for p in parts[1:]):  # all must be numbers except boxid (parts[0])
        return _parse_environment_data(raw, parts)

    # Unknown format
    return _get_base_dict("unknown", raw)


def _parse_environment_data(raw: str, parts: list) -> Dict:
    base_dict = _get_base_dict("environment", raw)
    base_dict["box_id"] = parts[0]

    try:
        if not raw.startswith(","):
            raise ValueError("Invalid environment format: no leading comma")

        # Parse metadata fields: Box ID and timestamp components
        month = int(parts[1])
        day = int(parts[2])
        hour = int(parts[3])
        minute = int(parts[4])
        readings = parts[5:]

        # Estimate readings year based on current time and month difference
        now = datetime.now(pytz.utc)
        year = now.year

        # Handle edge case where readings month crosses year boundary (e.g., Dec vs Jan)
        if now.month == 1 and month == 12:
            year -= 1
        elif now.month == 12 and month == 1:
            year += 1

        # Create reading timestamp
        timestamp = _get_utc_timestamp(year, month, day, hour, minute)

        # Group the remaining values into triplets: T, RH, Noise
        if len(readings) % 3 != 0:
            raise ValueError("Invalid environment format: number of readings non divisible by 3 (T, RH, Noise)")

        triples = [
            {
                "T": int(readings[i]),
                "RH": int(readings[i + 1]),
                "Noise": int(readings[i + 2]),
            }
            for i in range(0, len(readings), 3)
        ]

        return base_dict | {"timestamp": timestamp, "readings": triples}

    except Exception as e:
        return base_dict | _get_error_dict(e)


def _parse_error_message(raw: str, match: re.Match) -> Dict:
    base_dict = _get_base_dict("error", raw)
    base_dict["box_id"] = match.group(1)
    base_dict["error_code"] = match.group(5)

    try:
        date = match.group(2)
        hour = int(match.group(3))
        minute = int(match.group(4))

        timestamp = _parse_datetime(date, hour, minute)

        return base_dict | {"timestamp": timestamp}

    except Exception as e:
        return base_dict | _get_error_dict(e)


def _parse_startup_message(raw: str, match: re.Match) -> Dict:
    base_dict = _get_base_dict("startup", raw)
    base_dict["box_id"] = match.group(1)

    try:
        date = match.group(2)
        hour = int(match.group(3))
        minute = int(match.group(4))

        timestamp = _parse_datetime(date, hour, minute)

        return base_dict | {"timestamp": timestamp}

    except Exception as e:
        return base_dict | _get_error_dict(e)


def _parse_datetime(box_date: str, hour: int, minute: int) -> str:
    year = 2000 + int(box_date[:2])
    month = int(box_date[2:4])
    day = int(box_date[4:6])

    timestamp = _get_utc_timestamp(year, month, day, hour, minute)

    return timestamp


def _get_base_dict(datatype, raw, parser_version=1.0):
    return {
        "datatype": datatype,
        "raw": raw,
        "parsed_at": datetime.now(pytz.utc).isoformat(),
        "parser_version": parser_version,
    }


def _get_error_dict(e):
    return {"malformed": True, "parsing_error": str(e)}


def _get_utc_timestamp(year, month, day, hour, minute):
    return datetime(year, month, day, hour, minute, tzinfo=timezone.utc).isoformat()
