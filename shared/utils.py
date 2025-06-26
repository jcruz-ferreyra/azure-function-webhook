from datetime import datetime


def parse_iso_datetime(dt_str: str) -> datetime:
    """Parses ISO 8601 with 'Z' or '+00:00' into a datetime object."""
    if dt_str.endswith("Z"):
        dt_str = dt_str.replace("Z", "+00:00")
    return datetime.fromisoformat(dt_str)
