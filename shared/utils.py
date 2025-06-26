import os
from datetime import datetime
from typing import Dict


def get_blob_folder(parsed: Dict) -> str:
    """
    Determines the appropriate blob folder for the parsed payload.

    - Groups 'invalid' under 'unknown'
    - Adds 'malformed/' subfolder if the 'malformed' flag is True

    Args:
        parsed (Dict): Parsed payload dictionary

    Returns:
        str: Blob folder path (e.g., 'sensor', 'sensor/malformed', 'unknown', etc.)
    """
    base_folder = parsed.get("datatype", "unknown")
    if base_folder == "invalid":
        base_folder = "unknown"

    if parsed.get("malformed") is True:
        return os.path.join(base_folder, "malformed")

    return base_folder


def parse_iso_datetime(dt_str: str) -> datetime:
    """Parses ISO 8601 with 'Z' or '+00:00' into a datetime object."""
    if dt_str.endswith("Z"):
        dt_str = dt_str.replace("Z", "+00:00")
    return datetime.fromisoformat(dt_str)
