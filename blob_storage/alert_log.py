import json
import logging
import os
from datetime import datetime, timedelta
from typing import Dict

import pytz
from azure.storage.blob import BlobServiceClient

from shared.utils import parse_iso_datetime

CONTAINER_NAME = os.environ.get("BLOB_CONTAINER_NAME", "sensor-data")
ALERT_EXPIRATION_MINUTES = int(os.environ.get("ALERT_EXPIRATION_MINUTES", 360))


def get_recent_alerts(coreid: str, blob_client: BlobServiceClient) -> Dict[str, str]:
    """
    Retrieve recent alerts sent for a given coreid from Blob Storage.
    Returns a dict {reason: timestamp} filtered to only include alerts
    within the last ALERT_EXPIRATION_MINUTES.
    """
    blob_path = f"alerts/{coreid}.json"
    result = {}

    try:
        blob = blob_client.get_blob_client(container=CONTAINER_NAME, blob=blob_path)
        if not blob.exists():
            return {}

        content = blob.download_blob().readall()
        alerts = json.loads(content)

        now = datetime.now(pytz.utc)
        for reason, timestamp in alerts.items():
            if not timestamp or not reason:
                continue

            try:
                dt = parse_iso_datetime(timestamp)
                if now - dt < timedelta(minutes=ALERT_EXPIRATION_MINUTES):
                    result[reason] = timestamp
            except ValueError:
                continue

    except Exception as e:
        logging.warning(f"Failed to load recent alerts for {coreid}: {e}")

    return result


def upload_alert_log(alert_log: Dict[str, str], coreid: str, blob_client: BlobServiceClient):
    """
    Uploads the updated alert log for a given device to Blob Storage.

    Args:
        coreid: Device ID used as the filename.
        alert_log: Dictionary with alert reasons and their timestamps.
        blob_client: An instance of BlobServiceClient.
    """
    blob_name = f"alerts/{coreid}.json"
    json_data = json.dumps(alert_log, indent=2)

    container_client = blob_client.get_container_client(CONTAINER_NAME)
    container_client.upload_blob(
        name=blob_name, data=json_data, overwrite=True, encoding="utf-8"
    )
