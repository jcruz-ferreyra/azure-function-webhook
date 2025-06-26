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
    Retrieves recent alert logs for a given sensor core ID from Blob Storage.

    This function loads the alert history file stored at 'alerts/{coreid}.json',
    which contains a dictionary mapping alert reasons to their latest timestamps.
    It filters out entries that are older than the expiration threshold defined by
    the ALERT_EXPIRATION_MINUTES environment variable.

    Args:
        coreid (str): The sensor device core ID used to locate the alert log file.
        blob_client (BlobServiceClient): Azure BlobServiceClient used to access blob storage.

    Returns:
        Dict[str, str]: Filtered alert dictionary with recent (non-expired) alerts.
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


def upload_alert_log(alert_log: Dict[str, str], coreid: str, blob_client: BlobServiceClient) -> None:
    """
    Uploads the updated alert log for a given sensor core ID to Blob Storage.

    This function stores the alert history as a JSON file at 'alerts/{coreid}.json',
    containing a dictionary that maps alert reasons to their most recent timestamps.
    If a log already exists, it is overwritten with the new contents.

    Args:
        alert_log (Dict[str, str]): Dictionary of alert reasons and their timestamps.
        coreid (str): The sensor device core ID used as the log file name.
        blob_client (BlobServiceClient): Azure BlobServiceClient used to upload the log.
    """
    blob_name = f"alerts/{coreid}.json"
    json_data = json.dumps(alert_log, indent=2)

    container_client = blob_client.get_container_client(CONTAINER_NAME)
    container_client.upload_blob(
        name=blob_name, data=json_data, overwrite=True, encoding="utf-8"
    )
