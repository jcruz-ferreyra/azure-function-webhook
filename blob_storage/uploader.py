"""
Uses azure-storage-blob to upload JSON to a container
Stores files with timestamped filenames
"""

import json
import logging
import os
from datetime import datetime

import pytz
from azure.core.exceptions import ResourceExistsError
from azure.storage.blob import BlobServiceClient

CONNECTION_STRING = os.environ.get("BLOB_CONNECTION_STRING")
CONTAINER_NAME = os.environ.get("BLOB_CONTAINER_NAME", "sensor-data")

if not CONNECTION_STRING:
    raise ValueError("BLOB_CONNECTION_STRING environment variable is required")


def upload_to_blob(data: dict, blob_folder: str):
    """
    Upload data to blob storage (local or Azure).

    Args:
        data: Dictionary to upload as JSON

    Returns:
        str: File path (local mode) or blob name (Azure mode)

    Raises:
        ValueError: If data is not JSON serializable
        OSError: If local file operations fail
        Exception: If Azure upload fails
    """
    # Validate input
    if not isinstance(data, dict):
        raise ValueError("Data must be a dictionary")

    # Generate filename
    upload_timestamp = datetime.now(pytz.utc).strftime("%Y%m%dT%H%M%SZ")
    box_id = data.get("box_id", "unknown")
    blob_name = f"{blob_folder}/{box_id}_{upload_timestamp}.json"

    # Convert to JSON
    try:
        json_data = json.dumps(data, indent=2)
    except (TypeError, ValueError) as e:
        raise ValueError(f"Data is not JSON serializable: {e}")

    # Upload to azure
    try:
        blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STRING)
        container_client = blob_service_client.get_container_client(CONTAINER_NAME)

        # Ensure container exists (create if needed)
        try:
            container_client.create_container()
        except ResourceExistsError:
            pass  # Container already exists, which is fine
        except Exception as e:
            print(f"Warning: Could not create container: {e}")

        # Upload blob
        container_client.upload_blob(
            name=blob_name, data=json_data, overwrite=True, encoding="utf-8"
        )
        logging.info(f"Uploaded to blob: {blob_name}")
        return blob_name

    except Exception as e:
        logging.error(f"Error uploading to Azure Blob: {e}")
        raise
