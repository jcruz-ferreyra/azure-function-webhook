"""
Uses azure-storage-blob to upload JSON to a container
Stores files with timestamped filenames
"""

import json
import logging
import os
from datetime import datetime
from typing import Dict, Any

import pytz
from azure.core.exceptions import ResourceExistsError
from azure.storage.blob import BlobServiceClient


CONTAINER_NAME = os.environ.get("BLOB_CONTAINER_NAME", "sensor-data")


def upload_to_blob(parsed: Dict[str, Any], blob_client: BlobServiceClient):
    """
    Uploads a parsed payload dictionary to Azure Blob Storage.

    This function converts the dictionary to JSON, generates a blob name from metadata,
    and stores it in the defined container, creating it if necessary.

    Args:
        parsed (Dict[str, Any]): Parsed payload containing sensor data and metadata.
        blob_client (BlobServiceClient): Azure BlobServiceClient instance for accessing storage.

    Returns:
        str: Full blob name used for storing the uploaded JSON data.

    Raises:
        ValueError: If the input is not a dictionary or JSON serialization fails.
        Exception: If any error occurs during upload to Azure Blob Storage.
    """
    # Validate input
    if not isinstance(parsed, dict):
        raise ValueError("Data must be a dictionary")

    # Generate filename
    blob_folder = _get_blob_folder(parsed)
    blob_name = _get_blob_name(parsed, blob_folder)

    # Convert to JSON
    try:
        json_data = json.dumps(parsed, indent=2)
    except (TypeError, ValueError) as e:
        raise ValueError(f"Data is not JSON serializable: {e}")

    # Upload to azure
    try:
        container_client = blob_client.get_container_client(CONTAINER_NAME)

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


def _get_blob_folder(parsed: Dict[str, Any]) -> str:
    """
    Generates the blob folder path based on the payload type and structure.

    Groups 'invalid' under 'unknown', and appends 'malformed/' if applicable.

    Args:
        parsed (Dict): Parsed payload dictionary.

    Returns:
        str: Folder path for blob storage (e.g., 'environment/malformed', 'unknown').
    """
    base_folder = parsed.get("datatype", "unknown")
    if base_folder == "invalid":
        base_folder = "unknown"

    if parsed.get("malformed") is True:
        return f"{base_folder}/malformed"

    return base_folder


def _get_blob_name(parsed: Dict[str, Any], blob_folder: str):
    """
    Generates a unique blob name based on the box or core ID and the current UTC timestamp.
    """
    # TODO
    # Map coreid to box_id using sensors_metadata table on the database
    # Maybe we can use the second function app to save that table into json on the blob
    # Maybe we can hardcode it in this app (no updates)
    # Maybe we can just leave the coreid for unparsed files (expect low number of them)
    upload_timestamp = datetime.now(pytz.utc).strftime("%Y%m%dT%H%M%SZ")
    box_id = parsed.get("box_id") or parsed.get("coreid", "unknown")

    return f"{blob_folder}/{box_id}_{upload_timestamp}.json"
