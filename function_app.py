import json
import logging
import os

import azure.functions as func
from azure.storage.blob import BlobServiceClient

from blob_storage.uploader import upload_to_blob
from blob_storage.alert_log import upload_alert_log
from shared.alerts import check_and_alert
from shared.parser import parse_payload_data


CONNECTION_STRING = os.environ.get("BLOB_CONNECTION_STRING")

if not CONNECTION_STRING:
    raise ValueError("BLOB_CONNECTION_STRING environment variable is required")

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)


@app.route(route="webhook", methods=["POST"])
def webhook_handler(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Python HTTP trigger function processed a request.")

    try:
        payload = req.get_json()
    except ValueError:
        return func.HttpResponse(
            json.dumps({"error": "Invalid JSON"}),
            status_code=400,
            mimetype="application/json",
        )

    raw_data = payload.get("data")
    # if not raw_data:
    #     return func.HttpResponse(
    #         json.dumps({"error": "Missing 'data' field in request body"}),
    #         status_code=400,
    #         mimetype="application/json",
    #     )

    parsed = parse_payload_data(raw_data)
    parsed["event"] = payload.get("event", "")
    parsed["published_at"] = payload.get("published_at", "")
    parsed["coreid"] = payload.get("coreid", "")

    blob_client = BlobServiceClient.from_connection_string(CONNECTION_STRING)

    try:
        updated_alert_log = check_and_alert(parsed, blob_client)
    except Exception as e:
        logging.error(f"Failed to check and alert: {e}")
        pass

    if updated_alert_log:
        try:
            coreid = parsed.get("coreid", "no_coreid")
            upload_alert_log(updated_alert_log, coreid, blob_client)
        except Exception as e:
            logging.error(f"Failed to upload updated alert log: {e}")

    try:
        upload_to_blob(parsed, blob_client)
    except Exception as e:
        logging.exception("Failed to upload to blob.")
        return func.HttpResponse(
            json.dumps({"error": "Failed to upload to blob", "detail": str(e)}),
            status_code=500,
            mimetype="application/json",
        )

    return func.HttpResponse(
        json.dumps({"status": "received", "box_id": parsed.get("box_id", "unknown")}),
        status_code=200,
        mimetype="application/json",
    )
