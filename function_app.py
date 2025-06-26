import json
import logging

import azure.functions as func

from blob_storage.uploader import upload_to_blob
from shared.parser import parse_payload_data
from shared.utils import get_blob_folder
from shared.alerts import check_and_alert

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
    if not raw_data:
        return func.HttpResponse(
            json.dumps({"error": "Missing 'data' field in request body"}),
            status_code=400,
            mimetype="application/json",
        )

    parsed = parse_payload_data(raw_data)
    parsed["event"] = payload.get("event", "")
    parsed["published_at"] = payload.get("published_at", "")
    parsed["coreid"] = payload.get("coreid", "")

    try:
        check_and_alert(parsed)
    except Exception as e:
        logging.error(f"Failed to check and alert: {e}")
        pass

    try:
        blob_folder = get_blob_folder(parsed)
        upload_to_blob(parsed, blob_folder)
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
