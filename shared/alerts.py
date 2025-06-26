import logging
import os
import smtplib
from datetime import datetime
from email.message import EmailMessage
from typing import Any, Dict

import pytz
from azure.storage.blob import BlobServiceClient

from blob_storage.alert_log import get_recent_alerts
from shared.utils import parse_iso_datetime


EMAIL_HOST = os.getenv("EMAIL_HOST")
EMAIL_PORT = int(os.getenv("EMAIL_PORT", "587"))
EMAIL_SENDER = os.getenv("EMAIL_SENDER")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
EMAIL_RECIPIENT_DEVELOP = os.getenv("EMAIL_RECIPIENT_DEVELOP")
EMAIL_RECIPIENT_DEPLOY = os.getenv("EMAIL_RECIPIENT_DEPLOY")

MAX_LATENCY_MINUTES = 30

SUBJECT_PREFFIX = "[SENSOR DATA ALERT TRIGGERED] "


def check_and_alert(parsed: Dict[str, Any], blob_client: BlobServiceClient):
    """
    Checks the parsed dictionary and triggers alerts based on its content.
    Extend this logic to add new alert conditions.

    Returns:
    dict[str, str] if any alerts were triggered (reason → timestamp), else None.
    """
    alerts_triggered = False

    # If no coreid in the message send an alert to deployment
    coreid = parsed.get("coreid", "no_coreid")
    if coreid == "no_coreid":
        _send_alert_email(
            subject=SUBJECT_PREFFIX + "No coreid in the incoming sensor data",
            body=_compose_body(parsed, alert={}),
            recipient=EMAIL_RECIPIENT_DEVELOP,
        )

    # Deploy team checks
    deploy_alert = None
    latency = None

    deploy_checks = [_check_invalid, _check_error]
    for check in deploy_checks:
        deploy_alert = check(parsed)  # alert: dict if triggered else None
        if deploy_alert:
            break  # alerts ordered by priority

    latency = _check_latency(parsed)  # latency: dict if triggered else None

    if deploy_alert and latency:
        deploy_alert["latency"] = latency["summary"]
    elif not deploy_alert and latency:
        deploy_alert = latency

    # Develop team checks
    develop_alert = None

    develop_checks = [_check_unknown, _check_malformed]
    for check in develop_checks:
        develop_alert = check(parsed)
        if develop_alert:
            break

    # Exit if not alerts triggered
    if not deploy_alert and not develop_alert:
        return None

    # Check if current device triggered an alert for the same reason recently.
    # Note that the checks to deduplicate alerts are based on device's coreid.
    recent_alerts = get_recent_alerts(coreid, blob_client)

    # Send an alert email if no alert email was sent recently
    for alert, recipient in [
        (deploy_alert, EMAIL_RECIPIENT_DEPLOY),
        (develop_alert, EMAIL_RECIPIENT_DEVELOP),
    ]:
        if alert:
            reason = alert.get("reason")  # reason must be defined in the alert
            if reason and reason not in recent_alerts:
                _send_alert_email(
                    subject=SUBJECT_PREFFIX + alert.get("subject", "No subject"),
                    body=_compose_body(parsed, alert=alert),
                    recipient=recipient,
                )
                recent_alerts[reason] = datetime.now(pytz.utc).isoformat()
                alerts_triggered = True
            else:
                logging.warning(
                    f"Alert not sent for reason '{reason}' on coreid '{coreid}' "
                    "because it was already triggered recently."
                )

    return recent_alerts if alerts_triggered else None


def _check_invalid(parsed):
    datatype = parsed.get("datatype")
    if datatype == "invalid":
        alert_subject = "Invalid data received"
        alert_summary = (
            "Invalid data received: 'data' field must contain a non-empty string."
        )

        return {
            "reason": datatype,
            "subject": alert_subject,
            "summary": alert_summary,
        }

    return None


def _check_unknown(parsed):
    datatype = parsed.get("datatype")
    if datatype == "unknown":
        alert_subject = "Unrecognized data format received"
        alert_summary = "Unrecognized data format: does not match expected patterns for sensor readings, error logs, or startup messages."

        return {
            "reason": datatype,
            "subject": alert_subject,
            "summary": alert_summary,
        }

    return None


def _check_error(parsed):
    datatype = parsed.get("datatype")
    if datatype == "error":
        box_id = parsed.get("box_id", "unknown")
        error_code = parsed.get("error_code", "E")
        alert_subject = f"Error {error_code} detected in Box {box_id}"

        return {
            "reason": error_code,
            "subject": alert_subject,
            "summary": alert_subject,
        }

    return None


def _check_malformed(parsed):
    if parsed.get("malformed") is True:
        datatype = parsed.get("datatype")
        parsing_error = parsed.get(
            "parsing_error",
            f"Data does not match the expected pattern for type: {datatype}.",
        )
        alert_subject = f"Malformed {datatype} data received"
        alert_summary = f"Parsing error occurred. {parsing_error}"

        return {
            "reason": "malformed",
            "subject": alert_subject,
            "summary": alert_summary,
        }

    return None


def _check_latency(parsed):
    if parsed.get("timestamp") and parsed.get("published_at"):
        try:
            published_at = parse_iso_datetime(parsed["published_at"])
            timestamp = parse_iso_datetime(parsed["timestamp"])
            latency_minutes = (published_at - timestamp).total_seconds() / 60

            if latency_minutes > MAX_LATENCY_MINUTES:
                box_id = parsed.get("box_id", "unknown")
                alert_subject = f"High latency in Box {box_id}"
                alert_summary = f"High transmission latency: {latency_minutes:.1f} minutes (threshold: {MAX_LATENCY_MINUTES}m)"

                return {
                    "reason": "latency",
                    "subject": alert_subject,
                    "summary": alert_summary,
                }
        except Exception as e:
            logging.warning(f"Latency check failed: {e}")

    return None


def _send_alert_email(subject: str, body: str, recipient: str):
    """Internal helper to send email via SMTP."""
    try:
        msg = EmailMessage()
        msg["From"] = EMAIL_SENDER
        msg["To"] = recipient
        msg["Subject"] = subject
        msg.set_content(body)

        with smtplib.SMTP(EMAIL_HOST, EMAIL_PORT) as smtp:
            smtp.ehlo()
            smtp.starttls()
            smtp.login(EMAIL_SENDER, EMAIL_PASSWORD)
            smtp.send_message(msg)

        logging.info(f"Alert email sent: {subject}")
    except Exception as e:
        logging.error(f"Failed to send alert email: {e}")


def _compose_body(parsed: Dict[str, Any], alert: Dict[str, str]) -> str:
    """Create an informative body for the alert message."""
    summary = alert.get("summary", "")
    latency = "\n" + alert.get("latency") if alert.get("latency") else ""

    return f"""{SUBJECT_PREFFIX}
{summary}{latency}

Box ID: {parsed.get("box_id", "unknown")}
Core ID: {parsed.get("coreid", "N/A")}
Published_at: {parsed.get("published_at", "N/A")}
Parsed_at: {parsed.get("parsed_at", "N/A")}
Data: {parsed.get("raw", "N/A")}

Please investigate the issue.
"""
