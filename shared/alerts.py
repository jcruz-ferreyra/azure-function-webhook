import smtplib
from email.message import EmailMessage
import os
import logging

from shared.utils import parse_iso_datetime

EMAIL_HOST = os.getenv("EMAIL_HOST")
EMAIL_PORT = int(os.getenv("EMAIL_PORT", "587"))
EMAIL_SENDER = os.getenv("EMAIL_SENDER")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
EMAIL_RECIPIENT_DEVELOP = os.getenv("EMAIL_RECIPIENT_DEVELOP")
EMAIL_RECIPIENT_DEPLOY = os.getenv("EMAIL_RECIPIENT_DEPLOY")

MAX_LATENCY_MINUTES = 30

subject_preffix = "[Sensor data alert triggered]"


def check_and_alert(parsed: dict):
    """
    Checks the parsed dictionary and triggers alerts based on its content.
    Extend this logic to add new alert conditions.
    """
    datatype = parsed.get("datatype", "unknown")

    # Deploy team alerts
    if datatype == "invalid_input":
        summary = "Invalid data received. Data field must contain a non-empty string"
        _send_alert_email(
            subject=f"{subject_preffix} Invalid data received",
            body=_compose_body(parsed, summary=summary),
            recipient=EMAIL_RECIPIENT_DEPLOY,
        )
    elif datatype == "error":
        box_id = parsed.get("box_id", "unknown")
        error_code = parsed.get(
            "error_code", "[Unknown error: please check raw data below]"
        )
        summary = f"Error {error_code} detected in Box {box_id}"
        _send_alert_email(
            subject=f"{subject_preffix} {summary}",
            body=_compose_body(parsed, summary=summary),
            recipient=EMAIL_RECIPIENT_DEPLOY,
        )
    elif parsed.get("timestamp") and parsed.get("published_at"):
        try:
            published_at = parse_iso_datetime(parsed["published_at"])
            timestamp = parse_iso_datetime(parsed["timestamp"])
            latency_minutes = (published_at - timestamp).total_seconds() / 60

            if latency_minutes > MAX_LATENCY_MINUTES:
                box_id = parsed.get("box_id", "unknown")
                summary = f"High transmission latency: {latency_minutes:.1f} minutes (threshold: {MAX_LATENCY_MINUTES}m)"
                _send_alert_email(
                    subject=f"{subject_preffix} High latency in Box {box_id}",
                    body=_compose_body(parsed, summary=summary),
                    recipient=EMAIL_RECIPIENT_DEPLOY,
                )
        except Exception as e:
            logging.debug(f"Latency check failed: {e}")

    # Develop team alerts
    if parsed.get("malformed") is True:
        parsing_error = parsed.get(
            "parsing_error", f"Data does not match expected {datatype} data pattern"
        )
        summary = f"Parsing error occurred. {parsing_error}"
        _send_alert_email(
            subject=f"{subject_preffix} Malformed {datatype} data received",
            body=_compose_body(parsed, summary=summary),
            recipient=EMAIL_RECIPIENT_DEVELOP,
        )
    elif datatype == "unknown":
        summary = "Unrecognized data received. Data format does not match expected patterns for environment readings, error logs, or startup messages."
        _send_alert_email(
            subject=f"{subject_preffix} Unrecognized data received",
            body=_compose_body(parsed, summary=summary),
            recipient=EMAIL_RECIPIENT_DEVELOP,
        )


def _check_and_alert_invalid(parsed):
    datatype = parsed.get("datatype")
    if datatype == "invalid":
        summary = "Invalid data received. Data field must contain a non-empty string"
        _send_alert_email(
            subject=f"{subject_preffix} Invalid data received",
            body=_compose_body(parsed, summary=summary),
            recipient=EMAIL_RECIPIENT_DEPLOY,
        )

        return datatype

    return False


def _check_and_alert_error(parsed):
    datatype = parsed.get("datatype")
    if datatype == "error":
        box_id = parsed.get("box_id", "unknown")
        error_code = parsed.get("error_code", "E")
        summary = f"Error {error_code} detected in Box {box_id}"
        _send_alert_email(
            subject=f"{subject_preffix} {summary}",
            body=_compose_body(parsed, summary=summary),
            recipient=EMAIL_RECIPIENT_DEPLOY,
        )

        return error_code

    return False


def _check_and_alert_latency(parsed):
    if parsed.get("timestamp") and parsed.get("published_at"):
        try:
            published_at = parse_iso_datetime(parsed["published_at"])
            timestamp = parse_iso_datetime(parsed["timestamp"])
            latency_minutes = (published_at - timestamp).total_seconds() / 60

            if latency_minutes > MAX_LATENCY_MINUTES:
                box_id = parsed.get("box_id", "unknown")
                summary = f"High transmission latency: {latency_minutes:.1f} minutes (threshold: {MAX_LATENCY_MINUTES}m)"
                _send_alert_email(
                    subject=f"{subject_preffix} High latency in Box {box_id}",
                    body=_compose_body(parsed, summary=summary),
                    recipient=EMAIL_RECIPIENT_DEPLOY,
                )

                return "latency"
        except Exception as e:
            logging.debug(f"Latency check failed: {e}")

    return False


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


def _compose_body(parsed: dict, summary: str) -> str:
    """Create an informative body for the alert message."""
    return f"""{subject_preffix}
{summary}

Box ID: {parsed.get("box_id", "unknown")}
Core ID: {parsed.get("coreid", "N/A")}
Published_at: {parsed.get("published_at", "N/A")}
Parsed_at: {parsed.get("parsed_at", "N/A")}
Data: {parsed.get("raw", "N/A")}

Please investigate the issue.
"""
