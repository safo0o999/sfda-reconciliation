from __future__ import annotations

import os
import smtplib
from email.message import EmailMessage
from email.utils import parseaddr
from typing import Optional


def _env(primary: str, fallback: str = "") -> str:
    value = os.getenv(primary, "").strip()
    if value:
        return value
    return os.getenv(fallback, "").strip() if fallback else ""


def email_settings() -> dict:
    host = _env("SMTP_HOST", "VARIANCE_SMTP_HOST")
    sender = _env("SMTP_EMAIL_FROM", "VARIANCE_EMAIL_FROM")
    username = _env("SMTP_USERNAME", "VARIANCE_SMTP_USERNAME")
    password = os.getenv("SMTP_PASSWORD", "") or os.getenv("VARIANCE_SMTP_PASSWORD", "")
    port = int(_env("SMTP_PORT", "VARIANCE_SMTP_PORT") or "587")
    use_ssl = (_env("SMTP_SSL", "VARIANCE_SMTP_SSL") or "false").lower() in {"1", "true", "yes", "on"}
    use_tls = (_env("SMTP_STARTTLS", "VARIANCE_SMTP_STARTTLS") or "true").lower() in {"1", "true", "yes", "on"}
    return {
        "host": host,
        "port": port,
        "username": username,
        "password": password,
        "sender": sender,
        "use_ssl": use_ssl,
        "use_tls": use_tls,
        "configured": bool(host and sender),
    }


def validate_email(address: str) -> str:
    address = str(address or "").strip()
    parsed = parseaddr(address)[1]
    if not parsed or "@" not in parsed:
        raise ValueError("A valid email address is required.")
    return parsed


def send_email(
    recipient: str,
    subject: str,
    body: str,
    *,
    attachment_bytes: Optional[bytes] = None,
    attachment_name: str = "",
    attachment_content_type: str = "application/octet-stream",
) -> None:
    settings = email_settings()
    if not settings["configured"]:
        raise RuntimeError(
            "Email service is not configured. Set SMTP_HOST and SMTP_EMAIL_FROM "
            "(or the existing VARIANCE_SMTP_HOST / VARIANCE_EMAIL_FROM settings) "
            "in Azure Function App environment variables."
        )

    recipient = validate_email(recipient)
    message = EmailMessage()
    message["From"] = settings["sender"]
    message["To"] = recipient
    message["Subject"] = str(subject or "").strip()
    message.set_content(str(body or ""))

    if attachment_bytes is not None:
        content_type = attachment_content_type or "application/octet-stream"
        parts = content_type.split("/", 1)
        maintype = parts[0] if parts else "application"
        subtype = parts[1] if len(parts) > 1 else "octet-stream"
        message.add_attachment(
            attachment_bytes,
            maintype=maintype,
            subtype=subtype,
            filename=attachment_name or "attachment.bin",
        )

    smtp_class = smtplib.SMTP_SSL if settings["use_ssl"] else smtplib.SMTP
    with smtp_class(settings["host"], settings["port"], timeout=30) as server:
        if not settings["use_ssl"] and settings["use_tls"]:
            server.starttls()
        if settings["username"]:
            server.login(settings["username"], settings["password"])
        server.send_message(message)
