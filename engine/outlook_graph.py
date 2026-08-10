from __future__ import annotations

import base64
import hashlib
import json
import os
import secrets
from typing import Any, Dict
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


GRAPH_BASE = "https://graph.microsoft.com/v1.0"
DEFAULT_SCOPES = (
    "openid",
    "profile",
    "offline_access",
    "User.Read",
    "Mail.ReadWrite",
)


def graph_settings() -> Dict[str, Any]:
    tenant_id = os.getenv("GRAPH_TENANT_ID", "").strip()
    client_id = os.getenv("GRAPH_CLIENT_ID", "").strip()
    client_secret = os.getenv("GRAPH_CLIENT_SECRET", "")
    redirect_uri = os.getenv("GRAPH_REDIRECT_URI", "").strip()
    return {
        "tenant_id": tenant_id,
        "client_id": client_id,
        "client_secret": client_secret,
        "redirect_uri": redirect_uri,
        "configured": bool(
            tenant_id and client_id and client_secret and redirect_uri
        ),
    }


def _require_settings() -> Dict[str, Any]:
    settings = graph_settings()
    if not settings["configured"]:
        raise RuntimeError(
            "Microsoft Graph is not configured. Set GRAPH_TENANT_ID, "
            "GRAPH_CLIENT_ID, GRAPH_CLIENT_SECRET and GRAPH_REDIRECT_URI "
            "in Azure Function App environment variables."
        )
    return settings


def state_hash(state: str) -> str:
    return hashlib.sha256(str(state).encode("utf-8")).hexdigest()


def new_oauth_state() -> str:
    return secrets.token_urlsafe(48)


def authorization_url(state: str) -> str:
    settings = _require_settings()
    params = {
        "client_id": settings["client_id"],
        "response_type": "code",
        "redirect_uri": settings["redirect_uri"],
        "response_mode": "query",
        "scope": " ".join(DEFAULT_SCOPES),
        "state": state,
        "prompt": "select_account",
    }
    return (
        f"https://login.microsoftonline.com/{settings['tenant_id']}"
        f"/oauth2/v2.0/authorize?{urlencode(params)}"
    )


def _json_http(
    url: str,
    *,
    method: str = "GET",
    headers: Dict[str, str] | None = None,
    payload: Dict[str, Any] | None = None,
    form: Dict[str, str] | None = None,
) -> Dict[str, Any]:
    request_headers = dict(headers or {})
    data = None

    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        request_headers.setdefault("Content-Type", "application/json")
    elif form is not None:
        data = urlencode(form).encode("utf-8")
        request_headers.setdefault(
            "Content-Type",
            "application/x-www-form-urlencoded",
        )

    request = Request(
        url,
        data=data,
        headers=request_headers,
        method=method,
    )

    try:
        with urlopen(request, timeout=45) as response:
            raw = response.read().decode("utf-8")
            return json.loads(raw) if raw else {}
    except HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        try:
            details = json.loads(raw)
            error = details.get("error")
            message = (
                details.get("error_description")
                or (error.get("message") if isinstance(error, dict) else None)
                or raw
            )
        except Exception:
            message = raw or str(exc)
        raise RuntimeError(
            f"Microsoft Graph request failed ({exc.code}): {message}"
        ) from exc
    except URLError as exc:
        raise RuntimeError(
            f"Unable to reach Microsoft identity / Graph service: {exc}"
        ) from exc


def exchange_authorization_code(code: str) -> Dict[str, Any]:
    settings = _require_settings()
    if not code:
        raise ValueError("Microsoft authorization code is missing.")

    token_url = (
        f"https://login.microsoftonline.com/{settings['tenant_id']}"
        "/oauth2/v2.0/token"
    )
    result = _json_http(
        token_url,
        method="POST",
        form={
            "client_id": settings["client_id"],
            "client_secret": settings["client_secret"],
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": settings["redirect_uri"],
            "scope": " ".join(DEFAULT_SCOPES),
        },
    )
    if not result.get("access_token"):
        raise RuntimeError(
            "Microsoft sign-in completed without an access token."
        )
    return result


def graph_me(access_token: str) -> Dict[str, Any]:
    return _json_http(
        f"{GRAPH_BASE}/me?$select=mail,userPrincipalName,displayName",
        headers={"Authorization": f"Bearer {access_token}"},
    )


def create_draft_with_attachment(
    access_token: str,
    *,
    recipient: str,
    subject: str,
    body: str,
    attachment_bytes: bytes,
    attachment_name: str,
    attachment_content_type: str,
) -> Dict[str, Any]:
    if len(attachment_bytes) >= 3 * 1024 * 1024:
        raise ValueError(
            "The generated discrepancy report is larger than the 3 MB "
            "Microsoft Graph simple-attachment limit. Select fewer rows."
        )

    auth_headers = {"Authorization": f"Bearer {access_token}"}
    message = _json_http(
        f"{GRAPH_BASE}/me/messages",
        method="POST",
        headers=auth_headers,
        payload={
            "subject": str(subject or "").strip(),
            "body": {
                "contentType": "Text",
                "content": str(body or ""),
            },
            "toRecipients": [
                {
                    "emailAddress": {
                        "address": str(recipient or "").strip(),
                    }
                }
            ],
        },
    )

    message_id = str(message.get("id") or "")
    if not message_id:
        raise RuntimeError("Microsoft Graph did not return the draft message ID.")

    _json_http(
        f"{GRAPH_BASE}/me/messages/{message_id}/attachments",
        method="POST",
        headers=auth_headers,
        payload={
            "@odata.type": "#microsoft.graph.fileAttachment",
            "name": attachment_name,
            "contentType": attachment_content_type
            or "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            "contentBytes": base64.b64encode(attachment_bytes).decode("ascii"),
        },
    )

    web_link = str(message.get("webLink") or "")
    if not web_link:
        refreshed = _json_http(
            f"{GRAPH_BASE}/me/messages/{message_id}?$select=webLink",
            headers=auth_headers,
        )
        web_link = str(refreshed.get("webLink") or "")

    if not web_link:
        raise RuntimeError(
            "The Outlook draft was created, but Microsoft Graph did not "
            "return a web link for opening it."
        )

    return {
        "message_id": message_id,
        "web_link": web_link,
    }
