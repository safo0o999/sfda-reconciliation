from __future__ import annotations

import base64
import hashlib
import hmac
import os
import secrets
import threading
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, Tuple

from engine.database import (
    initialize_database,
    approve_user_by_token_hash,
    count_active_admins,
    create_auth_session,
    create_pending_user_record,
    delete_auth_session,
    find_user_by_email,
    get_auth_session_user,
    list_auth_users,
    set_user_status,
    get_or_create_warehouse,
    get_madinah_warehouse,
    list_warehouses,
)
from engine.email_service import email_settings, send_email




_AUTH_SCHEMA_READY = False
_AUTH_SCHEMA_LOCK = threading.Lock()


def ensure_auth_schema() -> None:
    global _AUTH_SCHEMA_READY
    if _AUTH_SCHEMA_READY:
        return
    with _AUTH_SCHEMA_LOCK:
        if _AUTH_SCHEMA_READY:
            return
        initialize_database()
        _AUTH_SCHEMA_READY = True

PBKDF2_ITERATIONS = 310_000
SESSION_HOURS = 12
APPROVAL_HOURS = 48


def allowed_domain() -> str:
    return (os.getenv("ALLOWED_USER_DOMAIN", "nupco.com").strip().lower().lstrip("@") or "nupco.com")


def normalize_email(value: str) -> str:
    return str(value or "").strip().lower()


def validate_corporate_email(value: str) -> str:
    email = normalize_email(value)
    domain = allowed_domain()
    if not email or "@" not in email or not email.endswith("@" + domain):
        raise ValueError(f"User name must be a company email ending with @{domain}.")
    return email


def validate_password(password: str) -> str:
    password = str(password or "")
    if len(password) < 10:
        raise ValueError("Password must contain at least 10 characters.")
    if not any(ch.isalpha() for ch in password) or not any(ch.isdigit() for ch in password):
        raise ValueError("Password must contain at least one letter and one number.")
    return password


def _password_hash(password: str, salt: Optional[bytes] = None) -> Tuple[str, str]:
    salt = salt or secrets.token_bytes(16)
    digest = hashlib.pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        salt,
        PBKDF2_ITERATIONS,
    )
    return base64.b64encode(salt).decode("ascii"), base64.b64encode(digest).decode("ascii")


def verify_password(password: str, salt_b64: str, hash_b64: str) -> bool:
    try:
        salt = base64.b64decode(salt_b64)
    except Exception:
        return False
    _, candidate = _password_hash(password, salt)
    return hmac.compare_digest(candidate, str(hash_b64 or ""))


def _token_hash(token: str) -> str:
    return hashlib.sha256(str(token).encode("utf-8")).hexdigest()


def register_user(email: str, password: str, warehouse_name: str, base_url: str) -> Dict[str, Any]:
    ensure_auth_schema()
    email = validate_corporate_email(email)
    password = validate_password(password)

    existing = find_user_by_email(email)
    if existing and str(existing.get("Status", "")).lower() == "active":
        raise ValueError("A user with this email already exists and is active.")

    salt_b64, password_hash = _password_hash(password)
    approval_token = secrets.token_urlsafe(48)
    approval_hash = _token_hash(approval_token)
    approval_expires = datetime.now(timezone.utc) + timedelta(hours=APPROVAL_HOURS)

    first_admin = count_active_admins() == 0
    if first_admin:
        warehouse = get_madinah_warehouse()
        role = "Admin"
        status = "Active"
        requested_warehouse_name = str(warehouse.get("WarehouseName") or "Madinah Warehouse")
    else:
        requested_warehouse_name = " ".join(str(warehouse_name or "").strip().split())
        if not requested_warehouse_name:
            raise ValueError("Warehouse name is required.")
        warehouse = get_or_create_warehouse(requested_warehouse_name)
        role = "User"
        status = "Pending"

    user = create_pending_user_record(
        email=email,
        password_salt=salt_b64,
        password_hash=password_hash,
        role=role,
        approval_token_hash=approval_hash,
        approval_expires_at=approval_expires,
        warehouse_id=int(warehouse["WarehouseID"]),
        requested_warehouse_name=requested_warehouse_name,
        status=status,
    )

    if first_admin:
        return {
            "user": user,
            "first_admin": True,
            "approval_email_sent": False,
            "approval_email_error": "",
        }

    admin_email = normalize_email(os.getenv("USER_APPROVAL_ADMIN_EMAIL", ""))
    mail_sent = False
    mail_error = ""
    if admin_email:
        approve_url = f"{base_url.rstrip('/')}/api/auth/approve?token={approval_token}"
        body = (
            "A new SFDA Reconciliation user is requesting access.\n\n"
            f"User: {email}\n"
            f"Requested role: {role}\n"
            f"Requested warehouse: {requested_warehouse_name}\n\n"
            "Approve this request using the secure link below:\n"
            f"{approve_url}\n\n"
            f"The link expires in {APPROVAL_HOURS} hours."
        )
        try:
            send_email(
                admin_email,
                "SFDA Reconciliation - New User Approval",
                body,
            )
            mail_sent = True
        except Exception as exc:
            mail_error = str(exc)
    else:
        mail_error = "USER_APPROVAL_ADMIN_EMAIL is not configured."

    return {
        "user": user,
        "approval_email_sent": mail_sent,
        "approval_email_error": mail_error,
    }


def approve_registration(token: str) -> Dict[str, Any]:
    ensure_auth_schema()
    if not token:
        raise ValueError("Approval token is required.")
    user = approve_user_by_token_hash(_token_hash(token))
    if not user:
        raise ValueError("Approval link is invalid, expired, or already used.")

    # Notify the user after approval when SMTP is available.
    try:
        send_email(
            str(user.get("Email") or ""),
            "SFDA Reconciliation - Access Approved",
            "Your SFDA Reconciliation account has been approved. You can now sign in.",
        )
    except Exception:
        pass
    return user


def login_user(email: str, password: str) -> Tuple[Dict[str, Any], str, datetime]:
    ensure_auth_schema()
    email = validate_corporate_email(email)
    user = find_user_by_email(email)
    if not user or str(user.get("Status", "")).lower() != "active":
        raise ValueError("Invalid credentials or the account is not approved.")

    if not verify_password(password, user.get("PasswordSalt", ""), user.get("PasswordHash", "")):
        raise ValueError("Invalid credentials or the account is not approved.")

    token = secrets.token_urlsafe(48)
    expires_at = datetime.now(timezone.utc) + timedelta(hours=SESSION_HOURS)
    create_auth_session(
        user_id=int(user["UserID"]),
        token_hash=_token_hash(token),
        expires_at=expires_at,
    )
    safe_user = {k: v for k, v in user.items() if k not in {"PasswordHash", "PasswordSalt", "ApprovalTokenHash"}}
    return safe_user, token, expires_at


def session_user(token: str) -> Optional[Dict[str, Any]]:
    ensure_auth_schema()
    if not token:
        return None
    return get_auth_session_user(_token_hash(token))


def logout_token(token: str) -> None:
    ensure_auth_schema()
    if token:
        delete_auth_session(_token_hash(token))


def admin_users() -> list[dict]:
    ensure_auth_schema()
    return list_auth_users()


def admin_set_user_status(user_id: int, status: str) -> Dict[str, Any]:
    ensure_auth_schema()
    status = str(status or "").strip().title()
    if status not in {"Active", "Disabled", "Pending"}:
        raise ValueError("Status must be Active, Disabled, or Pending.")
    return set_user_status(int(user_id), status)


def auth_settings() -> dict:
    mail = email_settings()
    return {
        "allowed_domain": allowed_domain(),
        "approval_admin_email": normalize_email(os.getenv("USER_APPROVAL_ADMIN_EMAIL", "")),
        "email_configured": bool(mail.get("configured")),
        "auth_required": (os.getenv("AUTH_REQUIRED", "true").strip().lower() not in {"0", "false", "no", "off"}),
    }


def admin_warehouses() -> list[dict]:
    ensure_auth_schema()
    return list_warehouses()
