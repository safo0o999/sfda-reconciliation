from __future__ import annotations

import base64
import hashlib
import hmac
import logging
import os
import secrets
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, Tuple

from engine.database import (
    verify_auth_schema,
    approve_user_by_token_hash,
    count_active_admins,
    create_auth_session,
    create_pending_user_record,
    delete_auth_session,
    find_user_by_email,
    get_auth_session_user,
    list_auth_users,
    set_user_status,
    get_madinah_warehouse,
    get_registration_warehouse_by_id,
    list_registration_warehouses,
    list_warehouses,
    create_password_reset_request,
    get_password_reset_request_by_token_hash,
    set_password_reset_request_status,
    reset_password_by_token_hash,
)
from engine.email_service import email_settings, send_email


logger = logging.getLogger("SFDA-Reconciliation.Auth")


_AUTH_SCHEMA_READY = False
_AUTH_SCHEMA_LOCK = threading.Lock()

# Collapse the burst of identical session lookups created by parallel UI API
# calls immediately after sign-in. SQL remains the source of truth.
_SESSION_CACHE_TTL_SECONDS = 15
_SESSION_CACHE: Dict[str, Tuple[float, Dict[str, Any]]] = {}
_SESSION_CACHE_LOCK = threading.Lock()


def ensure_auth_schema() -> None:
    """
    Perform a lightweight one-time verification of the V6 auth schema.

    Registration/login must never execute the full database migration.
    The deployment migration is handled separately in Azure SQL.
    """
    global _AUTH_SCHEMA_READY

    if _AUTH_SCHEMA_READY:
        return

    with _AUTH_SCHEMA_LOCK:
        if _AUTH_SCHEMA_READY:
            return

        started_at = datetime.now(timezone.utc)
        logger.info("Auth schema verification started.")

        verify_auth_schema()

        _AUTH_SCHEMA_READY = True
        elapsed = (datetime.now(timezone.utc) - started_at).total_seconds()
        logger.info("Auth schema verification completed in %.3f seconds.", elapsed)

PBKDF2_ITERATIONS = 310_000
SESSION_HOURS = 12
APPROVAL_HOURS = 48
PASSWORD_RESET_HOURS = 24


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


def register_user(email: str, password: str, warehouse_id: int, base_url: str) -> Dict[str, Any]:
    request_started = datetime.now(timezone.utc)
    normalized_email = normalize_email(email)
    logger.info("Registration started for %s.", normalized_email or "<empty>")

    ensure_auth_schema()
    logger.info("Registration schema verification ready for %s.", normalized_email or "<empty>")

    email = validate_corporate_email(email)
    password = validate_password(password)
    logger.info("Registration input validation passed for %s.", email)

    existing = find_user_by_email(email)
    logger.info(
        "Registration existing-user lookup completed for %s. Exists=%s Status=%s",
        email,
        bool(existing),
        str((existing or {}).get("Status") or ""),
    )

    if existing and str(existing.get("Status", "")).lower() == "active":
        raise ValueError("A user with this email already exists and is active.")

    salt_b64, password_hash = _password_hash(password)
    approval_token = secrets.token_urlsafe(48)
    approval_hash = _token_hash(approval_token)
    approval_expires = datetime.now(timezone.utc) + timedelta(hours=APPROVAL_HOURS)

    first_admin = count_active_admins() == 0
    logger.info("Registration admin-count check completed for %s. FirstAdmin=%s", email, first_admin)

    if first_admin:
        warehouse = get_madinah_warehouse()
        role = "Admin"
        status = "Active"
        requested_warehouse_name = str(warehouse.get("WarehouseName") or "Madinah Warehouse")
        logger.info(
            "First administrator %s assigned to WarehouseID=%s (%s).",
            email,
            warehouse.get("WarehouseID"),
            requested_warehouse_name,
        )
    else:
        warehouse = get_registration_warehouse_by_id(warehouse_id)
        requested_warehouse_name = str(warehouse.get("WarehouseName") or "").strip()
        if not requested_warehouse_name:
            raise ValueError("The selected warehouse is not valid.")

        role = "User"
        status = "Pending"
        logger.info(
            "Pending user %s assigned to approved WarehouseID=%s (%s).",
            email,
            warehouse.get("WarehouseID"),
            requested_warehouse_name,
        )

    logger.info("Registration database upsert started for %s.", email)
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
    logger.info(
        "Registration database upsert completed for %s. UserID=%s Status=%s",
        email,
        user.get("UserID"),
        user.get("Status"),
    )

    if first_admin:
        elapsed = (datetime.now(timezone.utc) - request_started).total_seconds()
        logger.info(
            "First administrator registration completed for %s in %.3f seconds.",
            email,
            elapsed,
        )
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

        logger.info(
            "Registration approval email attempt started for %s -> %s.",
            email,
            admin_email,
        )
        try:
            send_email(
                admin_email,
                "SFDA Reconciliation - New User Approval",
                body,
            )
            mail_sent = True
            logger.info("Registration approval email sent for %s.", email)
        except Exception as exc:
            mail_error = str(exc)
            logger.warning(
                "Registration approval email failed for %s: %s",
                email,
                mail_error,
            )
    else:
        mail_error = "USER_APPROVAL_ADMIN_EMAIL is not configured."
        logger.info("Registration approval email skipped for %s: %s", email, mail_error)

    elapsed = (datetime.now(timezone.utc) - request_started).total_seconds()
    logger.info(
        "Registration completed for %s in %.3f seconds. Status=%s EmailSent=%s",
        email,
        elapsed,
        user.get("Status"),
        mail_sent,
    )

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



def request_password_reset(email: str) -> Dict[str, Any]:
    """Create an admin-approved reset request and return the browser token."""
    ensure_auth_schema()
    normalized = validate_corporate_email(email)
    user = find_user_by_email(normalized)
    if not user or str(user.get("Status", "")).strip().lower() != "active":
        raise ValueError("No active account was found for this company email.")

    token = secrets.token_urlsafe(48)
    expires_at = datetime.now(timezone.utc) + timedelta(hours=PASSWORD_RESET_HOURS)
    create_password_reset_request(
        user_id=int(user["UserID"]),
        token_hash=_token_hash(token),
        expires_at=expires_at,
    )
    logger.info("Password reset request created for %s and is waiting for admin approval.", normalized)
    return {
        "request_token": token,
        "reset_status": "Pending",
        "expires_at": expires_at,
        "message": (
            "Password reset request submitted. "
            "Wait for administrator approval, then check the request status."
        ),
    }


def password_reset_status(token: str) -> Dict[str, Any]:
    ensure_auth_schema()
    if not token:
        raise ValueError("Password reset request token is required.")

    request = get_password_reset_request_by_token_hash(_token_hash(token))
    if not request:
        raise ValueError("Password reset request was not found.")

    expires_at = request.get("PasswordResetExpiresAt")
    if expires_at is not None:
        expiry = expires_at
        if getattr(expiry, "tzinfo", None) is None:
            expiry = expiry.replace(tzinfo=timezone.utc)
        if expiry < datetime.now(timezone.utc):
            return {
                "status": "Expired",
                "message": "This password reset request has expired. Submit a new request.",
            }

    status = str(request.get("PasswordResetStatus") or "Pending")
    messages = {
        "Pending": "Waiting for administrator approval.",
        "Approved": "Approved. You can now choose a new password.",
        "Rejected": "The administrator rejected this password reset request.",
        "Completed": "This password reset request has already been completed.",
    }
    return {
        "status": status,
        "message": messages.get(status, f"Password reset status: {status}."),
        "expires_at": request.get("PasswordResetExpiresAt"),
    }


def admin_set_password_reset_status(
    user_id: int,
    action: str,
    approved_by: str,
) -> Dict[str, Any]:
    ensure_auth_schema()
    return set_password_reset_request_status(
        user_id=int(user_id),
        action=action,
        approved_by=normalize_email(approved_by),
    )


def reset_password(token: str, new_password: str) -> Dict[str, Any]:
    ensure_auth_schema()
    if not token:
        raise ValueError("Password reset request token is required.")

    password = validate_password(new_password)
    password_salt, password_hash = _password_hash(password)
    user = reset_password_by_token_hash(
        _token_hash(token),
        password_salt,
        password_hash,
    )
    if not user:
        raise ValueError(
            "The reset request is not approved, has expired, or has already been used."
        )

    # Remove any cached sessions for this user. SQL sessions were revoked by DB.
    with _SESSION_CACHE_LOCK:
        _SESSION_CACHE.clear()

    logger.info("Admin-approved password reset completed for %s.", user.get("Email"))
    return user



def login_user(email: str, password: str) -> Tuple[Dict[str, Any], str, datetime]:
    ensure_auth_schema()
    email = validate_corporate_email(email)
    user = find_user_by_email(email)

    if not user:
        logger.warning("Login rejected for %s: user not found.", email)
        raise ValueError("Invalid email or password.")

    status = str(user.get("Status", "")).strip().lower()
    if status != "active":
        logger.info("Login rejected for %s: account status is %s.", email, user.get("Status"))
        if status == "pending":
            raise ValueError("Your account is still pending administrator approval.")
        if status == "disabled":
            raise ValueError("Your account is disabled. Contact the administrator.")
        raise ValueError("Your account is not active. Contact the administrator.")

    if not verify_password(password, user.get("PasswordSalt", ""), user.get("PasswordHash", "")):
        logger.info("Login rejected for %s: Active account but password verification failed.", email)
        raise ValueError("Incorrect password. Your account is already approved and active.")

    token = secrets.token_urlsafe(48)
    expires_at = datetime.now(timezone.utc) + timedelta(hours=SESSION_HOURS)
    create_auth_session(
        user_id=int(user["UserID"]),
        token_hash=_token_hash(token),
        expires_at=expires_at,
    )
    safe_user = {k: v for k, v in user.items() if k not in {"PasswordHash", "PasswordSalt", "ApprovalTokenHash"}}
    token_hash = _token_hash(token)
    with _SESSION_CACHE_LOCK:
        _SESSION_CACHE[token_hash] = (time.monotonic(), dict(safe_user))
    return safe_user, token, expires_at


def session_user(token: str) -> Optional[Dict[str, Any]]:
    ensure_auth_schema()
    if not token:
        return None

    token_hash = _token_hash(token)
    now = time.monotonic()

    with _SESSION_CACHE_LOCK:
        cached = _SESSION_CACHE.get(token_hash)
        if cached and (now - cached[0]) < _SESSION_CACHE_TTL_SECONDS:
            return dict(cached[1])
        if cached:
            _SESSION_CACHE.pop(token_hash, None)

    user = get_auth_session_user(token_hash)
    if user:
        with _SESSION_CACHE_LOCK:
            _SESSION_CACHE[token_hash] = (now, dict(user))
    return user


def logout_token(token: str) -> None:
    ensure_auth_schema()
    if token:
        token_hash = _token_hash(token)
        with _SESSION_CACHE_LOCK:
            _SESSION_CACHE.pop(token_hash, None)
        delete_auth_session(token_hash)


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


def registration_warehouses() -> list[dict]:
    ensure_auth_schema()
    return list_registration_warehouses()


def admin_warehouses() -> list[dict]:
    ensure_auth_schema()
    return list_warehouses()
