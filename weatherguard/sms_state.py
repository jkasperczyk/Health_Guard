from __future__ import annotations

import os
from typing import Any, Dict

USERS_DB = os.environ.get("SMS_USERS_DB", "/opt/weatherguard/data/sms_users.json")

from app.feedback_store import (
    sms_ensure_user,
    sms_get_user,
    sms_is_subscribed,
    sms_set_subscribed,
    sms_update_factor,
    sms_clear_factors,
    sms_migrate_from_json,
)

# Migrate existing JSON data into SQLite on first import (idempotent).
try:
    sms_migrate_from_json(USERS_DB)
except Exception:
    pass


def normalize_phone(phone: str) -> str:
    p = (phone or "").strip()
    if p.startswith("whatsapp:"):
        p = p.replace("whatsapp:", "", 1)
    if p.startswith("sms:"):
        p = p.replace("sms:", "", 1)
    return p.strip()


def ensure_user(phone: str) -> Dict[str, Any]:
    return sms_ensure_user(normalize_phone(phone))


def is_sms_subscribed(phone: str) -> bool:
    return sms_is_subscribed(normalize_phone(phone))


def set_subscribed(phone: str, subscribed: bool) -> None:
    sms_set_subscribed(normalize_phone(phone), subscribed)


def update_factor(phone: str, key: str, value: Any, extra: Dict[str, Any] | None = None) -> None:
    sms_update_factor(normalize_phone(phone), key, value, extra)


def clear_factors(phone: str) -> None:
    sms_clear_factors(normalize_phone(phone))


def get_user(phone: str) -> Dict[str, Any]:
    return sms_get_user(normalize_phone(phone))
