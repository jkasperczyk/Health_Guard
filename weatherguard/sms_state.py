import json
import os
from datetime import datetime, timezone
from typing import Any, Dict

USERS_DB = os.environ.get("SMS_USERS_DB", "/opt/weatherguard/data/sms_users.json")

def _utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00","Z")

def normalize_phone(phone: str) -> str:
    p = (phone or "").strip()
    if p.startswith("whatsapp:"):
        p = p.replace("whatsapp:", "", 1)
    if p.startswith("sms:"):
        p = p.replace("sms:", "", 1)
    return p.strip()

def _load() -> Dict[str, Any]:
    try:
        with open(USERS_DB, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def _save(db: Dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(USERS_DB) or ".", exist_ok=True)
    tmp = USERS_DB + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(db, f, ensure_ascii=False, indent=2)
    os.replace(tmp, USERS_DB)

def ensure_user(phone: str) -> Dict[str, Any]:
    phone = normalize_phone(phone)
    db = _load()
    u = db.get(phone) or {}
    now = _utc_iso()
    if "created_at" not in u:
        u["created_at"] = now
    u["updated_at"] = now
    u["last_interaction_at"] = now
    if "subscribed" not in u:
        u["subscribed"] = True
    if "factors" not in u:
        u["factors"] = {}
    db[phone] = u
    _save(db)
    return u

def is_sms_subscribed(phone: str) -> bool:
    phone = normalize_phone(phone)
    db = _load()
    u = db.get(phone)
    if not u:
        return True
    return bool(u.get("subscribed", True))

def set_subscribed(phone: str, subscribed: bool) -> None:
    phone = normalize_phone(phone)
    db = _load()
    u = db.get(phone) or {}
    now = _utc_iso()
    if "created_at" not in u:
        u["created_at"] = now
    u["updated_at"] = now
    u["last_interaction_at"] = now
    u["subscribed"] = bool(subscribed)
    u.setdefault("factors", {})
    db[phone] = u
    _save(db)

def update_factor(phone: str, key: str, value: Any, extra: Dict[str, Any] | None = None) -> None:
    phone = normalize_phone(phone)
    db = _load()
    u = db.get(phone) or {}
    now = _utc_iso()
    if "created_at" not in u:
        u["created_at"] = now
    u["updated_at"] = now
    u["last_interaction_at"] = now
    u.setdefault("subscribed", True)
    u.setdefault("factors", {})
    entry = {"at": now}
    if value is not None:
        entry["value"] = value
    if extra:
        entry.update(extra)
    u["factors"][key] = entry
    db[phone] = u
    _save(db)

def clear_factors(phone: str) -> None:
    phone = normalize_phone(phone)
    db = _load()
    u = db.get(phone) or {}
    now = _utc_iso()
    if "created_at" not in u:
        u["created_at"] = now
    u["updated_at"] = now
    u["last_interaction_at"] = now
    u.setdefault("subscribed", True)
    u["factors"] = {}
    db[phone] = u
    _save(db)

def get_user(phone: str) -> Dict[str, Any]:
    phone = normalize_phone(phone)
    db = _load()
    return db.get(phone) or {}
