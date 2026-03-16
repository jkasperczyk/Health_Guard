from __future__ import annotations

import os
import sqlite3
import time
import json
from typing import Optional, Any, Dict, List, Tuple

DEFAULT_DB = "/opt/weatherguard/data/feedback.db"


def _db_path() -> str:
    return os.environ.get("WEATHERGUARD_FEEDBACK_DB", DEFAULT_DB)


def _conn() -> sqlite3.Connection:
    c = sqlite3.connect(_db_path())
    c.execute("PRAGMA journal_mode=WAL;")
    c.execute("PRAGMA synchronous=NORMAL;")
    return c


def ensure_schema() -> None:
    """
    Ensures SQLite schema is present and applies additive migrations.

    Tables:
      - alerts: one row per sent alert (after send)
      - feedback: every inbound WA feedback message
      - readings: one row per run/user/profile (even if no alert) for trend building
    """
    c = _conn()
    try:
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS alerts (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              ts INTEGER NOT NULL,
              phone TEXT NOT NULL,
              profile TEXT NOT NULL,
              location TEXT,
              score INTEGER,
              threshold INTEGER,
              label TEXT,
              reasons_json TEXT,
              sid TEXT,
              feedback_answer TEXT,
              feedback_detail TEXT,
              feedback_ts INTEGER
            )
            """
        )

        c.execute(
            """
            CREATE TABLE IF NOT EXISTS feedback (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              ts INTEGER NOT NULL,
              phone TEXT NOT NULL,
              answer TEXT NOT NULL,
              detail TEXT,
              raw TEXT,
              remote_addr TEXT,
              user_agent TEXT,
              alert_id INTEGER
            )
            """
        )

        c.execute(
            """
            CREATE TABLE IF NOT EXISTS readings (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              ts INTEGER NOT NULL,
              phone TEXT NOT NULL,
              profile TEXT NOT NULL,
              location TEXT,
              score INTEGER,
              threshold INTEGER,
              label TEXT,
              reasons_json TEXT,
              feats_json TEXT
            )
            """
        )

        cols_fb = {r[1] for r in c.execute("PRAGMA table_info(feedback);").fetchall()}
        if "alert_id" not in cols_fb:
            c.execute("ALTER TABLE feedback ADD COLUMN alert_id INTEGER;")

        cols_a = {r[1] for r in c.execute("PRAGMA table_info(alerts);").fetchall()}
        for name, ddl in [
            ("feedback_answer", "ALTER TABLE alerts ADD COLUMN feedback_answer TEXT;"),
            ("feedback_detail", "ALTER TABLE alerts ADD COLUMN feedback_detail TEXT;"),
            ("feedback_ts", "ALTER TABLE alerts ADD COLUMN feedback_ts INTEGER;"),
        ]:
            if name not in cols_a:
                c.execute(ddl)

        c.execute("CREATE INDEX IF NOT EXISTS idx_alerts_phone_profile_ts ON alerts(phone, profile, ts);")
        c.execute("CREATE INDEX IF NOT EXISTS idx_feedback_phone_ts ON feedback(phone, ts);")
        c.execute("CREATE INDEX IF NOT EXISTS idx_readings_phone_profile_ts ON readings(phone, profile, ts);")

        c.execute(
            """
            CREATE TABLE IF NOT EXISTS sms_users (
                phone               TEXT PRIMARY KEY,
                subscribed          INTEGER NOT NULL DEFAULT 1,
                factors_json        TEXT,
                created_at          TEXT,
                updated_at          TEXT,
                last_interaction_at TEXT
            )
            """
        )
        c.execute("CREATE INDEX IF NOT EXISTS idx_sms_users_phone ON sms_users(phone);")

        c.execute(
            """
            CREATE TABLE IF NOT EXISTS wg_users (
                phone         TEXT PRIMARY KEY,
                profiles_json TEXT NOT NULL DEFAULT '["migraine"]',
                location      TEXT NOT NULL DEFAULT '',
                threshold     INTEGER,
                quiet_hours   TEXT,
                enabled       INTEGER NOT NULL DEFAULT 1,
                updated_at    TEXT NOT NULL DEFAULT ''
            )
            """
        )
        c.execute("CREATE INDEX IF NOT EXISTS idx_wg_users_enabled ON wg_users(enabled);")

        c.commit()
    finally:
        c.close()


def _json_dump(v: Any) -> Optional[str]:
    if v is None:
        return None
    try:
        return json.dumps(v, ensure_ascii=False, separators=(",", ":"))
    except Exception:
        return str(v)


def record_alert(
    *,
    phone: str,
    profile: str,
    location: Optional[str] = None,
    score: Optional[int] = None,
    threshold: Optional[int] = None,
    label: Optional[str] = None,
    reasons_json: Optional[str] = None,
    reasons: Optional[Any] = None,
    sid: Optional[str] = None,
) -> int:
    """Record a sent alert (reasons can be passed as reasons_json OR reasons=list/dict)."""
    ensure_schema()
    c = _conn()
    try:
        ts = int(time.time())
        rj = reasons_json if reasons_json is not None else _json_dump(reasons)
        cur = c.execute(
            """
            INSERT INTO alerts(ts, phone, profile, location, score, threshold, label, reasons_json, sid)
            VALUES(?,?,?,?,?,?,?,?,?)
            """,
            (ts, phone, profile, location, score, threshold, label, rj, sid),
        )
        c.commit()
        return int(cur.lastrowid)
    finally:
        c.close()


def record_reading(
    *,
    phone: str,
    profile: str,
    location: Optional[str] = None,
    score: Optional[int] = None,
    threshold: Optional[int] = None,
    label: Optional[str] = None,
    reasons: Optional[Any] = None,
    feats: Optional[Dict[str, Any]] = None,
) -> int:
    """Record a per-run reading for trend building (even if no alert)."""
    ensure_schema()
    c = _conn()
    try:
        ts = int(time.time())
        cur = c.execute(
            """
            INSERT INTO readings(ts, phone, profile, location, score, threshold, label, reasons_json, feats_json)
            VALUES(?,?,?,?,?,?,?,?,?)
            """,
            (ts, phone, profile, location, score, threshold, label, _json_dump(reasons), _json_dump(feats)),
        )
        c.commit()
        return int(cur.lastrowid)
    finally:
        c.close()


def _latest_alert_id(phone: str, profile: Optional[str], window_seconds: int = 24 * 3600) -> Optional[int]:
    ensure_schema()
    c = _conn()
    try:
        since = int(time.time()) - int(window_seconds)
        if profile:
            row = c.execute(
                "SELECT id FROM alerts WHERE phone=? AND profile=? AND ts>=? ORDER BY ts DESC LIMIT 1",
                (phone, profile, since),
            ).fetchone()
            if row:
                return int(row[0])

        row2 = c.execute(
            "SELECT id FROM alerts WHERE phone=? AND ts>=? ORDER BY ts DESC LIMIT 1",
            (phone, since),
        ).fetchone()
        return int(row2[0]) if row2 else None
    finally:
        c.close()


def record_feedback(
    *,
    phone: str,
    answer: str,
    detail: Optional[str] = None,
    raw: Optional[str] = None,
    remote_addr: Optional[str] = None,
    user_agent: Optional[str] = None,
    profile_hint: Optional[str] = "migraine",
    link_window_seconds: int = 24 * 3600,
) -> Optional[int]:
    """Store inbound feedback and link to the latest alert in the time window."""
    ensure_schema()
    alert_id = (
        _latest_alert_id(phone, profile_hint, window_seconds=link_window_seconds)
        if profile_hint
        else _latest_alert_id(phone, None, window_seconds=link_window_seconds)
    )

    c = _conn()
    try:
        ts = int(time.time())
        cur = c.execute(
            """
            INSERT INTO feedback(ts, phone, answer, detail, raw, remote_addr, user_agent, alert_id)
            VALUES(?,?,?,?,?,?,?,?)
            """,
            (ts, phone, answer, detail, raw, remote_addr, user_agent, alert_id),
        )
        if alert_id is not None:
            c.execute(
                "UPDATE alerts SET feedback_answer=?, feedback_detail=?, feedback_ts=? WHERE id=?",
                (answer, detail, ts, alert_id),
            )
        c.commit()
        return int(cur.lastrowid)
    finally:
        c.close()


def query_alerts(hours: int = 24, phone: Optional[str] = None, profile: Optional[str] = None) -> List[Tuple]:
    ensure_schema()
    c = _conn()
    try:
        since = int(time.time()) - int(hours * 3600)
        q = """
            SELECT id, ts, phone, profile, location, score, threshold, label, sid,
                   feedback_answer, feedback_detail, feedback_ts
            FROM alerts
            WHERE ts>=?
        """
        params: List[Any] = [since]
        if phone:
            q += " AND phone=?"
            params.append(phone)
        if profile:
            q += " AND profile=?"
            params.append(profile)
        q += " ORDER BY ts DESC"
        return c.execute(q, params).fetchall()
    finally:
        c.close()


def query_feedback(hours: int = 24, phone: Optional[str] = None) -> List[Tuple]:
    ensure_schema()
    c = _conn()
    try:
        since = int(time.time()) - int(hours * 3600)
        q = "SELECT id, ts, phone, answer, detail, raw, alert_id FROM feedback WHERE ts>=?"
        params: List[Any] = [since]
        if phone:
            q += " AND phone=?"
            params.append(phone)
        q += " ORDER BY ts DESC"
        return c.execute(q, params).fetchall()
    finally:
        c.close()


def query_trend(phone: str, profile: str, days: int = 7) -> List[Tuple]:
    ensure_schema()
    c = _conn()
    try:
        since = int(time.time()) - int(days * 86400)
        return c.execute(
            """
            SELECT ts, score, threshold, label, location
            FROM readings
            WHERE phone=? AND profile=? AND ts>=?
            ORDER BY ts ASC
            """,
            (phone, profile, since),
        ).fetchall()
    finally:
        c.close()


# ---------------------------------------------------------------------------
# SMS user state (replaces sms_users.json)
# ---------------------------------------------------------------------------

import datetime as _dt


def _sms_utc_iso() -> str:
    return _dt.datetime.now(_dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _sms_row_to_dict(row) -> Dict[str, Any]:
    phone, subscribed, factors_json, created_at, updated_at, last_interaction_at = row
    factors: Dict[str, Any] = {}
    if factors_json:
        try:
            factors = json.loads(factors_json)
        except Exception:
            factors = {}
    return {
        "subscribed": bool(subscribed),
        "factors": factors,
        "created_at": created_at,
        "updated_at": updated_at,
        "last_interaction_at": last_interaction_at,
    }


def sms_ensure_user(phone: str) -> Dict[str, Any]:
    """Upsert a user row (creates if absent). Returns the user dict."""
    ensure_schema()
    now = _sms_utc_iso()
    c = _conn()
    try:
        c.execute(
            """
            INSERT INTO sms_users(phone, subscribed, factors_json, created_at, updated_at, last_interaction_at)
            VALUES (?, 1, '{}', ?, ?, ?)
            ON CONFLICT(phone) DO UPDATE SET
                updated_at          = excluded.updated_at,
                last_interaction_at = excluded.last_interaction_at
            """,
            (phone, now, now, now),
        )
        c.commit()
        row = c.execute(
            "SELECT phone, subscribed, factors_json, created_at, updated_at, last_interaction_at FROM sms_users WHERE phone=?",
            (phone,),
        ).fetchone()
        return _sms_row_to_dict(row) if row else {}
    finally:
        c.close()


def sms_get_user(phone: str) -> Dict[str, Any]:
    """Return user dict or {} if not found."""
    ensure_schema()
    c = _conn()
    try:
        row = c.execute(
            "SELECT phone, subscribed, factors_json, created_at, updated_at, last_interaction_at FROM sms_users WHERE phone=?",
            (phone,),
        ).fetchone()
        return _sms_row_to_dict(row) if row else {}
    finally:
        c.close()


def sms_is_subscribed(phone: str) -> bool:
    """Return True if user is subscribed (default True for unknown users)."""
    ensure_schema()
    c = _conn()
    try:
        row = c.execute("SELECT subscribed FROM sms_users WHERE phone=?", (phone,)).fetchone()
        return bool(row[0]) if row else True
    finally:
        c.close()


def sms_set_subscribed(phone: str, subscribed: bool) -> None:
    ensure_schema()
    now = _sms_utc_iso()
    c = _conn()
    try:
        c.execute(
            """
            INSERT INTO sms_users(phone, subscribed, factors_json, created_at, updated_at, last_interaction_at)
            VALUES (?, ?, '{}', ?, ?, ?)
            ON CONFLICT(phone) DO UPDATE SET
                subscribed          = excluded.subscribed,
                updated_at          = excluded.updated_at,
                last_interaction_at = excluded.last_interaction_at
            """,
            (phone, 1 if subscribed else 0, now, now, now),
        )
        c.commit()
    finally:
        c.close()


def sms_update_factor(phone: str, key: str, value: Any, extra: Optional[Dict[str, Any]] = None) -> None:
    ensure_schema()
    now = _sms_utc_iso()
    c = _conn()
    try:
        row = c.execute("SELECT factors_json FROM sms_users WHERE phone=?", (phone,)).fetchone()
        factors: Dict[str, Any] = {}
        if row and row[0]:
            try:
                factors = json.loads(row[0])
            except Exception:
                factors = {}
        entry: Dict[str, Any] = {"at": now}
        if value is not None:
            entry["value"] = value
        if extra:
            entry.update(extra)
        factors[key] = entry
        c.execute(
            """
            INSERT INTO sms_users(phone, subscribed, factors_json, created_at, updated_at, last_interaction_at)
            VALUES (?, 1, ?, ?, ?, ?)
            ON CONFLICT(phone) DO UPDATE SET
                factors_json        = excluded.factors_json,
                updated_at          = excluded.updated_at,
                last_interaction_at = excluded.last_interaction_at
            """,
            (phone, json.dumps(factors, ensure_ascii=False), now, now, now),
        )
        c.commit()
    finally:
        c.close()


def sms_clear_factors(phone: str) -> None:
    ensure_schema()
    now = _sms_utc_iso()
    c = _conn()
    try:
        c.execute(
            """
            INSERT INTO sms_users(phone, subscribed, factors_json, created_at, updated_at, last_interaction_at)
            VALUES (?, 1, '{}', ?, ?, ?)
            ON CONFLICT(phone) DO UPDATE SET
                factors_json        = '{}',
                updated_at          = excluded.updated_at,
                last_interaction_at = excluded.last_interaction_at
            """,
            (phone, now, now, now),
        )
        c.commit()
    finally:
        c.close()


def get_wg_users() -> list:
    """Return all enabled users from wg_users as UserCfg instances, one per profile.
    Returns empty list if the table is empty (caller should fall back to users.txt)."""
    from app.config import UserCfg  # lazy to avoid circular-import risk
    ensure_schema()
    c = _conn()
    try:
        rows = c.execute(
            "SELECT phone, profiles_json, location, threshold, quiet_hours "
            "FROM wg_users WHERE enabled=1 ORDER BY phone"
        ).fetchall()
    finally:
        c.close()
    result = []
    for phone, profiles_json, location, threshold, quiet_hours in rows:
        try:
            profiles = json.loads(profiles_json or '["migraine"]')
        except Exception:
            profiles = ["migraine"]
        for profile in (profiles or ["migraine"]):
            result.append(UserCfg(
                phone=phone,
                profile=str(profile),
                location=location or "",
                threshold=int(threshold) if threshold is not None else None,
                quiet_hours=quiet_hours or None,
            ))
    return result


def sms_migrate_from_json(json_path: str, users_txt: str = "") -> int:
    """Import users into the sms_users table from two sources (INSERT OR IGNORE — safe to call repeatedly):
    1. sms_users.json (json_path) — preserves subscribed flag and factors.
    2. users.txt (users_txt) — seeds any phone not already present with subscribed=1, empty factors.
    Returns total number of rows newly inserted."""
    ensure_schema()
    now = _sms_utc_iso()
    c = _conn()
    count = 0
    try:
        # --- source 1: sms_users.json ---
        if os.path.exists(json_path):
            try:
                with open(json_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
            except Exception:
                data = {}
            if isinstance(data, dict):
                for phone, u in data.items():
                    if not isinstance(u, dict):
                        continue
                    c.execute(
                        """
                        INSERT OR IGNORE INTO sms_users(phone, subscribed, factors_json, created_at, updated_at, last_interaction_at)
                        VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        (
                            phone,
                            1 if u.get("subscribed", True) else 0,
                            json.dumps(u.get("factors") or {}, ensure_ascii=False),
                            u.get("created_at") or now,
                            u.get("updated_at") or now,
                            u.get("last_interaction_at") or now,
                        ),
                    )
                    count += c.execute("SELECT changes()").fetchone()[0]

        # --- source 2: users.txt ---
        if users_txt and os.path.exists(users_txt):
            from app.config import parse_users  # lazy import to avoid circularity
            for user in parse_users(users_txt):
                c.execute(
                    """
                    INSERT OR IGNORE INTO sms_users(phone, subscribed, factors_json, created_at, updated_at, last_interaction_at)
                    VALUES (?, 1, '{}', ?, ?, ?)
                    """,
                    (user.phone, now, now, now),
                )
                count += c.execute("SELECT changes()").fetchone()[0]

        c.commit()
    finally:
        c.close()
    return count
