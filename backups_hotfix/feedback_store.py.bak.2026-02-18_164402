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
