from __future__ import annotations
from weatherguard.sms_state import is_sms_subscribed

import os
import time
import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterable, Optional, List, Tuple

# Matplotlib: used only for file output, no GUI
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

from twilio.rest import Client

from app.config import load_settings, parse_users

DEFAULT_DB = "/opt/weatherguard/data/feedback.db"
DEFAULT_MEDIA_ROOT = "/opt/weatherguard/public_media"

@dataclass
class Point:
    ts: int
    score: int


def _db_path() -> str:
    return os.environ.get("WEATHERGUARD_FEEDBACK_DB", DEFAULT_DB)


def _conn() -> sqlite3.Connection:
    c = sqlite3.connect(_db_path())
    c.execute("PRAGMA journal_mode=WAL;")
    c.execute("PRAGMA synchronous=NORMAL;")
    return c


def _table_exists(c: sqlite3.Connection, name: str) -> bool:
    row = c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (name,)).fetchone()
    return bool(row)


def _fetch_points(phone: str, profile: str, since_ts: int) -> List[Point]:
    """Prefer readings (continuous). Fallback to alerts (sparse)."""
    c = _conn()
    try:
        if _table_exists(c, "readings"):
            rows = c.execute(
                """
                SELECT ts, score
                FROM readings
                WHERE phone=? AND profile=? AND ts>=?
                ORDER BY ts ASC
                """,
                (phone, profile, since_ts),
            ).fetchall()
            pts = [Point(int(r[0]), int(r[1])) for r in rows if r[1] is not None]
            if pts:
                return pts

        if _table_exists(c, "alerts"):
            rows = c.execute(
                """
                SELECT ts, score
                FROM alerts
                WHERE phone=? AND profile=? AND ts>=?
                ORDER BY ts ASC
                """,
                (phone, profile, since_ts),
            ).fetchall()
            return [Point(int(r[0]), int(r[1] or 0)) for r in rows]

        return []
    finally:
        c.close()


def _render_chart(points: List[Point], *, title: str, out_path: str, tz_name: str = "Europe/Warsaw") -> None:
    # Convert to datetimes in local tz for nicer labels
    try:
        import zoneinfo
        tz = zoneinfo.ZoneInfo(tz_name)
    except Exception:
        tz = timezone.utc

    xs = [datetime.fromtimestamp(p.ts, tz=tz) for p in points]
    ys = [p.score for p in points]

    plt.figure(figsize=(9, 4.5), dpi=140)
    plt.plot(xs, ys, marker="o", linewidth=2)
    plt.title(title)
    plt.xlabel("Czas")
    plt.ylabel("Score")
    plt.grid(True, alpha=0.25)
    plt.tight_layout()

    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    plt.savefig(out_path)
    plt.close()


def _public_url_for(local_path: str, public_base: str) -> str:
    # local_path expected under DEFAULT_MEDIA_ROOT
    rel = os.path.relpath(local_path, DEFAULT_MEDIA_ROOT).replace(os.sep, "/")
    base = public_base.rstrip("/")
    return f"{base}/wg-media/{rel}"


def _send_whatsapp_media(*, to_phone: str, from_phone: str, sid: str, token: str, body: str, media_url: str) -> str:
    client = Client(sid, token)

        # [WG-SMS v4] subscription guard
        try:
            if not is_sms_subscribed(to_phone):
                return None
        except Exception:
            pass
    msg = client.messages.create(
        from_=from_phone,
        to=to_phone,
        body=body,
        media_url=[media_url],
    )
    return str(getattr(msg, "sid", ""))


def main() -> int:
    env_path = os.environ.get("WEATHERGUARD_ENV", "/opt/weatherguard/config/.env")
    public_base = os.environ.get("WEATHERGUARD_PUBLIC_BASE", "https://dev.pracunia.pl")
    tz_name = os.environ.get("WEATHERGUARD_TZ", "Europe/Warsaw")

    settings = load_settings(env_path)
    users = parse_users(settings.users_file)

    now = int(time.time())
    since = now - 7 * 24 * 3600

    ok = 0
    skipped = 0
    for u in users:
        # Only users with a phone and a known profile
        phone = getattr(u, "phone", None)
        profile = getattr(u, "profile", None)
        if not phone or not profile:
            skipped += 1
            continue

        pts = _fetch_points(phone, profile, since)
        if len(pts) < 2:
            skipped += 1
            continue

        # Output file name includes phone/profile and date
        day = datetime.now().strftime("%Y-%m-%d")
        safe_phone = phone.replace("+", "").replace(":", "").replace(" ", "")
        out_dir = os.path.join(DEFAULT_MEDIA_ROOT, "trends")
        out_path = os.path.join(out_dir, f"trend_{safe_phone}_{profile}_{day}.png")

        title = f"Trend 7 dni — {profile} — {safe_phone}"
        _render_chart(pts, title=title, out_path=out_path, tz_name=tz_name)

        media_url = _public_url_for(out_path, public_base)
        body = f"Twoj trend (ostatnie 7 dni) — {profile}."\
               f"\nJesli chcesz zmienic alerty: napisz 'ALERTY'."\
               f"\nJesli potrzebujesz pomocy: 'POMOC'."

        _send_whatsapp_media(
            to_phone=phone,
            from_phone=settings.twilio_whatsapp_from,
            sid=settings.twilio_account_sid,
            token=settings.twilio_auth_token,
            body=body,
            media_url=media_url,
        )
        ok += 1

    print(f"OK: weekly trends sent={ok} skipped={skipped}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
