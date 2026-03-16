from __future__ import annotations

import os
import time
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone, date
from typing import Optional, List, Dict, Tuple

# Matplotlib: used only for file output, no GUI
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

from twilio.rest import Client

from app.config import load_settings, parse_users
from weatherguard.sms_state import is_sms_subscribed

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
    phone = (phone or "").strip()
    if phone.startswith("whatsapp:"):
        phone = phone.replace("whatsapp:", "", 1)

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

def _aggregate_daily(points: List[Point], tz_name: str) -> Tuple[List[datetime], List[float], List[int], List[int]]:
    """Return daily x (date), mean, min, max."""
    try:
        import zoneinfo
        tz = zoneinfo.ZoneInfo(tz_name)
    except Exception:
        tz = timezone.utc

    buckets: Dict[date, List[int]] = {}
    for p in points:
        dt = datetime.fromtimestamp(p.ts, tz=tz)
        buckets.setdefault(dt.date(), []).append(int(p.score))

    days = sorted(buckets.keys())
    xs = [datetime(d.year, d.month, d.day, tzinfo=tz) for d in days]
    means = [sum(buckets[d]) / max(1, len(buckets[d])) for d in days]
    mins = [min(buckets[d]) for d in days]
    maxs = [max(buckets[d]) for d in days]
    return xs, means, mins, maxs

def _render_chart_daily(points: List[Point], *, title: str, out_path: str, tz_name: str = "Europe/Warsaw") -> None:
    xs, means, mins, maxs = _aggregate_daily(points, tz_name)
    if not xs:
        return

    yerr_low = [m - lo for m, lo in zip(means, mins)]
    yerr_high = [hi - m for m, hi in zip(means, maxs)]

    plt.figure(figsize=(9, 4.5), dpi=140)
    plt.errorbar(xs, means, yerr=[yerr_low, yerr_high], fmt="-o", linewidth=2, capsize=4)

    plt.title(title)
    plt.xlabel("Dzień")
    plt.ylabel("Score")
    plt.grid(True, alpha=0.25)

    ax = plt.gca()
    ax.xaxis.set_major_locator(mdates.DayLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%d.%m"))
    plt.gcf().autofmt_xdate()

    plt.tight_layout()
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    plt.savefig(out_path)
    plt.close()

def _public_url_for_trend(out_path: str) -> str:
    base = os.environ.get("TRENDS_PUBLIC_URL_BASE", "").rstrip("/")
    fname = os.path.basename(out_path)
    if base:
        return f"{base}/{fname}"

    public_base = os.environ.get("WEATHERGUARD_PUBLIC_BASE", "https://dev.pracunia.pl").rstrip("/")
    rel = os.path.relpath(out_path, DEFAULT_MEDIA_ROOT).replace(os.sep, "/")
    return f"{public_base}/wg-media/{rel}"

def _send_sms_link(*, to_phone: str, from_phone: str, sid: str, token: str, body: str) -> Optional[str]:
    try:
        if not is_sms_subscribed(to_phone):
            return None
    except Exception:
        pass

    client = Client(sid, token)
    msg = client.messages.create(
        from_=from_phone,
        to=to_phone,
        body=body,
    )
    return str(getattr(msg, "sid", ""))

def main() -> int:
    env_path = os.environ.get("WEATHERGUARD_ENV", "/opt/weatherguard/config/.env")
    tz_name = os.environ.get("WEATHERGUARD_TZ", "Europe/Warsaw")

    settings = load_settings(env_path)
    users = parse_users(settings.users_file)

    now = int(time.time())
    since = now - 7 * 24 * 3600

    ok = 0
    skipped = 0

    tw_from = getattr(settings, "twilio_from", None) or os.environ.get("TWILIO_FROM", "")
    if not tw_from:
        print("ERROR: Missing TWILIO_FROM (set in .env)")
        return 2

    for u in users:
        phone = getattr(u, "phone", None)
        profile = getattr(u, "profile", None)
        if not phone or not profile:
            skipped += 1
            continue

        phone_norm = phone.strip()
        if phone_norm.startswith("whatsapp:"):
            phone_norm = phone_norm.replace("whatsapp:", "", 1)

        pts = _fetch_points(phone_norm, profile, since)
        if len(pts) < 2:
            skipped += 1
            continue

        day = datetime.now().strftime("%Y-%m-%d")
        safe_phone = phone_norm.replace("+", "").replace(":", "").replace(" ", "")
        out_dir = os.path.join(DEFAULT_MEDIA_ROOT, "trends")
        out_path = os.path.join(out_dir, f"trend_{safe_phone}_{profile}_{day}.png")

        title = f"Trend 7 dni — {profile} — {safe_phone}"
        _render_chart_daily(pts, title=title, out_path=out_path, tz_name=tz_name)

        url = _public_url_for_trend(out_path)
        body = f"Trend (ostatnie 7 dni) — {profile}\n{url}\nKomendy: POMOC, TREND, STOP/START."

        _send_sms_link(
            to_phone=phone_norm,
            from_phone=tw_from,
            sid=settings.twilio_account_sid,
            token=settings.twilio_auth_token,
            body=body,
        )
        ok += 1

    print(f"OK: weekly trends sms sent={ok} skipped={skipped}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
