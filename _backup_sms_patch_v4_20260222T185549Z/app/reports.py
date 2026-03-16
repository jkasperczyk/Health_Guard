"""WeatherGuard reporting utilities.

Generates CSV reports from feedback.db:
- Daily report: all alerts from last 24h (rolling) including latest user feedback.
- Monthly report: aggregates all daily CSVs for a given month.

Designed to be run as:
  python -m app.reports --daily
  python -m app.reports --monthly --month 2026-02

"""

from __future__ import annotations

import argparse
import csv
import glob
import os
import re
import sqlite3
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone

try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore


DEFAULT_TZ = "Europe/Warsaw"
DEFAULT_DB = "/opt/weatherguard/data/feedback.db"
DEFAULT_REPORT_DIR = "/opt/weatherguard/reports"


@dataclass
class AlertRow:
    alert_id: int
    alert_ts: int
    alert_time_utc: str
    alert_time_local: str
    phone: str
    profile: str
    location: str
    score: int | None
    threshold: int | None
    label: str
    sid: str
    reasons_json: str
    feedback_answer: str
    feedback_detail: str
    feedback_ts: int | None
    feedback_time_utc: str
    feedback_time_local: str
    feedback_raw: str


def _tz(tz_name: str):
    if ZoneInfo is None:
        return timezone.utc
    try:
        return ZoneInfo(tz_name)
    except Exception:
        return timezone.utc


def _fmt_ts(ts: int | None, tzinfo) -> str:
    if not ts:
        return ""
    return datetime.fromtimestamp(int(ts), tz=tzinfo).strftime("%Y-%m-%d %H:%M:%S %Z")


def _ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)


def _connect(db_path: str) -> sqlite3.Connection:
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    return con


def fetch_alert_rows_last_24h(db_path: str, tz_name: str) -> list[AlertRow]:
    tz_local = _tz(tz_name)
    now_utc = datetime.now(timezone.utc)
    since = int((now_utc - timedelta(hours=24)).timestamp())

    sql = """
    SELECT
      a.id AS alert_id,
      a.ts AS alert_ts,
      a.phone,
      a.profile,
      COALESCE(a.location,'') AS location,
      a.score,
      a.threshold,
      COALESCE(a.label,'') AS label,
      COALESCE(a.sid,'') AS sid,
      COALESCE(a.reasons_json,'') AS reasons_json,
      COALESCE(a.feedback_answer,'') AS feedback_answer_a,
      COALESCE(a.feedback_detail,'') AS feedback_detail_a,
      a.feedback_ts AS feedback_ts_a,
      COALESCE(f.answer,'') AS feedback_answer_f,
      COALESCE(f.detail,'') AS feedback_detail_f,
      f.ts AS feedback_ts_f,
      COALESCE(f.raw,'') AS feedback_raw
    FROM alerts a
    LEFT JOIN (
      SELECT alert_id, answer, detail, ts, raw
      FROM (
        SELECT
          alert_id, answer, detail, ts, raw, id,
          ROW_NUMBER() OVER (PARTITION BY alert_id ORDER BY ts DESC, id DESC) AS rn
        FROM feedback
        WHERE alert_id IS NOT NULL
      )
      WHERE rn = 1
    ) f
      ON f.alert_id = a.id
    WHERE a.ts >= ?
    ORDER BY a.ts ASC;
    """

    rows: list[AlertRow] = []
    with _connect(db_path) as con:
        for r in con.execute(sql, (since,)):
            alert_ts = int(r["alert_ts"])
            fb_answer = (r["feedback_answer_a"] or r["feedback_answer_f"] or "").strip()
            fb_detail = (r["feedback_detail_a"] or r["feedback_detail_f"] or "").strip()
            fb_ts = r["feedback_ts_a"] or r["feedback_ts_f"]
            fb_ts_i = int(fb_ts) if fb_ts is not None else None

            rows.append(
                AlertRow(
                    alert_id=int(r["alert_id"]),
                    alert_ts=alert_ts,
                    alert_time_utc=_fmt_ts(alert_ts, timezone.utc),
                    alert_time_local=_fmt_ts(alert_ts, tz_local),
                    phone=str(r["phone"]),
                    profile=str(r["profile"]),
                    location=str(r["location"]),
                    score=(int(r["score"]) if r["score"] is not None else None),
                    threshold=(int(r["threshold"]) if r["threshold"] is not None else None),
                    label=str(r["label"]),
                    sid=str(r["sid"]),
                    reasons_json=str(r["reasons_json"]),
                    feedback_answer=fb_answer,
                    feedback_detail=fb_detail,
                    feedback_ts=fb_ts_i,
                    feedback_time_utc=_fmt_ts(fb_ts_i, timezone.utc),
                    feedback_time_local=_fmt_ts(fb_ts_i, tz_local),
                    feedback_raw=str(r["feedback_raw"]),
                )
            )

    return rows


def write_daily_report(db_path: str, report_dir: str, tz_name: str) -> str:
    tz_local = _tz(tz_name)
    today_local = datetime.now(tz_local).strftime("%Y-%m-%d")
    out_path = os.path.join(report_dir, f"weatherguard_daily_{today_local}.csv")

    _ensure_dir(report_dir)
    rows = fetch_alert_rows_last_24h(db_path, tz_name)

    # Stable header order for Excel
    fieldnames = list(AlertRow.__annotations__.keys())

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for row in rows:
            w.writerow(asdict(row))

    return out_path


_DAILY_RE = re.compile(r"weatherguard_daily_(\d{4}-\d{2}-\d{2})\.csv$")


def _iter_daily_files(report_dir: str, month: str) -> list[str]:
    # month is YYYY-MM
    pattern = os.path.join(report_dir, "weatherguard_daily_????-??-??.csv")
    paths = sorted(glob.glob(pattern))
    out: list[str] = []
    for p in paths:
        m = _DAILY_RE.search(os.path.basename(p))
        if not m:
            continue
        day = m.group(1)
        if day.startswith(month + "-"):
            out.append(p)
    return out


def write_monthly_report(report_dir: str, tz_name: str, month: str | None = None) -> str:
    tz_local = _tz(tz_name)
    if not month:
        month = datetime.now(tz_local).strftime("%Y-%m")

    out_path = os.path.join(report_dir, f"weatherguard_monthly_{month}.csv")
    _ensure_dir(report_dir)

    daily_files = _iter_daily_files(report_dir, month)

    # If there are no daily files, still generate an empty file with the header.
    fieldnames = list(AlertRow.__annotations__.keys())
    seen_alert_ids: set[int] = set()

    with open(out_path, "w", newline="", encoding="utf-8") as out:
        w = csv.DictWriter(out, fieldnames=fieldnames)
        w.writeheader()

        for df in daily_files:
            with open(df, "r", newline="", encoding="utf-8") as inp:
                r = csv.DictReader(inp)
                # tolerate older files missing some columns
                for row in r:
                    try:
                        aid = int(row.get("alert_id") or 0)
                    except Exception:
                        aid = 0
                    if aid and aid in seen_alert_ids:
                        continue
                    if aid:
                        seen_alert_ids.add(aid)

                    # Normalize to expected header
                    normalized = {k: (row.get(k, "") if row.get(k, "") is not None else "") for k in fieldnames}
                    w.writerow(normalized)

    return out_path


def _parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(prog="weatherguard-report", add_help=True)

    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--daily", action="store_true", help="Generate daily report (last 24 hours) as CSV")
    g.add_argument("--monthly", action="store_true", help="Generate monthly report by aggregating daily CSV files")

    p.add_argument("--db", default=DEFAULT_DB, help=f"Path to feedback.db (default: {DEFAULT_DB})")
    p.add_argument("--out", default=DEFAULT_REPORT_DIR, help=f"Reports directory (default: {DEFAULT_REPORT_DIR})")
    p.add_argument("--tz", default=DEFAULT_TZ, help=f"Timezone for filenames/local times (default: {DEFAULT_TZ})")
    p.add_argument("--month", default=None, help="Month for monthly report in YYYY-MM (default: current month)")

    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    ns = _parse_args(sys.argv[1:] if argv is None else argv)

    if ns.daily:
        out = write_daily_report(ns.db, ns.out, ns.tz)
        print(out)
        return 0

    if ns.monthly:
        if ns.month and not re.match(r"^\d{4}-\d{2}$", ns.month):
            print("ERROR: --month must be YYYY-MM", file=sys.stderr)
            return 2
        out = write_monthly_report(ns.out, ns.tz, ns.month)
        print(out)
        return 0

    return 2


if __name__ == "__main__":
    raise SystemExit(main())
