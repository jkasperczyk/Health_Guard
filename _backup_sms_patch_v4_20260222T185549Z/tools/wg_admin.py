#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""WeatherGuard Admin Console (wg-admin)

- Reads /opt/weatherguard/data/feedback.db
- Shows alerts + feedback and linkages
- Health check: systemd services/timers, ports, apache ProxyPass, DB schema
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import re
import socket
import sqlite3
import subprocess
import sys
from pathlib import Path
from typing import Any, List, Optional, Tuple

DEFAULT_DB = "/opt/weatherguard/data/feedback.db"


def eprint(*a: Any) -> None:
    print(*a, file=sys.stderr)


def run_cmd(cmd: List[str], timeout: int = 8) -> Tuple[int, str]:
    try:
        p = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        out = (p.stdout or "") + (p.stderr or "")
        return p.returncode, out.strip()
    except FileNotFoundError:
        return 127, f"command not found: {cmd[0]}"
    except subprocess.TimeoutExpired:
        return 124, f"timeout: {' '.join(cmd)}"


def ts_to_str(ts: Optional[int], utc: bool = False) -> str:
    if not ts:
        return "-"
    tz = dt.timezone.utc if utc else None
    d = dt.datetime.fromtimestamp(int(ts), tz=tz)
    return d.strftime("%Y-%m-%d %H:%M:%S") + ("Z" if utc else "")


def connect_db(db_path: str) -> sqlite3.Connection:
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    return con


def table_exists(con: sqlite3.Connection, name: str) -> bool:
    r = con.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (name,),
    ).fetchone()
    return r is not None


def columns(con: sqlite3.Connection, table: str) -> List[str]:
    return [row["name"] for row in con.execute(f"PRAGMA table_info({table});").fetchall()]


def parse_since(days: Optional[int], hours: Optional[int], since: Optional[str]) -> Optional[int]:
    """Returns unix timestamp lower bound, or None.

    since can be ISO-like: '2026-02-16', '2026-02-16T12:00'
    """
    now = int(dt.datetime.now(dt.timezone.utc).timestamp())
    if days is not None:
        return now - int(days) * 86400
    if hours is not None:
        return now - int(hours) * 3600
    if since:
        s = since.strip()
        fmts = [
            "%Y-%m-%d",
            "%Y-%m-%dT%H:%M",
            "%Y-%m-%d %H:%M",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%d %H:%M:%S",
        ]
        for f in fmts:
            try:
                d = dt.datetime.strptime(s, f)
                return int(d.replace(tzinfo=dt.timezone.utc).timestamp())
            except ValueError:
                pass
        raise SystemExit(
            f"ERROR: cannot parse --since '{since}'. Use e.g. 2026-02-16 or 2026-02-16T12:00"
        )
    return None


def print_kv(title: str, kv: List[Tuple[str, str]]) -> None:
    print(f"\n== {title} ==")
    w = max((len(k) for k, _ in kv), default=10)
    for k, v in kv:
        print(f"{k.ljust(w)} : {v}")


def cmd_doctor(args: argparse.Namespace) -> int:
    db = args.db
    kv: List[Tuple[str, str]] = []
    kv.append(("db", db))
    kv.append(("db_exists", "yes" if Path(db).exists() else "NO"))
    if Path(db).exists():
        try:
            con = connect_db(db)
            kv.append(
                (
                    "tables",
                    ", ".join(
                        [
                            r["name"]
                            for r in con.execute(
                                "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;"
                            )
                        ]
                    )
                    or "-",
                )
            )
            if table_exists(con, "feedback"):
                kv.append(
                    (
                        "feedback_rows",
                        str(con.execute("SELECT COUNT(1) c FROM feedback;").fetchone()["c"]),
                    )
                )
            if table_exists(con, "alerts"):
                kv.append(
                    (
                        "alerts_rows",
                        str(con.execute("SELECT COUNT(1) c FROM alerts;").fetchone()["c"]),
                    )
                )
        except Exception as ex:
            kv.append(("db_error", repr(ex)))
    print_kv("doctor", kv)
    return 0


def cmd_recent(args: argparse.Namespace) -> int:
    con = connect_db(args.db)
    if not table_exists(con, "alerts"):
        eprint("ERROR: no alerts table. Run schema migration (ensure_schema).")
        return 2
    where = ""
    params: List[Any] = []
    since_ts = parse_since(args.days, args.hours, args.since)
    if since_ts:
        where = "WHERE ts >= ?"
        params.append(since_ts)
    q = f"""
    SELECT id, ts, phone, profile, location, score, threshold, label,
           feedback_answer, feedback_detail, feedback_ts
    FROM alerts
    {where}
    ORDER BY id DESC
    LIMIT ?;
    """
    params.append(args.limit)
    rows = con.execute(q, params).fetchall()
    print(f"Recent alerts (limit={args.limit})")
    for r in rows:
        fb = r["feedback_answer"] or "-"
        fbd = r["feedback_detail"] or "-"
        fbt = ts_to_str(r["feedback_ts"], utc=args.utc) if r["feedback_ts"] else "-"
        print(
            f'#{r["id"]:>5}  {ts_to_str(r["ts"], utc=args.utc)}  '
            f'{r["phone"]:<18}  {r["profile"]:<10}  score={r["score"]} th={r["threshold"]}  '
            f'fb={fb}/{fbd} at {fbt}'
        )
    return 0


def cmd_feedback(args: argparse.Namespace) -> int:
    con = connect_db(args.db)
    if not table_exists(con, "feedback"):
        eprint("ERROR: no feedback table.")
        return 2
    where = ""
    params: List[Any] = []
    since_ts = parse_since(args.days, args.hours, args.since)
    if since_ts:
        where = "WHERE ts >= ?"
        params.append(since_ts)
    q = f"""
    SELECT id, ts, phone, answer, detail, raw, alert_id
    FROM feedback
    {where}
    ORDER BY id DESC
    LIMIT ?;
    """
    params.append(args.limit)
    rows = con.execute(q, params).fetchall()
    print(f"Recent feedback (limit={args.limit})")
    for r in rows:
        det = r["detail"] or "-"
        aid = r["alert_id"] if "alert_id" in r.keys() else None
        print(
            f'#{r["id"]:>5}  {ts_to_str(r["ts"], utc=args.utc)}  {r["phone"]:<18}  '
            f'ans={r["answer"]:<3} det={det:<7} alert_id={aid if aid is not None else "-":<5} '
            f'raw="{(r["raw"] or "").strip()}"'
        )
    return 0


def cmd_users(args: argparse.Namespace) -> int:
    con = connect_db(args.db)
    phones = set()
    if table_exists(con, "alerts"):
        phones.update([r["phone"] for r in con.execute("SELECT DISTINCT phone FROM alerts;").fetchall()])
    if table_exists(con, "feedback"):
        phones.update(
            [r["phone"] for r in con.execute("SELECT DISTINCT phone FROM feedback;").fetchall()]
        )
    print("Users (phones):")
    for p in sorted(phones):
        print(" -", p)
    return 0


def cmd_user(args: argparse.Namespace) -> int:
    phone = args.phone.strip()
    con = connect_db(args.db)
    print(f"User: {phone}")
    if table_exists(con, "alerts"):
        rows = con.execute(
            """
            SELECT id, ts, profile, score, threshold, label,
                   feedback_answer, feedback_detail, feedback_ts
            FROM alerts WHERE phone=?
            ORDER BY id DESC LIMIT ?;
            """,
            (phone, args.limit),
        ).fetchall()
        print("\nAlerts:")
        for r in rows:
            print(
                f'#{r["id"]:>5} {ts_to_str(r["ts"], utc=args.utc)} {r["profile"]:<10} '
                f'score={r["score"]} th={r["threshold"]} label={r["label"] or "-"} '
                f'fb={r["feedback_answer"] or "-"}/{r["feedback_detail"] or "-"}'
            )
    if table_exists(con, "feedback"):
        rows = con.execute(
            """
            SELECT id, ts, answer, detail, raw, alert_id
            FROM feedback WHERE phone=?
            ORDER BY id DESC LIMIT ?;
            """,
            (phone, args.limit),
        ).fetchall()
        print("\nFeedback:")
        for r in rows:
            print(
                f'#{r["id"]:>5} {ts_to_str(r["ts"], utc=args.utc)} '
                f'ans={r["answer"]} det={r["detail"] or "-"} '
                f'alert_id={r["alert_id"] if "alert_id" in r.keys() else "-"} '
                f'raw="{(r["raw"] or "").strip()}"'
            )
    return 0


def cmd_stats(args: argparse.Namespace) -> int:
    con = connect_db(args.db)
    if not table_exists(con, "alerts"):
        eprint("ERROR: no alerts table.")
        return 2
    since_ts = parse_since(args.days, args.hours, args.since)
    params: List[Any] = []
    where = []
    if since_ts:
        where.append("ts >= ?")
        params.append(since_ts)
    if args.profile:
        where.append("profile = ?")
        params.append(args.profile)
    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    rows = con.execute(
        f"""
        SELECT profile,
               COUNT(1) AS alerts,
               SUM(CASE WHEN feedback_answer='TAK' THEN 1 ELSE 0 END) AS yes_cnt,
               SUM(CASE WHEN feedback_answer='NIE' THEN 1 ELSE 0 END) AS no_cnt,
               SUM(CASE WHEN feedback_answer IS NOT NULL THEN 1 ELSE 0 END) AS fb_cnt
        FROM alerts
        {where_sql}
        GROUP BY profile
        ORDER BY alerts DESC;
        """,
        params,
    ).fetchall()

    print("Stats (per profile)")
    for r in rows:
        alerts = int(r["alerts"] or 0)
        fb = int(r["fb_cnt"] or 0)
        yes = int(r["yes_cnt"] or 0)
        no = int(r["no_cnt"] or 0)
        rate = (100.0 * fb / alerts) if alerts else 0.0
        print(
            f'- {r["profile"]:<10} alerts={alerts:<5} feedback={fb:<5} (rate={rate:5.1f}%) '
            f'yes={yes:<4} no={no:<4}'
        )

    if args.profile in (None, "migraine"):
        params2 = params[:]
        where2 = []
        if since_ts:
            where2.append("ts >= ?")
        if args.profile is None:
            where2.append("profile = 'migraine'")
        else:
            where2.append("profile = ?")
            if args.profile != "migraine":
                return 0
        where2_sql = "WHERE " + " AND ".join(where2) if where2 else ""
        q = f"""
        SELECT feedback_detail AS detail,
               SUM(CASE WHEN feedback_answer='TAK' THEN 1 ELSE 0 END) AS yes_cnt,
               SUM(CASE WHEN feedback_answer='NIE' THEN 1 ELSE 0 END) AS no_cnt,
               SUM(CASE WHEN feedback_answer IS NOT NULL THEN 1 ELSE 0 END) AS fb_cnt
        FROM alerts
        {where2_sql}
        GROUP BY feedback_detail
        ORDER BY fb_cnt DESC;
        """
        rows2 = con.execute(q, params2).fetchall()
        print("\nMigraine feedback breakdown (detail)")
        for r in rows2:
            det = r["detail"] or "-"
            print(
                f'- {det:<8} feedback={int(r["fb_cnt"] or 0):<5} '
                f'yes={int(r["yes_cnt"] or 0):<4} no={int(r["no_cnt"] or 0):<4}'
            )
    return 0


def cmd_export_csv(args: argparse.Namespace) -> int:
    out_dir = Path(args.out).expanduser().resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    con = connect_db(args.db)
    since_ts = parse_since(args.days, args.hours, args.since)

    def dump(table: str, filename: str, where: str = "", params: List[Any] = []) -> None:
        if not table_exists(con, table):
            eprint(f"SKIP: no table {table}")
            return
        rows = con.execute(f"SELECT * FROM {table} {where} ORDER BY id ASC;", params).fetchall()
        path = out_dir / filename
        with path.open("w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(rows[0].keys() if rows else [])
            for r in rows:
                w.writerow([r[k] for k in r.keys()])
        print(f"OK: wrote {path} ({len(rows)} rows)")

    where = ""
    params: List[Any] = []
    if since_ts:
        where = "WHERE ts >= ?"
        params = [since_ts]

    dump("alerts", "alerts.csv", where, params)
    dump("feedback", "feedback.csv", where, params)

    if table_exists(con, "alerts") and table_exists(con, "feedback"):
        q = f"""
        SELECT
          a.id AS alert_id,
          a.ts AS alert_ts,
          a.phone,
          a.profile,
          a.location,
          a.score,
          a.threshold,
          a.label,
          a.feedback_answer,
          a.feedback_detail,
          a.feedback_ts,
          f.id AS feedback_id,
          f.ts AS fb_ts,
          f.answer AS fb_answer,
          f.detail AS fb_detail,
          f.raw AS fb_raw,
          f.remote_addr,
          f.user_agent
        FROM alerts a
        LEFT JOIN feedback f ON f.alert_id = a.id
        {('WHERE a.ts >= ?' if since_ts else '')}
        ORDER BY a.id ASC;
        """
        rows = con.execute(q, ([since_ts] if since_ts else [])).fetchall()
        path = out_dir / "joined.csv"
        with path.open("w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            if rows:
                w.writerow(rows[0].keys())
                for r in rows:
                    w.writerow([r[k] for k in r.keys()])
            else:
                w.writerow([])
        print(f"OK: wrote {path} ({len(rows)} rows)")
    return 0


def check_port(host: str, port: int, timeout: float = 0.6) -> str:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return "open"
    except OSError as ex:
        return f"closed ({ex.__class__.__name__})"


def apache_proxy_summary() -> List[str]:
    conf = Path("/etc/apache2/sites-enabled/dev.pracunia.pl.conf")
    if not conf.exists():
        return ["dev.pracunia.pl.conf not found"]
    txt = conf.read_text(encoding="utf-8", errors="replace").splitlines()
    proxy = [ln.strip() for ln in txt if ln.strip().startswith("ProxyPass ")]
    if not proxy:
        return ["no ProxyPass lines found"]
    return proxy


def systemd_units(prefix: str = "weatherguard") -> Tuple[List[str], List[str]]:
    rc, out = run_cmd(["systemctl", "list-units", "--type=service", "--all", "--no-pager"])
    services: List[str] = []
    if rc == 0:
        for ln in out.splitlines():
            if prefix in ln:
                services.append(ln)
    else:
        services.append(out)

    rc, out = run_cmd(["systemctl", "list-timers", "--all", "--no-pager"])
    timers: List[str] = []
    if rc == 0:
        for ln in out.splitlines():
            if prefix in ln:
                timers.append(ln)
    else:
        timers.append(out)
    return services, timers


def cmd_health(args: argparse.Namespace) -> int:
    kv: List[Tuple[str, str]] = []
    kv.append(("venv_python", "yes" if Path("/opt/weatherguard/venv/bin/python").exists() else "NO"))
    kv.append(("runner.py", "yes" if Path("/opt/weatherguard/app/runner.py").exists() else "NO"))
    kv.append(("webhook.py", "yes" if Path("/opt/weatherguard/app/webhook.py").exists() else "NO"))
    kv.append(
        (
            "feedback_store.py",
            "yes" if Path("/opt/weatherguard/app/feedback_store.py").exists() else "NO",
        )
    )
    kv.append(("port_8020_127.0.0.1", check_port("127.0.0.1", 8020)))
    kv.append(("port_8001_127.0.0.1", check_port("127.0.0.1", 8001)))

    services, timers = systemd_units("weatherguard")
    print_kv("health (quick)", kv)

    print("\n== systemd services (match 'weatherguard') ==")
    for ln in services or ["(none found)"]:
        print(ln)

    print("\n== systemd timers (match 'weatherguard') ==")
    for ln in timers or ["(none found)"]:
        print(ln)

    if args.verbose:
        units = []
        for ln in services:
            m = re.match(r"^(\S+)\s+", ln)
            if m:
                units.append(m.group(1))
        for u in units:
            print(f"\n== systemctl status {u} ==")
            _, out = run_cmd(["systemctl", "status", u, "--no-pager", "--full"], timeout=8)
            print(out)

        print("\n== apache ProxyPass summary (dev.pracunia.pl) ==")
        for ln in apache_proxy_summary():
            print(ln)

        if any("weatherguard-webhook.service" in s for s in services):
            print("\n== journalctl -u weatherguard-webhook.service (last 80) ==")
            _, out = run_cmd(
                ["journalctl", "-u", "weatherguard-webhook.service", "-n", "80", "--no-pager"],
                timeout=10,
            )
            print(out)

    print("\n== db schema check ==")
    db_path = args.db
    if not Path(db_path).exists():
        print(f"DB missing: {db_path}")
        return 2
    try:
        con = connect_db(db_path)
        needed = {
            "alerts": [
                "id",
                "ts",
                "phone",
                "profile",
                "score",
                "threshold",
                "feedback_answer",
                "feedback_detail",
                "feedback_ts",
            ],
            "feedback": ["id", "ts", "phone", "answer", "detail", "raw", "alert_id"],
        }
        for t, cols_need in needed.items():
            if not table_exists(con, t):
                print(f"- {t}: MISSING")
                continue
            cols_have = columns(con, t)
            miss = [c for c in cols_need if c not in cols_have]
            print(f"- {t}: OK" if not miss else f"- {t}: missing columns: {', '.join(miss)}")
    except Exception as ex:
        print("DB error:", repr(ex))
        return 2

    print("\nOK: health check finished")
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="wg-admin", description="WeatherGuard admin console dashboard")
    p.add_argument("--db", default=DEFAULT_DB, help=f"SQLite DB path (default: {DEFAULT_DB})")
    p.add_argument("--utc", action="store_true", help="Render timestamps in UTC (Z)")
    sub = p.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("doctor", help="Basic checks: DB present, tables, row counts")
    s.set_defaults(func=cmd_doctor)

    s = sub.add_parser(
        "health",
        help="Comprehensive health check (services, timers, ports, apache, db schema)",
    )
    s.add_argument("--verbose", action="store_true", help="Include systemctl status + apache + journal tails")
    s.set_defaults(func=cmd_health)

    def add_time_opts(sp: argparse.ArgumentParser) -> None:
        sp.add_argument("--days", type=int, default=None, help="Filter to last N days")
        sp.add_argument("--hours", type=int, default=None, help="Filter to last N hours")
        sp.add_argument(
            "--since",
            default=None,
            help="Filter since timestamp/date (e.g. 2026-02-16 or 2026-02-16T12:00)",
        )

    s = sub.add_parser("recent", help="Show recent alerts")
    s.add_argument("--limit", type=int, default=20)
    add_time_opts(s)
    s.set_defaults(func=cmd_recent)

    s = sub.add_parser("feedback", help="Show recent inbound feedback")
    s.add_argument("--limit", type=int, default=20)
    add_time_opts(s)
    s.set_defaults(func=cmd_feedback)

    s = sub.add_parser("users", help="List known users (phones) from DB")
    s.set_defaults(func=cmd_users)

    s = sub.add_parser("user", help="Show details for one phone")
    s.add_argument("phone")
    s.add_argument("--limit", type=int, default=50)
    s.set_defaults(func=cmd_user)

    s = sub.add_parser(
        "stats",
        help="Stats: alert counts, feedback rate, yes/no per profile, migraine detail breakdown",
    )
    s.add_argument("--profile", default=None, help="Filter to one profile (e.g. migraine)")
    add_time_opts(s)
    s.set_defaults(func=cmd_stats)

    s = sub.add_parser("export-csv", help="Export alerts/feedback/joined to CSV")
    s.add_argument("--out", default="./wg_export", help="Output directory")
    add_time_opts(s)
    s.set_defaults(func=cmd_export_csv)

    return p


def main(argv: Optional[List[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
