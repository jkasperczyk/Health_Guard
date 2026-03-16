import os
import json
import re
from datetime import datetime, timezone, date
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs
from typing import Dict, Any, Optional, Tuple

import requests
from twilio.request_validator import RequestValidator

from .sms_state import (
    ensure_user, set_subscribed, is_sms_subscribed, update_factor, clear_factors, get_user
)

def _utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00","Z")

def _twiml(msg: str) -> bytes:
    esc = (msg.replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")
              .replace('"',"&quot;").replace("'","&apos;"))
    xml = f'<?xml version="1.0" encoding="UTF-8"?><Response><Message>{esc}</Message></Response>'
    return xml.encode("utf-8")

def _append_jsonl(path: str, payload: Dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False) + "\n")

def _normalize(s: str) -> str:
    s = (s or "").strip()
    repl = {
        "Ó":"O","ó":"o","Ł":"L","ł":"l","Ś":"S","ś":"s","Ż":"Z","ż":"z","Ź":"Z","ź":"z",
        "Ć":"C","ć":"c","Ń":"N","ń":"n","Ą":"A","ą":"a","Ę":"E","ę":"e",
    }
    for a,b in repl.items():
        s = s.replace(a,b)
    return s.strip()

def _parse_date_yyyy_mm_dd(s: str) -> Optional[str]:
    try:
        parts = s.split("-")
        if len(parts) != 3:
            return None
        y, m, d = int(parts[0]), int(parts[1]), int(parts[2])
        dt = date(y, m, d)
        return dt.isoformat()
    except Exception:
        return None

def parse_command(body: str) -> Dict[str, Any]:
    raw = (body or "").strip()
    u = _normalize(raw).upper()

    # core
    if u in {"HELP", "POMOC"}:
        return {"type": "help"}
    if u in {"STATUS", "STAT"}:
        return {"type": "status"}
    if u in {"STOP", "STOPALL", "UNSUBSCRIBE", "CANCEL", "END", "QUIT"}:
        return {"type": "stop"}
    if u in {"START", "YES", "UNSTOP"}:
        return {"type": "start"}
    if u in {"ACK", "OK", "TAK"}:
        return {"type": "ack"}
    if u in {"NIE", "NO", "FALSE", "FALSZ"}:
        return {"type": "no"}
    if u in {"TREND", "TRENDS", "WYKRES", "WYKRESY"}:
        return {"type": "trend"}
    if u in {"PROFILE", "PROFIL"}:
        return {"type": "profile"}
    if u in {"USUN DANE", "USUN", "DELETE DATA"}:
        return {"type": "clear"}

    # delay: OPOZNIJ 30
    m = re.search(r"(OPOZNIJ|DELAY)\s+(\d{1,4})", u)
    if m:
        return {"type": "delay", "minutes": int(m.group(2))}

    # stress: STRES 7 / STRESS 7 / S7
    m = re.search(r"^(STRES|STRESS)\s+(\d{1,2})$", u)
    if m:
        v = int(m.group(2))
        v = max(0, min(10, v))
        return {"type": "stress", "value": v}
    m = re.search(r"^S(\d{1,2})$", u)
    if m:
        v = int(m.group(1))
        v = max(0, min(10, v))
        return {"type": "stress", "value": v}

    # exercise: WYSILEK 2 / EXERCISE 2 / E2
    m = re.search(r"^(WYSILEK|WYSIŁEK|EXERCISE)\s+(\d)$", u)
    if m:
        v = int(m.group(2))
        v = max(0, min(3, v))
        return {"type": "exercise", "value": v}
    m = re.search(r"^E(\d)$", u)
    if m:
        v = int(m.group(1))
        v = max(0, min(3, v))
        return {"type": "exercise", "value": v}

    # period: OKRES [YYYY-MM-DD]
    m = re.search(r"^OKRES(?:\s+([0-9]{4}-[0-9]{2}-[0-9]{2}))?$", u)
    if m:
        ds = m.group(1)
        if ds:
            parsed = _parse_date_yyyy_mm_dd(ds)
            if not parsed:
                return {"type": "period", "error": "bad_date"}
            return {"type": "period", "date": parsed}
        else:
            return {"type": "period", "date": date.today().isoformat()}

    # cycle: CYKL 28 / CYKL OFF
    m = re.search(r"^CYKL\s+OFF$", u)
    if m:
        return {"type": "cycle_off"}
    m = re.search(r"^CYKL\s+(\d{2})$", u)
    if m:
        days = int(m.group(1))
        days = max(20, min(45, days))
        return {"type": "cycle_days", "days": days}

    return {"type": None, "raw": raw}

def validate_signature(full_url: str, params: Dict[str, str], signature: str) -> bool:
    if os.environ.get("DISABLE_TWILIO_SIGNATURE_VALIDATION", "0") == "1":
        return True
    token = os.environ.get("TWILIO_AUTH_TOKEN", "")
    if not token:
        return False
    return RequestValidator(token).validate(full_url, params, signature)

def latest_trend_link() -> Optional[str]:
    d = os.environ.get("TRENDS_PUBLIC_DIR", "/var/www/html/weatherguard/trends")
    base = os.environ.get("TRENDS_PUBLIC_URL_BASE", "").rstrip("/")
    glob_pat = os.environ.get("TRENDS_GLOB", "*.png")
    try:
        import glob
        import os as _os
        files = glob.glob(_os.path.join(d, glob_pat))
        if not files:
            return None
        files.sort(key=lambda p: _os.path.getmtime(p), reverse=True)
        fname = _os.path.basename(files[0])
        return f"{base}/{fname}" if base else fname
    except Exception:
        return None

def forward_to_legacy(legacy_url: str, raw_body: bytes) -> Tuple[bool, Optional[int], Optional[bytes]]:
    try:
        r = requests.post(
            legacy_url,
            data=raw_body,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=10
        )
        return True, r.status_code, r.content
    except Exception:
        return False, None, None

def help_text() -> str:
    return (
        "Komendy: HELP, STATUS, STOP, START, TREND, ACK, NIE, OPÓŹNIJ 30, "
        "STRES 0-10, WYSILEK 0-3, OKRES [YYYY-MM-DD], CYKL 28, CYKL OFF, PROFILE, USUN DANE."
    )

def profile_text(phone: str) -> str:
    u = get_user(phone)
    sub = "WLACZONE" if bool(u.get("subscribed", True)) else "WYLACZONE"
    factors = u.get("factors") or {}
    s = factors.get("stress", {}).get("value")
    e = factors.get("exercise", {}).get("value")
    cyc = factors.get("cycle", {})
    cycle_on = cyc.get("opt_in", False)
    last_start = cyc.get("last_period_start")
    cycle_days = cyc.get("cycle_days")
    parts = [f"Alerty SMS: {sub}"]
    if s is not None:
        parts.append(f"Stres: {s}/10")
    if e is not None:
        parts.append(f"Wysilek: {e}/3")
    if cycle_on:
        parts.append(f"Cykl: ON (start={last_start}, dl={cycle_days})")
    else:
        parts.append("Cykl: OFF")
    return " | ".join(parts)

class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        path = self.path.split("?")[0]
        if path not in ("/twilio/sms/inbound", "/twilio/sms/inbound/") and not path.startswith("/twilio/sms/"):
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b"not found")
            return

        length = int(self.headers.get("Content-Length", "0") or "0")
        raw = self.rfile.read(length)

        qs = parse_qs(raw.decode("utf-8", errors="ignore"))
        params = {k: (v[0] if v else "") for k, v in qs.items()}

        signature = self.headers.get("X-Twilio-Signature", "")
        public_base = os.environ.get("PUBLIC_BASE_URL", "").rstrip("/")
        inbound_path = os.environ.get("TWILIO_SMS_INBOUND_PATH", "/twilio/sms/inbound")
        full_url = (public_base + inbound_path) if public_base else (f"http://{self.headers.get('Host','localhost')}{inbound_path}")

        if not validate_signature(full_url, params, signature):
            self.send_response(403)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(b"invalid signature")
            return

        from_phone = (params.get("From") or "").strip()
        to_phone = (params.get("To") or "").strip()
        body = (params.get("Body") or "").strip()
        msg_sid = params.get("MessageSid")

        ensure_user(from_phone)
        cmd = parse_command(body)

        log_payload: Dict[str, Any] = {
            "received_at": _utc_iso(),
            "from": from_phone,
            "to": to_phone,
            "body": body,
            "message_sid": msg_sid,
            "parsed": cmd,
            "handled_by": "gateway",
            "forwarded_to_legacy": False,
            "legacy_status": None,
        }

        reply_xml: Optional[bytes] = None

        t = cmd.get("type")
        if t == "help":
            reply_xml = _twiml(help_text())
        elif t == "status":
            reply_xml = _twiml(profile_text(from_phone) + " | " + help_text())
        elif t == "stop":
            set_subscribed(from_phone, False)
            reply_xml = _twiml("OK. Alerty SMS wylaczone. START aby wlaczyc.")
        elif t == "start":
            set_subscribed(from_phone, True)
            reply_xml = _twiml("OK. Alerty SMS wlaczone.")
        elif t == "trend":
            link = latest_trend_link()
            reply_xml = _twiml(f"Trend: {link}" if link else "Brak trendow do pobrania.")
        elif t == "ack":
            reply_xml = _twiml("Dzieki! Potwierdzone 👍")
        elif t == "no":
            reply_xml = _twiml("OK, zapisane jako falszywy alarm. Dzieki!")
        elif t == "delay":
            mins = cmd.get("minutes")
            update_factor(from_phone, "delay", mins, extra={})
            reply_xml = _twiml(f"OK, zapisane: opoznij {mins} min.")
        elif t == "stress":
            update_factor(from_phone, "stress", cmd.get("value"))
            reply_xml = _twiml(f"OK. Stres zapisany: {cmd.get('value')}/10")
        elif t == "exercise":
            update_factor(from_phone, "exercise", cmd.get("value"))
            reply_xml = _twiml(f"OK. Wysilek zapisany: {cmd.get('value')}/3")
        elif t == "period":
            if cmd.get("error") == "bad_date":
                reply_xml = _twiml("Zly format daty. Uzyj: OKRES YYYY-MM-DD")
            else:
                # ensure cycle section
                u = get_user(from_phone)
                cycle = (u.get("factors") or {}).get("cycle") or {}
                cycle = dict(cycle)
                cycle["opt_in"] = True
                cycle["last_period_start"] = cmd.get("date")
                # keep cycle_days if exists else default 28
                cycle.setdefault("cycle_days", 28)
                cycle["updated_at"] = _utc_iso()
                # store as full object under factors.cycle
                update_factor(from_phone, "cycle", None, extra=cycle)
                reply_xml = _twiml(f"OK. OKRES ustawiony: {cmd.get('date')} (CYKL {cycle.get('cycle_days')})")
        elif t == "cycle_days":
            u = get_user(from_phone)
            cycle = (u.get("factors") or {}).get("cycle") or {}
            cycle = dict(cycle)
            cycle["opt_in"] = True
            cycle["cycle_days"] = cmd.get("days")
            cycle.setdefault("last_period_start", date.today().isoformat())
            cycle["updated_at"] = _utc_iso()
            update_factor(from_phone, "cycle", None, extra=cycle)
            reply_xml = _twiml(f"OK. Dlugosc cyklu: {cmd.get('days')} dni. (OKRES {cycle.get('last_period_start')})")
        elif t == "cycle_off":
            u = get_user(from_phone)
            cycle = (u.get("factors") or {}).get("cycle") or {}
            cycle = dict(cycle)
            cycle["opt_in"] = False
            cycle["updated_at"] = _utc_iso()
            update_factor(from_phone, "cycle", None, extra=cycle)
            reply_xml = _twiml("OK. Cykl: OFF")
        elif t == "profile":
            reply_xml = _twiml(profile_text(from_phone))
        elif t == "clear":
            clear_factors(from_phone)
            reply_xml = _twiml("OK. Usunieto dane profilu (stres/wysilek/cykl).")

        if reply_xml is None:
            # not a command -> preserve old symptom/signal flow
            legacy = os.environ.get("LEGACY_WEBHOOK_URL", "").strip()
            if legacy:
                ok, status, content = forward_to_legacy(legacy, raw)
                log_payload["forwarded_to_legacy"] = ok
                log_payload["legacy_status"] = status
                if ok and status and 200 <= status < 300 and content:
                    # legacy returns TwiML - pass through
                    reply_xml = content
                else:
                    reply_xml = _twiml("OK. (Komendy: HELP)")
            else:
                # no legacy configured - do not break, just confirm
                reply_xml = _twiml("OK. (Komendy: HELP)")

        _append_jsonl(os.environ.get("SMS_FEEDBACK_LOG", "/opt/weatherguard/data/sms_feedback.jsonl"), log_payload)

        self.send_response(200)
        self.send_header("Content-Type", "application/xml")
        self.end_headers()
        self.wfile.write(reply_xml)

def main():
    port = int(os.environ.get("WG_SMS_WEBHOOK_PORT", "5055"))
    httpd = HTTPServer(("127.0.0.1", port), Handler)
    print(f"[weatherguard-sms-webhook v4] listening on 127.0.0.1:{port}")
    httpd.serve_forever()

if __name__ == "__main__":
    main()
