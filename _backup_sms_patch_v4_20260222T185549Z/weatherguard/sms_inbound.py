import json
import os
from datetime import datetime, timezone
from typing import Any, Dict

from flask import request, Response
from twilio.request_validator import RequestValidator

from .sms_parser import parse_sms

def _utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

def _log_feedback(payload: Dict[str, Any]) -> None:
    path = os.environ.get("SMS_FEEDBACK_LOG", "./data/sms_feedback.jsonl")
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False) + "\n")

def _twiml(msg: str) -> Response:
    esc = (msg.replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")
              .replace('"',"&quot;").replace("'","&apos;"))
    xml = f'<?xml version="1.0" encoding="UTF-8"?><Response><Message>{esc}</Message></Response>'
    return Response(xml, mimetype="application/xml")

def _validate_twilio_signature(full_url: str, params: Dict[str, Any], signature: str) -> bool:
    if os.environ.get("DISABLE_TWILIO_SIGNATURE_VALIDATION", "0") == "1":
        return True
    token = os.environ.get("TWILIO_AUTH_TOKEN", "")
    if not token:
        return False
    return RequestValidator(token).validate(full_url, params, signature)

def register_sms_inbound(app):
    """Register /twilio/sms/inbound route in an existing Flask app."""

    @app.post(os.environ.get("TWILIO_SMS_INBOUND_PATH", "/twilio/sms/inbound"))
    def wg_twilio_sms_inbound():
        params = request.form.to_dict(flat=True)
        signature = request.headers.get("X-Twilio-Signature", "")

        base = os.environ.get("PUBLIC_BASE_URL", "").rstrip("/")
        inbound_path = os.environ.get("TWILIO_SMS_INBOUND_PATH", "/twilio/sms/inbound")
        full_url = (base + inbound_path) if base else request.url

        if not _validate_twilio_signature(full_url, params, signature):
            return Response("invalid signature", status=403)

        payload = {
            "received_at": _utc_iso(),
            "from": (params.get("From") or "").strip(),
            "to": (params.get("To") or "").strip(),
            "body": (params.get("Body") or "").strip(),
            "message_sid": params.get("MessageSid"),
        }

        parsed = parse_sms(payload["body"])
        payload["parsed"] = parsed

        _log_feedback(payload)

        t = parsed.get("type")
        if t == "stop":
            reply = "OK. Alerty SMS wyłączone. Aby włączyć: START."
        elif t == "start":
            reply = "OK. Alerty SMS włączone. Komendy: STATUS / STOP / HELP."
        elif t == "status":
            reply = "Status: OK. Komendy: ACK / NIE / OPÓŹNIJ 30 / STOP / START / HELP."
        elif t == "help":
            reply = "Komendy: ACK, NIE, OPÓŹNIJ 30, STATUS, STOP, START."
        elif t == "ack":
            reply = "Dzięki! Potwierdzone 👍"
        elif t == "no":
            reply = "OK, zapisane jako fałszywy alarm. Dzięki!"
        elif t == "delay":
            reply = f"OK, zapisane: ponów za {parsed.get('delay_minutes')} min."
        else:
            reply = "Dzięki! Zapisano wiadomość. (HELP)"

        return _twiml(reply)

    return app
