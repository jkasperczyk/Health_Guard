import os
from typing import Optional, Dict, Any
from twilio.rest import Client

def _client() -> Client:
    sid = os.environ["TWILIO_ACCOUNT_SID"]
    token = os.environ["TWILIO_AUTH_TOKEN"]
    return Client(sid, token)

def send_sms(to: str, body: str, media_url: Optional[str] = None) -> Dict[str, Any]:
    client = _client()
    from_number = os.environ.get("TWILIO_FROM")
    msg_service = os.environ.get("TWILIO_MESSAGING_SERVICE_SID")

    kwargs: Dict[str, Any] = {"to": to, "body": body}
    if media_url:
        kwargs["media_url"] = [media_url]

    if msg_service:
        kwargs["messaging_service_sid"] = msg_service
    elif from_number:
        kwargs["from_"] = from_number
    else:
        raise RuntimeError("Set TWILIO_FROM or TWILIO_MESSAGING_SERVICE_SID")

    m = client.messages.create(**kwargs)
    return {"sid": m.sid, "status": m.status}
