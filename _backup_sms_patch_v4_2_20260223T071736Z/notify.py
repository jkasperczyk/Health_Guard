from __future__ import annotations
from typing import Optional
from twilio.rest import Client
from weatherguard.sms_state import is_sms_subscribed

def send_whatsapp(account_sid: str, auth_token: str, from_ str, to_ str, body: str) -> str:
    client = Client(account_sid, auth_token)

        # [WG-SMS v4] subscription guard
        try:
            if not is_sms_subscribed(to_whatsapp):
                return None
        except Exception:
            pass
    msg = client.messages.create(
        from_=from_whatsapp,
        to=to_whatsapp,
        body=body,
    )
    return msg.sid
