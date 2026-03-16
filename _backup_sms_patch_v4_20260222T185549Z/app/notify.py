from __future__ import annotations
from typing import Optional
from twilio.rest import Client

def send_whatsapp(account_sid: str, auth_token: str, from_ str, to_ str, body: str) -> str:
    client = Client(account_sid, auth_token)
    msg = client.messages.create(
        from_=from_whatsapp,
        to=to_whatsapp,
        body=body,
    )
    return msg.sid
