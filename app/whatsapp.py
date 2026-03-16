from __future__ import annotations

import requests
from typing import Optional

class WhatsAppSender:
    """
    Minimalny sender WhatsApp przez Twilio REST API.
    Wymaga:
      - account_sid
      - auth_token
      - from_number (np. +14155238886 dla sandbox)
    """
    def __init__(self, account_sid: str, auth_token: str, from_number: str):
        self.account_sid = account_sid.strip()
        self.auth_token = auth_token.strip()
        self.from_number = from_number.strip()
        if not (self.account_sid and self.auth_token and self.from_number):
            raise ValueError("WhatsAppSender: missing Twilio credentials / from number")

    def send(self, to_number: str, body: str) -> str:
        to_number = to_number.strip()
        if not to_number.startswith(""):
            raise ValueError("Twilio WhatsApp requires 'To' in format +48...")

        url = f"https://api.twilio.com/2010-04-01/Accounts/{self.account_sid}/Messages.json"
        data = {
            "From": self.from_number,
            "To": to_number,
            "Body": body,
        }
        r = requests.post(url, data=data, auth=(self.account_sid, self.auth_token), timeout=30)
        # Twilio zwraca JSON również przy błędach
        try:
            payload = r.json()
        except Exception:
            payload = {"raw": r.text}

        if r.status_code >= 400:
            msg = payload.get("message") or payload.get("error_message") or str(payload)
            raise RuntimeError(f"Twilio send failed HTTP {r.status_code}: {msg}")

        sid = payload.get("sid")
        return sid or "unknown"
