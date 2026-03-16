import re
from typing import Dict, Any

def parse_sms(text: str) -> Dict[str, Any]:
    raw = (text or "").strip()
    upper = raw.upper()

    if upper in {"STOP", "STOPALL", "UNSUBSCRIBE", "CANCEL", "END", "QUIT"}:
        return {"type": "stop", "delay_minutes": None}
    if upper in {"START", "YES", "UNSTOP"}:
        return {"type": "start", "delay_minutes": None}
    if upper in {"HELP", "POMOC"}:
        return {"type": "help", "delay_minutes": None}
    if upper in {"STATUS", "STAT"}:
        return {"type": "status", "delay_minutes": None}
    if upper in {"ACK", "OK", "TAK"}:
        return {"type": "ack", "delay_minutes": None}
    if upper in {"NIE", "NO", "FALSE", "FALSZ", "FAŁSZ"}:
        return {"type": "no", "delay_minutes": None}

    m = re.search(r"(OPÓŹNIJ|OPOZNIJ|DELAY)\s+(\d{1,4})", upper)
    if m:
        return {"type": "delay", "delay_minutes": int(m.group(2))}

    return {"type": "text", "delay_minutes": None, "text": raw}
