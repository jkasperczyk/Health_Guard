import os
from typing import Optional, Tuple

def format_trend_delivery(text: str, trend_url: Optional[str]) -> Tuple[str, Optional[str]]:
    mode = os.environ.get("SMS_TRENDS_MODE", "link").lower().strip()
    if not trend_url:
        return text, None
    if mode == "mms":
        return text, trend_url
    return f"{text}\n\nTrend: {trend_url}", None
