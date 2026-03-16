from typing import Optional, Dict, Any
from .sms_twilio import send_sms
from .sms_trends import format_trend_delivery

def send_weather_sms(to: str, body: str, trend_url: Optional[str] = None) -> Dict[str, Any]:
    final_body, media_url = format_trend_delivery(body, trend_url)
    return send_sms(to=to, body=final_body, media_url=media_url)
