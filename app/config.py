from __future__ import annotations

import os
from dataclasses import dataclass
from typing import List, Optional, Tuple

def _load_env_file(path: Optional[str]) -> None:
    if not path or not os.path.exists(path):
        return
    with open(path, "r", encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            k = k.strip()
            v = v.strip().strip('"').strip("'")
            if k and k not in os.environ:
                os.environ[k] = v

def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)).strip())
    except Exception:
        return default

def _env_str(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()

@dataclass
class Settings:
    users_file: str
    log_dir: str
    state_db: str

    ai_mode: str              # off / auto / on
    openai_api_key: str
    openai_model: str

    twilio_account_sid: str
    twilio_auth_token: str
    twilio_sms_from: str

    forecast_hours: int

    default_threshold_migraine: int
    default_threshold_heart: int
    default_threshold_allergy: int
    cooldown_minutes: int

@dataclass
class UserCfg:
    phone: str
    profile: str
    location: str
    threshold: Optional[int] = None
    quiet_hours: Optional[str] = None

def load_settings(env_path: Optional[str] = None) -> Settings:
    _load_env_file(env_path)

    base_dir = _env_str("BASE_DIR", "/opt/weatherguard")
    cfg_dir  = _env_str("CONFIG_DIR", f"{base_dir}/config")

    users_file = _env_str("USERS_FILE", f"{cfg_dir}/users.txt")
    log_dir    = _env_str("LOG_DIR", f"{base_dir}/logs")
    state_db   = _env_str("STATE_DB", f"{base_dir}/state.json")

    ai_mode = _env_str("AI_MODE", "auto").lower()
    if ai_mode not in ("off", "auto", "on"):
        ai_mode = "auto"

    return Settings(
        users_file=users_file,
        log_dir=log_dir,
        state_db=state_db,

        ai_mode=ai_mode,
        openai_api_key=_env_str("OPENAI_API_KEY", ""),
        openai_model=_env_str("OPENAI_MODEL", "gpt-4o-mini"),

        twilio_account_sid=_env_str("TWILIO_ACCOUNT_SID", ""),
        twilio_auth_token=_env_str("TWILIO_AUTH_TOKEN", ""),
        twilio_sms_from=_env_str("TWILIO_FROM", ""),

        forecast_hours=_env_int("FORECAST_HOURS", 24),

        default_threshold_migraine=_env_int("DEFAULT_THRESHOLD_MIGRAINE", 65),
        default_threshold_heart=_env_int("DEFAULT_THRESHOLD_HEART", 70),
        default_threshold_allergy=_env_int("DEFAULT_THRESHOLD_ALLERGY", 60),
        cooldown_minutes=_env_int("COOLDOWN_MINUTES", 360),
    )

def parse_users(users_file: str) -> List[UserCfg]:
    users: List[UserCfg] = []
    if not os.path.exists(users_file):
        return users

    allowed = {"migraine", "heart", "both", "allergy"}

    with open(users_file, "r", encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            parts = [p.strip() for p in line.split("|")]
            while len(parts) < 5:
                parts.append("")

            phone = parts[0]
            profile = parts[1].lower()
            location = parts[2]
            th_raw = parts[3]
            qh_raw = parts[4]

            if not phone or not profile or not location:
                continue
            if profile not in allowed:
                continue

            threshold = None
            if th_raw:
                try:
                    threshold = int(th_raw)
                except Exception:
                    threshold = None

            quiet_hours = qh_raw if qh_raw else None

            users.append(UserCfg(
                phone=phone,
                profile=profile,
                location=location,
                threshold=threshold,
                quiet_hours=quiet_hours
            ))
    return users

def parse_quiet_hours(qh: Optional[str]) -> Optional[Tuple[int, int]]:
    if not qh:
        return None
    s = qh.strip()
    if not s or "-" not in s:
        return None
    a, b = [x.strip() for x in s.split("-", 1)]
    try:
        start = int(a); end = int(b)
        if not (0 <= start <= 23 and 0 <= end <= 23):
            return None
        return (start, end)
    except Exception:
        return None

def is_in_quiet_hours(hour: int, qh: Tuple[int, int]) -> bool:
    start, end = qh
    if start < end:
        return start <= hour < end
    if start > end:
        return (hour >= start) or (hour < end)
    return True
