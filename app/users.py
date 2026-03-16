from __future__ import annotations
from dataclasses import dataclass
from typing import List, Optional, Tuple
import re

@dataclass
class UserConfig:
    phone: str                    # +48...
    profile: str                  # migraine / heart / both
    location: str                 # lat,lon OR City,CC
    threshold: Optional[int] = None
    quiet_hours: Optional[str] = None

def parse_quiet_hours(qh: Optional[str]) -> Optional[Tuple[int,int]]:
    if not qh:
        return None
    m = re.match(r"^\s*(\d{1,2})\s*-\s*(\d{1,2})\s*$", qh)
    if not m:
        return None
    start = int(m.group(1)); end = int(m.group(2))
    if not (0 <= start <= 23 and 0 <= end <= 23):
        return None
    return start, end

def load_users(path: str) -> List[UserConfig]:
    users: List[UserConfig] = []
    with open(path, "r", encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            parts = [p.strip() for p in line.split("|")]
            # allow missing fields at end
            while len(parts) < 5:
                parts.append("")
            phone, profile, location, threshold_s, quiet = parts[:5]
            profile = profile.lower()
            if profile not in ("migraine", "heart", "both"):
                raise ValueError(f"Invalid profile '{profile}' in line: {raw}")
            if not phone.startswith(""):
                raise ValueError(f"Phone must start with '' in line: {raw}")
            threshold = int(threshold_s) if threshold_s.strip() else None
            quiet_hours = quiet.strip() or None
            users.append(UserConfig(phone=phone.strip(), profile=profile, location=location.strip(), threshold=threshold, quiet_hours=quiet_hours))
    return users
