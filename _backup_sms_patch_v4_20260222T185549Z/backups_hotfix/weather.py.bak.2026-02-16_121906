from __future__ import annotations

import json
import math
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

try:
    import httpx
except Exception:  # pragma: no cover
    httpx = None  # type: ignore

# --- Open-Meteo endpoints ---
OPEN_METEO_GEOCODE = "https://geocoding-api.open-meteo.com/v1/search"
OPEN_METEO_FORECAST = "https://api.open-meteo.com/v1/forecast"
OPEN_METEO_AIR_QUALITY = "https://air-quality-api.open-meteo.com/v1/air-quality"

# --- External sources (PL / global) ---
IMGW_WARNINGS_METEO = "https://danepubliczne.imgw.pl/api/data/warningsmeteo"
NOAA_KP_1M = "https://services.swpc.noaa.gov/json/planetary_k_index_1m.json"

GIOS_STATIONS_V1 = "https://api.gios.gov.pl/pjp-api/v1/rest/station/findAll"
GIOS_INDEX_V1 = "https://api.gios.gov.pl/pjp-api/v1/rest/aqindex/getIndex/{stationId}"
GIOS_STATIONS_OLD = "https://api.gios.gov.pl/pjp-api/rest/station/findAll"
GIOS_INDEX_OLD = "https://api.gios.gov.pl/pjp-api/rest/aqindex/getIndex/{stationId}"

GOOGLE_POLLEN_FORECAST = "https://pollen.googleapis.com/v1/forecast:lookup"

# Optional reverse geocoding (to get voivodeship/state name if missing)
NOMINATIM_REVERSE = "https://nominatim.openstreetmap.org/reverse"

# Cache files
DEFAULT_CACHE_DIR = Path(os.getenv("WEATHERGUARD_CACHE_DIR", "/opt/weatherguard/data"))
DEFAULT_CACHE_DIR.mkdir(parents=True, exist_ok=True)
GIOS_CACHE_FILE = DEFAULT_CACHE_DIR / "gios_stations_cache.json"

# Poland voivodeship mapping (TERYT first 2 digits)
VOIVODESHIP_CODE_BY_NAME = {
    # English -> code
    "Lower Silesia": "02",
    "Kuyavia-Pomerania": "04",
    "Lublin": "06",
    "Lubusz": "08",
    "Łódź": "10",
    "Lodz": "10",
    "Lesser Poland": "12",
    "Masovia": "14",
    "Opole": "16",
    "Podkarpackie": "18",
    "Podlaskie": "20",
    "Pomerania": "22",
    "Silesia": "24",
    "Świętokrzyskie": "26",
    "Warmian-Masuria": "28",
    "Greater Poland": "30",
    "West Pomerania": "32",
    # Polish -> code
    "Dolnośląskie": "02",
    "Kujawsko-Pomorskie": "04",
    "Lubelskie": "06",
    "Lubuskie": "08",
    "Łódzkie": "10",
    "Małopolskie": "12",
    "Mazowieckie": "14",
    "Opolskie": "16",
    "Podkarpackie": "18",
    "Podlaskie": "20",
    "Pomorskie": "22",
    "Śląskie": "24",
    "Swietokrzyskie": "26",
    "Świętokrzyskie": "26",
    "Warmińsko-Mazurskie": "28",
    "Wielkopolskie": "30",
    "Zachodniopomorskie": "32",
}


@dataclass
class Location:
    name: str
    latitude: float
    longitude: float
    country_code: Optional[str] = None
    admin1: Optional[str] = None


def _http_get_json(url: str, params: Optional[dict] = None, headers: Optional[dict] = None, timeout: float = 20.0) -> Any:
    if httpx is None:
        raise RuntimeError("httpx is not available in this environment")
    with httpx.Client(timeout=timeout, follow_redirects=True) as client:
        r = client.get(url, params=params, headers=headers)
        r.raise_for_status()
        return r.json()


def _reverse_admin1(lat: float, lon: float) -> Optional[str]:
    """Best-effort: resolve 'admin1/state' name via Nominatim."""
    try:
        data = _http_get_json(
            NOMINATIM_REVERSE,
            params={"format": "jsonv2", "lat": f"{lat}", "lon": f"{lon}"},
            headers={"User-Agent": "WeatherGuard/1.0"},
            timeout=15.0,
        )
        addr = (data or {}).get("address") or {}
        return (addr.get("state") or addr.get("region") or addr.get("province"))
    except Exception:
        return None



def _is_latlon(s: str) -> bool:
    if not s or "," not in s:
        return False
    a, b = [p.strip() for p in s.split(",", 1)]
    def _num(x: str) -> bool:
        try:
            float(x)
            return True
        except Exception:
            return False
    return _num(a) and _num(b)


def resolve_location(location: str) -> Location:
    """Resolve location string into coordinates.

    Supports:
      - "lat,lon"
      - "City" or "City,CC"
    """
    loc = location.strip()
    if _is_latlon(loc):
        lat_s, lon_s = [p.strip() for p in loc.split(",", 1)]
        return Location(name=loc, latitude=float(lat_s), longitude=float(lon_s), country_code=None, admin1=None)

    name = loc
    country_code = None
    if "," in loc:
        a, b = [p.strip() for p in loc.split(",", 1)]
        if b and len(b) == 2 and b.isalpha():
            name = a
            country_code = b.upper()

    params = {"name": name, "count": 1, "language": "pl", "format": "json"}
    if country_code:
        params["country_code"] = country_code

    data = _http_get_json(OPEN_METEO_GEOCODE, params=params)
    results = data.get("results") or []
    if not results:
        raise RuntimeError(f"Geocoding failed for location='{location}'")

    top = results[0]
    resolved_name = ", ".join([p for p in [top.get("name"), top.get("admin1"), top.get("country_code")] if p])
    return Location(
        name=resolved_name,
        latitude=float(top["latitude"]),
        longitude=float(top["longitude"]),
        country_code=top.get("country_code"),
        admin1=top.get("admin1"),
    )


def fetch_weather(lat: float, lon: float, hours: int = 48) -> Dict[str, Any]:
    hourly = ",".join([
        "temperature_2m",
        "apparent_temperature",
        "relative_humidity_2m",
        "surface_pressure",
        "wind_gusts_10m",
        "precipitation_probability",
        "precipitation",
        "uv_index",
        "shortwave_radiation",
        "cloud_cover",
        "cape",
        "weather_code",
    ])
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": hourly,
        "forecast_days": max(2, int(math.ceil(hours / 24))),
        "timezone": "UTC",
        "timeformat": "iso8601",
        "windspeed_unit": "kmh",
        "temperature_unit": "celsius",
    }
    return _http_get_json(OPEN_METEO_FORECAST, params=params)


def fetch_air_quality(lat: float, lon: float) -> Dict[str, Any]:
    # CAMS Europe is usually best for Poland.
    hourly = ",".join([
        "pm2_5",
        "us_aqi",
        # pollen (may be empty/out-of-season)
        "alder_pollen",
        "birch_pollen",
        "grass_pollen",
        "mugwort_pollen",
        "olive_pollen",
        "ragweed_pollen",
    ])
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": hourly,
        "timezone": "UTC",
        "timeformat": "iso8601",
        "domains": os.getenv("OPEN_METEO_AIR_DOMAIN", "cams_europe"),
    }
    return _http_get_json(OPEN_METEO_AIR_QUALITY, params=params)


def _parse_time_utc(ts: str) -> datetime:
    # Open-Meteo often returns naive ISO strings when timezone=UTC.
    # If naive -> treat as UTC.
    t = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    if t.tzinfo is None:
        t = t.replace(tzinfo=timezone.utc)
    return t.astimezone(timezone.utc)


def _find_idx0(times: List[str]) -> int:
    now = datetime.now(timezone.utc)
    best = 0
    for i, ts in enumerate(times):
        try:
            dt = _parse_time_utc(ts)
        except Exception:
            continue
        if dt >= now:
            return i
        best = i
    return best


def _slice(arr: List[Optional[float]], i0: int, n: int) -> List[float]:
    out: List[float] = []
    for x in arr[i0:i0+n]:
        if x is None:
            continue
        try:
            out.append(float(x))
        except Exception:
            pass
    return out


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(p1)*math.cos(p2)*math.sin(dl/2)**2
    return 2*r*math.asin(math.sqrt(a))


def _load_gios_stations(max_age_hours: int = 24*7) -> List[dict]:
    try:
        if GIOS_CACHE_FILE.exists():
            raw = json.loads(GIOS_CACHE_FILE.read_text(encoding="utf-8"))
            ts = raw.get("cached_at")
            if ts:
                cached_at = datetime.fromisoformat(ts)
                if cached_at.tzinfo is None:
                    cached_at = cached_at.replace(tzinfo=timezone.utc)
                age_h = (datetime.now(timezone.utc) - cached_at).total_seconds()/3600.0
                if age_h <= max_age_hours:
                    return raw.get("stations") or []
    except Exception:
        pass

    # Fetch fresh
    stations = None
    for url in (GIOS_STATIONS_V1, GIOS_STATIONS_OLD):
        try:
            stations = _http_get_json(url)
            break
        except Exception:
            continue
    if stations is None:
        return []

    try:
        GIOS_CACHE_FILE.write_text(json.dumps({
            "cached_at": datetime.now(timezone.utc).isoformat(),
            "stations": stations,
        }, ensure_ascii=False), encoding="utf-8")
    except Exception:
        pass

    return stations


def fetch_gios_aq_index(lat: float, lon: float) -> Optional[dict]:
    """Return nearest GIOŚ station AQ index (Poland) as dict.

    Returns None if disabled or not available.
    """
    if os.getenv("GIOS_MODE", "on").lower() in {"0", "off", "false", "no"}:
        return None

    stations = _load_gios_stations()
    if not stations:
        return None

    best_station = None
    best_d = 1e9
    for st in stations:
        try:
            slat = float(st.get("gegrLat"))
            slon = float(st.get("gegrLon"))
        except Exception:
            continue
        d = _haversine_km(lat, lon, slat, slon)
        if d < best_d:
            best_d = d
            best_station = st

    if not best_station or best_d > float(os.getenv("GIOS_MAX_DISTANCE_KM", "60")):
        return None

    station_id = best_station.get("id")
    if station_id is None:
        return None

    payload = None
    for tmpl in (GIOS_INDEX_V1, GIOS_INDEX_OLD):
        try:
            payload = _http_get_json(tmpl.format(stationId=station_id))
            break
        except Exception:
            continue
    if not payload:
        return None

    level = payload.get("stIndexLevel") or {}
    level_name = (level.get("indexLevelName") or "").strip()

    # Map Polish index names to a numeric severity 0..5
    mapping = {
        "Bardzo dobry": 0,
        "Dobry": 1,
        "Umiarkowany": 2,
        "Dostateczny": 3,
        "Zły": 4,
        "Bardzo zły": 5,
        "Bardzo zly": 5,
        "Brak indeksu": None,
    }
    sev = mapping.get(level_name)

    return {
        "station_id": station_id,
        "station_name": best_station.get("stationName"),
        "distance_km": round(best_d, 1),
        "index_level_name": level_name or None,
        "severity": sev,
        "raw": payload,
    }


def fetch_imgw_warnings_for_voivodeship(voiv_code: Optional[str]) -> Optional[dict]:
    """Fetch IMGW meteo warnings for given voivodeship code (TERYT first 2 digits).

    We approximate location-match by voivodeship (województwo).
    """
    if os.getenv("IMGW_WARNINGS_MODE", "on").lower() in {"0", "off", "false", "no"}:
        return None
    if not voiv_code:
        return None

    try:
        payload = _http_get_json(IMGW_WARNINGS_METEO, timeout=25.0)
    except Exception:
        return None

    max_level = 0
    events: List[str] = []

    for w in (payload or []):
        try:
            level = int(w.get("stopien") or 0)
        except Exception:
            level = 0
        teryt = (w.get("teryt") or "")
        # teryt can be string of codes separated by comma or a single code
        teryt_list = [x.strip() for x in str(teryt).split(",") if x.strip()]
        if not any(x.startswith(voiv_code) for x in teryt_list):
            continue
        if level > max_level:
            max_level = level
        # event / phenomenon name varies by field ("zjawisko" is common)
        z = (w.get("zjawisko") or w.get("typ") or w.get("nazwa") or "").strip()
        if z and z not in events:
            events.append(z)

    return {"max_level": max_level, "events": events} if (max_level > 0 or events) else {"max_level": 0, "events": []}


def fetch_space_weather_kp() -> Optional[float]:
    if os.getenv("SPACE_WEATHER_MODE", "on").lower() in {"0", "off", "false", "no"}:
        return None
    try:
        payload = _http_get_json(NOAA_KP_1M, timeout=20.0)
        if not payload:
            return None
        last = payload[-1]
        kp = last.get("kp_index")
        return float(kp) if kp is not None else None
    except Exception:
        return None


def fetch_google_pollen_index(lat: float, lon: float) -> Optional[dict]:
    """Fetch daily pollen indices using Google Pollen API.

    Requires GOOGLE_POLLEN_API_KEY in environment.
    """
    mode = os.getenv("GOOGLE_POLLEN_MODE", "auto").lower()
    api_key = os.getenv("GOOGLE_POLLEN_API_KEY", "").strip()

    if mode in {"0", "off", "false", "no"}:
        return None
    if mode == "auto" and not api_key:
        return None
    if not api_key:
        return None

    params = {
        "key": api_key,
        "location.latitude": f"{lat}",
        "location.longitude": f"{lon}",
        "days": "1",
        "languageCode": os.getenv("GOOGLE_POLLEN_LANG", "pl"),
    }

    try:
        payload = _http_get_json(GOOGLE_POLLEN_FORECAST, params=params, timeout=20.0)
    except Exception:
        return None

    daily = (payload or {}).get("dailyInfo") or []
    if not daily:
        return None

    pollen_types = daily[0].get("pollenTypeInfo") or []
    values: List[Tuple[str, int, str]] = []  # (type, value, category)

    for pt in pollen_types:
        ptype = (pt.get("code") or pt.get("pollenType") or "").strip()
        idx = pt.get("indexInfo") or {}
        try:
            val = int(idx.get("value"))
        except Exception:
            val = -1
        cat = (idx.get("category") or "").strip()
        if val >= 0:
            values.append((ptype or "NIEZNANE", val, cat))

    if not values:
        return None

    max_item = max(values, key=lambda x: x[1])
    return {
        "max_value": max_item[1],
        "max_type": max_item[0],
        "max_category": max_item[2] or None,
        "breakdown": [{"type": t, "value": v, "category": c} for (t, v, c) in values],
    }


def extract_features(weather: Dict[str, Any], air_quality: Dict[str, Any], loc: Optional[Location] = None) -> Dict[str, Any]:
    """Extract features for next 6 hours + some 'now' values.

    Keeps backwards-compatible keys already used in logs.
    Adds extra keys from GIOŚ / IMGW / NOAA / Google Pollen when available.
    """
    feats: Dict[str, Any] = {}

    hourly = (weather or {}).get("hourly") or {}
    times = hourly.get("time") or []
    if not times:
        return feats

    i0 = _find_idx0(times)
    n6 = 7  # now + next 6 hours (7 values)

    # weather arrays
    temp = hourly.get("temperature_2m") or []
    app = hourly.get("apparent_temperature") or []
    rh = hourly.get("relative_humidity_2m") or []
    pres = hourly.get("surface_pressure") or []
    gust = hourly.get("wind_gusts_10m") or []
    pprob = hourly.get("precipitation_probability") or []
    uv = hourly.get("uv_index") or []
    sw = hourly.get("shortwave_radiation") or []
    cloud = hourly.get("cloud_cover") or []
    cape = hourly.get("cape") or []
    wcode = hourly.get("weather_code") or []

    temp6 = _slice(temp, i0, n6)
    app6 = _slice(app, i0, n6)
    pres6 = _slice(pres, i0, n6)
    rh6 = _slice(rh, i0, n6)
    gust6 = _slice(gust, i0, n6)
    pprob6 = _slice(pprob, i0, n6)
    uv6 = _slice(uv, i0, n6)
    sw6 = _slice(sw, i0, n6)
    cloud6 = _slice(cloud, i0, n6)
    cape6 = _slice(cape, i0, n6)

    def _delta(arr: List[float]) -> float:
        return (arr[-1] - arr[0]) if len(arr) >= 2 else 0.0

    feats["pressure_delta_3h"] = (pres6[3] - pres6[0]) if len(pres6) >= 4 else None
    feats["pressure_delta_6h"] = _delta(pres6) if pres6 else None
    feats["temp_delta_6h"] = _delta(temp6) if temp6 else None
    feats["apparent_delta_6h"] = _delta(app6) if app6 else None
    feats["humidity_now"] = int(round(rh6[0])) if rh6 else None
    feats["gust_max_6h"] = max(gust6) if gust6 else None
    feats["precip_prob_max_6h"] = max(pprob6) if pprob6 else None
    feats["uv_max_6h"] = max(uv6) if uv6 else None
    feats["sw_rad_max_6h"] = max(sw6) if sw6 else None
    feats["cloud_min_6h"] = min(cloud6) if cloud6 else None
    feats["cape_max_6h"] = max(cape6) if cape6 else None

    # thunder: Open-Meteo weather codes 95/96/99 indicate thunderstorm
    thunder = False
    try:
        for x in wcode[i0:i0+n6]:
            if x is None:
                continue
            if int(x) in {95, 96, 99}:
                thunder = True
                break
    except Exception:
        pass
    feats["thunder_next_6h"] = thunder

    # air quality
    aqh = (air_quality or {}).get("hourly") or {}
    aqt = aqh.get("time") or []
    if aqt:
        j0 = _find_idx0(aqt)
        pm25 = _slice(aqh.get("pm2_5") or [], j0, n6)
        aqi = _slice(aqh.get("us_aqi") or [], j0, n6)
        feats["pm2_5_max_6h"] = max(pm25) if pm25 else None
        feats["aqi_us_max_6h"] = max(aqi) if aqi else None

        # pollen from Open-Meteo (counts)
        pollen_arrays = [
            aqh.get("alder_pollen") or [],
            aqh.get("birch_pollen") or [],
            aqh.get("grass_pollen") or [],
            aqh.get("mugwort_pollen") or [],
            aqh.get("olive_pollen") or [],
            aqh.get("ragweed_pollen") or [],
        ]
        pollen_vals: List[float] = []
        for arr in pollen_arrays:
            pollen_vals.extend(_slice(arr, j0, n6))
        feats["pollen_max_6h"] = max(pollen_vals) if pollen_vals else 0.0
    else:
        feats["pm2_5_max_6h"] = None
        feats["aqi_us_max_6h"] = None
        feats["pollen_max_6h"] = 0.0

    # --- Extra sources ---
    # If caller didn't pass Location, we can still use coordinates from Open-Meteo response.
    if loc is None:
        try:
            lat = float((weather or {}).get("latitude"))
            lon = float((weather or {}).get("longitude"))
            loc = Location(name="", latitude=lat, longitude=lon)
        except Exception:
            loc = None

    if loc is not None:
        if not loc.admin1:
            loc.admin1 = _reverse_admin1(loc.latitude, loc.longitude)
        # GIOŚ air-quality index (PL)
        gios = fetch_gios_aq_index(loc.latitude, loc.longitude)
        if gios:
            feats["gios_index_name"] = gios.get("index_level_name")
            feats["gios_index_severity"] = gios.get("severity")
            feats["gios_station_name"] = gios.get("station_name")
            feats["gios_station_distance_km"] = gios.get("distance_km")

        # IMGW warnings (approx by voivodeship)
        voiv_code = VOIVODESHIP_CODE_BY_NAME.get(loc.admin1 or "") if loc.admin1 else None
        imgw = fetch_imgw_warnings_for_voivodeship(voiv_code)
        if imgw:
            feats["imgw_warning_level"] = imgw.get("max_level")
            feats["imgw_warning_events"] = imgw.get("events")

        # NOAA geomagnetic activity (Kp)
        kp = fetch_space_weather_kp()
        if kp is not None:
            feats["kp_index"] = kp

        # Google Pollen API (daily index 0..5)
        gp = fetch_google_pollen_index(loc.latitude, loc.longitude)
        if gp:
            feats["google_pollen_max"] = gp.get("max_value")
            feats["google_pollen_type"] = gp.get("max_type")
            feats["google_pollen_category"] = gp.get("max_category")
            feats["google_pollen_breakdown"] = gp.get("breakdown")

    return feats
