from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional


@dataclass
class RiskResult:
    profile: str
    score: int        # final_score = clamp(base_score × modifier, 0, 100)
    base_score: int   # environmental score only (0-100)
    label: str        # Polish label
    reasons: List[str]


def _clamp(x: float, lo: float = 0.0, hi: float = 100.0) -> int:
    return int(max(lo, min(hi, round(x))))


def _label_pl(score: int) -> str:
    if score >= 80:
        return "bardzo wysokie"
    if score >= 60:
        return "wysokie"
    if score >= 30:
        return "umiarkowane"
    return "niskie"


def _get(feats: Dict[str, Any], key: str) -> Optional[float]:
    v = feats.get(key)
    try:
        return float(v) if v is not None else None
    except Exception:
        return None


def _personal_modifier(feats: Dict[str, Any]) -> float:
    """Compute personal modifier in [0.85, 1.25] from self-reported wellbeing factors.

    Returns 1.0 (neutral) if no user data present for today.
    Each factor contributes a delta proportionally:
      - Bad values push modifier up (toward 1.25)
      - Good values push modifier down (toward 0.85)
    """
    delta = 0.0
    has_any = False

    # Stress (1-10): high stress is bad
    stress = _get(feats, "stress_1_10")
    if stress is not None:
        has_any = True
        if stress >= 9:
            delta += 0.08
        elif stress >= 8:
            delta += 0.06
        elif stress >= 7:
            delta += 0.04
        elif stress >= 6:
            delta += 0.02
        elif stress <= 2:
            delta -= 0.06
        elif stress <= 3:
            delta -= 0.04
        elif stress <= 4:
            delta -= 0.02

    # Sleep quality (1-10): low = bad sleep
    sleep = _get(feats, "sleep_quality_1_10")
    if sleep is not None:
        has_any = True
        if sleep <= 2:
            delta += 0.08
        elif sleep <= 3:
            delta += 0.06
        elif sleep <= 4:
            delta += 0.03
        elif sleep >= 9:
            delta -= 0.06
        elif sleep >= 8:
            delta -= 0.04
        elif sleep >= 7:
            delta -= 0.02

    # Hydration (1-10): low = bad
    hydration = _get(feats, "hydration_1_10")
    if hydration is not None:
        has_any = True
        if hydration <= 2:
            delta += 0.05
        elif hydration <= 3:
            delta += 0.03
        elif hydration >= 9:
            delta -= 0.04
        elif hydration >= 8:
            delta -= 0.02

    # Headache (0-10): 0=none, 10=severe; high = bad
    headache = _get(feats, "headache_1_10")
    if headache is not None:
        has_any = True
        if headache >= 9:
            delta += 0.08
        elif headache >= 7:
            delta += 0.06
        elif headache >= 5:
            delta += 0.03
        elif headache <= 1:
            delta -= 0.02

    # Exercise (1-10): low = bad for health
    exercise = _get(feats, "exercise_1_10")
    if exercise is not None:
        has_any = True
        if exercise <= 2:
            delta += 0.04
        elif exercise <= 3:
            delta += 0.02
        elif exercise >= 8:
            delta -= 0.04
        elif exercise >= 7:
            delta -= 0.02

    if not has_any:
        return 1.0

    return max(0.85, min(1.25, 1.0 + delta))


def _personal_reasons(feats: Dict[str, Any]) -> List[str]:
    """Generate human-readable reasons for personal modifier contribution."""
    reasons: List[str] = []

    stress = _get(feats, "stress_1_10")
    if stress is not None:
        if stress >= 8:
            reasons.append(f"Wysoki stres ({stress:.0f}/10) — wyzwalacz ryzyka")
        elif stress >= 6:
            reasons.append(f"Podwyższony stres ({stress:.0f}/10)")
        elif stress <= 3:
            reasons.append(f"Niski stres — czynnik ochronny ({stress:.0f}/10)")

    sleep = _get(feats, "sleep_quality_1_10")
    if sleep is not None:
        if sleep <= 3:
            reasons.append(f"Zły sen ({sleep:.0f}/10) — zwiększa ryzyko")
        elif sleep >= 8:
            reasons.append(f"Dobry sen — czynnik ochronny ({sleep:.0f}/10)")

    hydration = _get(feats, "hydration_1_10")
    if hydration is not None:
        if hydration <= 3:
            reasons.append(f"Niedostateczne nawodnienie ({hydration:.0f}/10)")

    headache = _get(feats, "headache_1_10")
    if headache is not None:
        if headache >= 7:
            reasons.append(f"Aktywny ból głowy ({headache:.0f}/10)")
        elif headache >= 5:
            reasons.append(f"Umiarkowany ból głowy ({headache:.0f}/10)")

    exercise = _get(feats, "exercise_1_10")
    if exercise is not None:
        if exercise <= 2:
            reasons.append(f"Niska aktywność fizyczna ({exercise:.0f}/10)")
        elif exercise >= 8:
            reasons.append(f"Wysoka aktywność — czynnik ochronny ({exercise:.0f}/10)")

    return reasons


def migraine_risk(feats: Dict[str, Any]) -> RiskResult:
    score = 0.0
    reasons: List[str] = []

    p6 = _get(feats, "pressure_delta_6h")
    if p6 is not None:
        ap6 = abs(p6)
        if ap6 >= 6:
            score += 30
            reasons.append(f"Szybka zmiana ciśnienia (~{ap6:.1f} hPa/6h)")
        elif ap6 >= 3:
            score += 15
            reasons.append(f"Umiarkowana zmiana ciśnienia (~{ap6:.1f} hPa/6h)")
        elif ap6 >= 1:
            score += 5
            reasons.append(f"Niewielka zmiana ciśnienia (~{ap6:.1f} hPa/6h)")

    t6 = _get(feats, "temp_delta_6h")
    if t6 is not None:
        at6 = abs(t6)
        if at6 >= 8:
            score += 20
            reasons.append(f"Duża zmiana temperatury (~{at6:.1f}°C/6h)")
        elif at6 >= 4:
            score += 10
            reasons.append(f"Zmienna temperatura (~{at6:.1f}°C/6h)")
        elif at6 >= 2:
            score += 5
            reasons.append(f"Lekka zmienność temperatury (~{at6:.1f}°C/6h)")

    h = _get(feats, "humidity_now")
    if h is not None:
        if h >= 85:
            score += 10
            reasons.append(f"Wysoka wilgotność ({h:.0f}%)")
        elif h >= 75:
            score += 6
            reasons.append(f"Podwyższona wilgotność ({h:.0f}%)")
        elif h >= 65:
            score += 3
            reasons.append(f"Umiarkowanie wysoka wilgotność ({h:.0f}%)")

    gust = _get(feats, "gust_max_6h")
    if gust is not None:
        if gust >= 60:
            score += 10
            reasons.append(f"Silne porywy wiatru (do {gust:.0f} km/h)")
        elif gust >= 40:
            score += 5
            reasons.append(f"Porywisty wiatr (do {gust:.0f} km/h)")

    pprob = _get(feats, "precip_prob_max_6h")
    if pprob is not None:
        if pprob >= 70:
            score += 8
            reasons.append(f"Duże prawdopodobieństwo opadów ({pprob:.0f}%)")
        elif pprob >= 40:
            score += 4
            reasons.append(f"Możliwe opady ({pprob:.0f}%)")

    if bool(feats.get("thunder_next_6h")):
        score += 15
        reasons.append("Możliwe burze w najbliższych godzinach")

    uv = _get(feats, "uv_max_6h")
    if uv is not None:
        if uv >= 7:
            score += 10
            reasons.append("Wysokie UV (bodziec świetlny)")
        elif uv >= 4:
            score += 5
            reasons.append("Umiarkowane UV (bodziec świetlny)")

    imgw = _get(feats, "imgw_warning_level")
    if imgw is not None and imgw > 0:
        if imgw >= 3:
            score += 15
        elif imgw == 2:
            score += 10
        else:
            score += 5
        events = feats.get("imgw_warning_events") or []
        if events:
            reasons.append(f"Ostrzeżenia IMGW: {', '.join(events[:3])}")
        else:
            reasons.append("Ostrzeżenia IMGW w regionie")

    kp = _get(feats, "kp_index")
    if kp is not None:
        if kp >= 6:
            score += 10
            reasons.append(f"Podwyższona aktywność geomagnetyczna (Kp≈{kp:.0f})")
        elif kp >= 5:
            score += 5
            reasons.append(f"Umiarkowana aktywność geomagnetyczna (Kp≈{kp:.0f})")

    base = _clamp(score)
    modifier = _personal_modifier(feats)
    final = _clamp(base * modifier)
    all_reasons = reasons + _personal_reasons(feats)
    return RiskResult("migraine", final, base, _label_pl(final), all_reasons)


def heart_risk(feats: Dict[str, Any]) -> RiskResult:
    score = 0.0
    reasons: List[str] = []

    tdelta = _get(feats, "temp_delta_6h")
    if tdelta is not None and abs(tdelta) >= 6:
        score += 8
        reasons.append(f"Duża zmiana temperatury (~{abs(tdelta):.1f}°C/6h)")

    p6 = _get(feats, "pressure_delta_6h")
    if p6 is not None and abs(p6) >= 5:
        score += 10
        reasons.append(f"Szybka zmiana ciśnienia (~{abs(p6):.1f} hPa/6h)")

    gust = _get(feats, "gust_max_6h")
    if gust is not None:
        if gust >= 70:
            score += 15
            reasons.append(f"Silny wiatr (porywy do {gust:.0f} km/h)")
        elif gust >= 50:
            score += 8
            reasons.append(f"Porywisty wiatr (do {gust:.0f} km/h)")

    aqi = _get(feats, "aqi_us_max_6h")
    if aqi is not None:
        if aqi >= 151:
            score += 20
            reasons.append(f"Zła jakość powietrza (AQI≈{aqi:.0f})")
        elif aqi >= 101:
            score += 12
            reasons.append(f"Podwyższone zanieczyszczenie (AQI≈{aqi:.0f})")
        elif aqi >= 51:
            score += 6
            reasons.append(f"Umiarkowane zanieczyszczenie (AQI≈{aqi:.0f})")

    gios_sev = feats.get("gios_index_severity")
    if gios_sev is not None:
        try:
            sev = int(gios_sev)
            add = max(0, min(15, sev * 3))
            if add:
                score += add
                name = feats.get("gios_index_name")
                reasons.append(f"GIOŚ: {name or 'indeks'} (stacja ~{feats.get('gios_station_distance_km','?')} km)")
        except Exception:
            pass

    imgw = _get(feats, "imgw_warning_level")
    if imgw is not None and imgw >= 2:
        score += 6
        reasons.append("Silniejsze ostrzeżenia IMGW w regionie")

    base = _clamp(score)
    modifier = _personal_modifier(feats)
    final = _clamp(base * modifier)
    all_reasons = reasons + _personal_reasons(feats)
    return RiskResult("heart", final, base, _label_pl(final), all_reasons)


def allergy_risk(feats: Dict[str, Any]) -> RiskResult:
    score = 0.0
    reasons: List[str] = []

    gp = feats.get("google_pollen_max")
    if gp is not None:
        try:
            gpv = int(gp)
        except Exception:
            gpv = None
        if gpv is not None:
            if gpv >= 5:
                score += 55
                reasons.append("Bardzo wysokie stężenie pyłków (Google Pollen)")
            elif gpv == 4:
                score += 45
                reasons.append("Wysokie stężenie pyłków (Google Pollen)")
            elif gpv == 3:
                score += 30
                reasons.append("Umiarkowane stężenie pyłków (Google Pollen)")
            elif gpv == 2:
                score += 18
                reasons.append("Niewielkie–umiarkowane pyłki (Google Pollen)")
            elif gpv == 1:
                score += 8
                reasons.append("Niewielkie pyłki (Google Pollen)")

            ptype = feats.get("google_pollen_type")
            if ptype:
                reasons.append(f"Dominujący typ pyłku: {ptype}")

    pm25 = _get(feats, "pm2_5_max_6h")
    if pm25 is not None:
        if pm25 >= 55:
            score += 30
            reasons.append(f"Wysokie PM2.5 (do {pm25:.1f} µg/m³)")
        elif pm25 >= 25:
            score += 20
            reasons.append(f"Podwyższone PM2.5 (do {pm25:.1f} µg/m³)")
        elif pm25 >= 12:
            score += 10
            reasons.append(f"Umiarkowane PM2.5 (do {pm25:.1f} µg/m³)")

    aqi = _get(feats, "aqi_us_max_6h")
    if aqi is not None:
        if aqi >= 151:
            score += 25
            reasons.append(f"Zła jakość powietrza (AQI≈{aqi:.0f})")
        elif aqi >= 101:
            score += 15
            reasons.append(f"Podwyższone zanieczyszczenie (AQI≈{aqi:.0f})")
        elif aqi >= 51:
            score += 5
            reasons.append(f"Umiarkowane zanieczyszczenie (AQI≈{aqi:.0f})")

    gios_sev = feats.get("gios_index_severity")
    if gios_sev is not None:
        try:
            sev = int(gios_sev)
            add = max(0, min(20, sev * 4))
            if add:
                score += add
                name = feats.get("gios_index_name")
                reasons.append(f"GIOŚ: {name or 'indeks'} (stacja ~{feats.get('gios_station_distance_km','?')} km)")
        except Exception:
            pass

    gust = _get(feats, "gust_max_6h")
    if gust is not None and gust >= 50:
        score += 5
        reasons.append("Wiatr może nasilać ekspozycję na pyłki/pyły")

    base = _clamp(score)
    modifier = _personal_modifier(feats)
    final = _clamp(base * modifier)
    all_reasons = reasons + _personal_reasons(feats)
    return RiskResult("allergy", final, base, _label_pl(final), all_reasons)


def combined_risk(profile: str, feats: Dict[str, Any]) -> RiskResult:
    p = (profile or "").strip().lower()

    if p in {"migraine", "migrena"}:
        return migraine_risk(feats)
    if p in {"heart", "serce", "cardio"}:
        return heart_risk(feats)
    if p in {"allergy", "alergia", "astma"}:
        return allergy_risk(feats)

    # both / oba
    if p in {"both", "oba", "migraine+heart", "heart+migraine"}:
        m = migraine_risk(feats)
        h = heart_risk(feats)
        if h.score > m.score:
            primary, secondary = h, m
        else:
            primary, secondary = m, h
        reasons = primary.reasons[:]
        if secondary.score >= 30:
            reasons.append(f"Dodatkowo: ryzyko {('migrenowe' if secondary.profile=='migraine' else 'krążeniowe')} {secondary.label} ({secondary.score}/100)")
        return RiskResult("both", primary.score, primary.base_score, primary.label, reasons)

    # Unknown profile -> treat as migraine (default)
    return migraine_risk(feats)
