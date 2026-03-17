from __future__ import annotations

import logging
import os
from typing import Any, Dict, List, Optional

from anthropic import Anthropic

log = logging.getLogger("weatherguard")


def _alert_title(profile: str) -> str:
    p = (profile or "").strip().lower()
    if p in {"migraine", "migrena"}:
        return "Alert migrenowy"
    if p in {"heart", "serce", "cardio"}:
        return "Alert dla chorób serca"
    if p in {"allergy", "alergia", "astma"}:
        return "Alert alergiczny"
    if p in {"both", "oba"}:
        return "Alert zdrowotny (migrena + serce)"
    return "Alert zdrowotny"


def _fallback_message(
    profile: str,
    location_name: str,
    score: int,
    label_pl: str,
    reasons: List[str],
) -> str:
    title = _alert_title(profile)
    lines = [
        f"{title}",
        f"Lokalizacja: {location_name}",
        f"Ryzyko objawów: {label_pl} ({score}/100)",
    ]
    if reasons:
        lines.append("Czynniki ryzyka (z danych środowiskowych):")
        for r in reasons[:6]:
            lines.append(f"- {r}")
    lines.append("Wskazówka: jeśli masz tendencję do objawów przy takich warunkach, rozważ dziś ostrożniejsze tempo i obserwuj sygnały organizmu.")
    lines.append('To nie jest porada medyczna.')
    return "\n".join(lines)


def generate_message(
    *,
    profile: str,
    location_name: str,
    score: int,
    reasons: Optional[List[str]] = None,
    feats: Optional[Dict[str, Any]] = None,
    risk_label: Optional[str] = None,
    risk_label_pl: Optional[str] = None,
    model: Optional[str] = None,
    api_key: Optional[str] = None,
    **_: Any,
) -> str:
    """Generate a Polish, symptom-focused alert message using Claude.

    If AI_MODE=off or no API key / error -> uses deterministic fallback message.
    """

    reasons = reasons or []
    label_pl = (risk_label_pl or risk_label or "").strip().lower() or "niskie"

    ai_mode = os.getenv("AI_MODE", "on").lower()
    if ai_mode in {"0", "off", "false", "no"}:
        log.info("AI_MODE=off → using fallback message.")
        return _fallback_message(profile, location_name, score, label_pl, reasons)

    api_key = api_key or os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        log.info("ANTHROPIC_API_KEY missing → using fallback message.")
        return _fallback_message(profile, location_name, score, label_pl, reasons)

    model = model or os.getenv("CLAUDE_MODEL_FAST", "claude-haiku-4-5-20251001")

    title = _alert_title(profile)

    # Create a compact feature summary to avoid hallucination.
    feats = feats or {}
    important = {
        k: feats.get(k)
        for k in [
            "pressure_delta_6h",
            "temp_delta_6h",
            "humidity_now",
            "gust_max_6h",
            "precip_prob_max_6h",
            "thunder_next_6h",
            "uv_max_6h",
            "pm2_5_max_6h",
            "aqi_us_max_6h",
            "gios_index_name",
            "gios_index_severity",
            "google_pollen_max",
            "google_pollen_type",
            "imgw_warning_level",
            "imgw_warning_events",
            "kp_index",
        ]
        if k in feats
    }

    system = (
        "Jesteś asystentem aplikacji zdrowotnej. "
        "Tworzysz krótkie, konkretne powiadomienia o ryzyku wystąpienia objawów na podstawie czynników środowiskowych. "
        "NIE pisz ogólnej prognozy pogody. "
        "Zawsze pisz po polsku. "
        "Nie stawiaj diagnozy. "
        "Na końcu dodaj dokładnie zdanie: 'To nie jest porada medyczna.'."
    )

    user = {
        "title": title,
        "location": location_name,
        "risk": {"score": score, "label": label_pl},
        "profile": profile,
        "reasons": reasons,
        "features": important,
        "instructions": [
            "Zacznij od nagłówka dokładnie takiego jak title.",
            "Druga linia: 'Ryzyko objawów: <label> (<score>/100)'.",
            "Następnie 2–4 zdania o możliwych objawach i dlaczego mogą wystąpić przy tych czynnikach (odwołuj się do reasons i features).",
            "Dodaj 2–4 praktyczne, bezpieczne wskazówki (np. nawodnienie, unikanie bodźców, planowanie wysiłku) — bez leków i bez instrukcji medycznych.",
            "Użyj tonu stopniowanego: dla niskiego ryzyka uspokajająco, dla wysokiego bardziej ostrożnie.",
        ],
    }

    try:
        client = Anthropic(api_key=api_key)
        msg = client.messages.create(
            model=model,
            max_tokens=300,
            system=system,
            messages=[{"role": "user", "content": str(user)}],
        )
        text = (msg.content[0].text or "").strip()
        if not text:
            return _fallback_message(profile, location_name, score, label_pl, reasons)
        # Safety: ensure header is present.
        if not text.lower().startswith(title.lower()):
            text = f"{title}\n" + text
        # Ensure Polish medical disclaimer.
        if "to nie jest porada medyczna" not in text.lower():
            text = text.rstrip() + "\nTo nie jest porada medyczna."
        return text
    except Exception as e:
        log.warning(f"AI failed → using fallback message: {e}")
        return _fallback_message(profile, location_name, score, label_pl, reasons)
