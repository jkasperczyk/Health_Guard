from __future__ import annotations

import argparse
import logging
import time
from datetime import datetime

from app.config import load_settings, parse_users, parse_quiet_hours, is_in_quiet_hours
from app.logging_setup import setup_logging
from app.weather import resolve_location, fetch_weather, fetch_air_quality, extract_features
from app.risk import migraine_risk, heart_risk, combined_risk, allergy_risk
from app.ai import generate_message
from app.state import StateDB
from app.whatsapp import WhatsAppSender
from app.feedback_store import record_alert, record_reading

log = logging.getLogger("weatherguard.runner")


def pick_threshold(settings, user) -> int:
    if user.threshold is not None:
        return int(user.threshold)
    if user.profile == "migraine":
        return settings.default_threshold_migraine
    if user.profile == "heart":
        return settings.default_threshold_heart
    if user.profile == "allergy":
        return settings.default_threshold_allergy
    return max(settings.default_threshold_migraine, settings.default_threshold_heart)


def pick_risk_fn(profile: str):
    if profile == "migraine":
        return migraine_risk
    if profile == "heart":
        return heart_risk
    if profile == "allergy":
        return allergy_risk
    return combined_risk


def enforce_header(msg: str, profile: str) -> str:
    """Ensure exactly one header (robust vs 'Alert migrenowy:' variants)."""
    header_map = {
        "migraine": "Alert migrenowy",
        "allergy": "Alert alergiczny",
        "heart": "Alert sercowy",
    }
    h = header_map.get(profile)
    if not h:
        return msg or ""

    def is_header_line(line: str) -> bool:
        s = (line or "").strip()
        if not s:
            return False
        s2 = s.rstrip(" :—-–\t").strip()
        if s2.lower() == h.lower():
            return True
        if s.lower().startswith(h.lower()):
            tail = s[len(h):].strip()
            if tail in ("", ":", "—", "-", "–"):
                return True
        return False

    raw_lines = [ln.rstrip("\n") for ln in (msg or "").splitlines()]
    filtered = [ln for ln in raw_lines if not is_header_line(ln)]
    body = "\n".join([ln for ln in filtered if ln.strip()]).strip()
    return f"{h}\n{body}".strip() if body else h


def _call_generate_message(settings, u, loc_name: str, rr, feats):
    """Signature-compatible call to app.ai.generate_message (ai.py may evolve)."""
    import inspect

    kwargs = {
        "openai_api_key": settings.openai_api_key,
        "model": settings.openai_model,
        "ai_mode": settings.ai_mode,
        "score": getattr(rr, "score", None),
        "risk_score": getattr(rr, "score", None),
        "profile": u.profile,
        "location_name": loc_name,
        "risk_label": getattr(rr, "label", ""),
        "label": getattr(rr, "label", ""),
        "reasons": getattr(rr, "reasons", []),
        "feats": feats,
        "features": feats,
    }

    sig = inspect.signature(generate_message)
    allowed = set(sig.parameters.keys())
    has_var_kw = any(p.kind == inspect.Parameter.VAR_KEYWORD for p in sig.parameters.values())

    if has_var_kw:
        return generate_message(**kwargs)

    filtered = {k: v for k, v in kwargs.items() if k in allowed}
    if "score" in allowed and "score" not in filtered:
        filtered["score"] = getattr(rr, "score", None)
    return generate_message(**filtered)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--env", default=None, help="Path to .env file")
    ap.add_argument("--once", action="store_true", help="Run one cycle and exit")
    ap.add_argument("--dry-run", action="store_true", help="Do not send messages")
    args = ap.parse_args()

    settings = load_settings(args.env)
    setup_logging(settings.log_dir)

    users = parse_users(settings.users_file)
    if not users:
        log.warning("Brak użytkowników. Sprawdź USERS_FILE.")
        return

    st = StateDB(settings.state_db)

    sender = None
    if not args.dry_run:
        sender = WhatsAppSender(
            settings.twilio_account_sid,
            settings.twilio_auth_token,
            settings.twilio_whatsapp_from,
        )

    now_ts = int(time.time())
    now_hour = datetime.now().hour

    for u in users:
        try:
            th = pick_threshold(settings, u)

            qh = parse_quiet_hours(u.quiet_hours)
            if qh and is_in_quiet_hours(now_hour, qh):
                log.info(f"[SKIP cisza nocna] {u.phone} ({u.profile}) {u.location}")
                continue

            loc = resolve_location(u.location)
            w = fetch_weather(loc.latitude, loc.longitude, settings.forecast_hours)

            # fetch_air_quality compatibility (2 args vs 3 args)
            try:
                import inspect
                _sig = inspect.signature(fetch_air_quality)
                _params = list(_sig.parameters.values())
                _has_var = any(p.kind == inspect.Parameter.VAR_POSITIONAL for p in _params) or any(
                    p.kind == inspect.Parameter.VAR_KEYWORD for p in _params
                )
                _pos = [
                    p
                    for p in _params
                    if p.kind in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD)
                ]
                if _has_var or len(_pos) >= 3:
                    a = fetch_air_quality(loc.latitude, loc.longitude, settings.forecast_hours)
                else:
                    a = fetch_air_quality(loc.latitude, loc.longitude)
            except Exception:
                try:
                    a = fetch_air_quality(loc.latitude, loc.longitude, settings.forecast_hours)
                except TypeError:
                    a = fetch_air_quality(loc.latitude, loc.longitude)

            feats = extract_features(w, a)
            rr = pick_risk_fn(u.profile)(feats)

            # trend reading (always, best-effort)
            try:
                record_reading(
                    phone=u.phone,
                    profile=u.profile,
                    location=getattr(loc, "name", str(loc)),
                    score=int(getattr(rr, "score", 0)),
                    threshold=int(th),
                    label=str(getattr(rr, "label", "")),
                    reasons=list(getattr(rr, "reasons", [])),
                    feats=feats,
                )
            except Exception:
                pass

            if rr.score < th:
                log.info(f"[OK] {u.phone} ({u.profile}) {loc.name} score={rr.score} th={th}")
                continue

            last = st.last_sent(u.phone, u.profile)
            if last is not None and settings.cooldown_minutes > 0:
                elapsed = now_ts - int(last)
                cd = settings.cooldown_minutes * 60
                if elapsed < cd:
                    remaining = int((cd - elapsed) / 60)
                    log.info(
                        f"[SKIP cooldown] {u.phone} ({u.profile}) {loc.name} score={rr.score} th={th} pozostało≈{remaining} min"
                    )
                    continue

            msg = _call_generate_message(settings, u, loc.name, rr, feats)
            msg = enforce_header(msg, u.profile)

            if u.profile == "migraine":
                q = (
                    "Czy obserwujesz u siebie objawy lub sygnały migreny?\n"
                    "Odpowiedz: TAK OBJAWY / TAK SYGNAŁY / NIE"
                )
                if "Czy obserwujesz u siebie objawy" not in (msg or ""):
                    msg = (msg or "").rstrip() + "\n\n" + q

            if args.dry_run:
                log.info(f"[DRY-RUN SEND] to={u.phone} profile={u.profile} loc={loc.name} score={rr.score} th={th}")
                log.info(msg)
                continue

            sid = sender.send(u.phone, msg)
            st.mark_sent(u.phone, u.profile, rr.score)
            log.info(f"[SENT] to={u.phone} sid={sid} loc={loc.name} score={rr.score} th={th}")

            # IMPORTANT FIX: store alert with correct kwargs (previously this silently failed)
            try:
                record_alert(
                    phone=u.phone,
                    profile=u.profile,
                    location=getattr(loc, "name", str(loc)),
                    score=int(getattr(rr, "score", 0)),
                    threshold=int(th),
                    label=str(getattr(rr, "label", "")),
                    reasons=list(getattr(rr, "reasons", [])),
                    sid=str(sid) if sid is not None else None,
                )
            except Exception as e:
                log.warning(f"[WARN] record_alert failed: {e}")

        except Exception as e:
            log.exception(f"[ERROR] user={u.phone} profile={u.profile} location={u.location}: {e}")

    st.close()


if __name__ == "__main__":
    main()
