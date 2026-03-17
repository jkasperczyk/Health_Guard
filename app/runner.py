from __future__ import annotations

import argparse
import logging
import time
from datetime import datetime

from app.config import load_settings, parse_users, parse_quiet_hours, is_in_quiet_hours
from app.logging_setup import setup_logging
from app.weather import resolve_location, fetch_weather, fetch_air_quality, extract_features, extract_features_at_offset
from app.risk import migraine_risk, heart_risk, combined_risk, allergy_risk
from app.ai import generate_message
from app.state import StateDB
from app.feedback_store import (
    record_alert, record_reading,
    record_alert_queue, record_forecast_alert,
    get_wg_users, get_today_wellbeing,
)

log = logging.getLogger("weatherguard.runner")

# Predictive alert windows (hours ahead)
FORECAST_OFFSETS = [3, 6, 9, 12]


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
    """Signature-compatible call to app.ai.generate_message."""
    import inspect

    kwargs = {
        "api_key": settings.anthropic_api_key,
        "model": settings.claude_model_fast,
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
    ap.add_argument("--dry-run", action="store_true", help="Do not send messages / write alerts_queue")
    args = ap.parse_args()

    settings = load_settings(args.env)
    setup_logging(settings.log_dir)

    users = get_wg_users()
    if not users:
        log.info("Brak użytkowników w wg_users — fallback: users.txt")
        users = parse_users(settings.users_file)
    if not users:
        log.warning("Brak użytkowników. Sprawdź wg_users w feedback.db lub USERS_FILE.")
        return

    st = StateDB(settings.state_db)

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

            # Merge today's self-reported wellbeing (best-effort)
            wellbeing = {}
            try:
                wellbeing = get_today_wellbeing(u.phone)
                if wellbeing:
                    feats.update(wellbeing)
                    log.debug(f"[wellbeing] {u.phone} stress={wellbeing.get('stress_1_10')} exercise={wellbeing.get('exercise_1_10')}")
            except Exception:
                pass

            rr = pick_risk_fn(u.profile)(feats)

            # ML blending (opt-in per user)
            ml_score_val = None
            if getattr(u, "use_ml", False):
                try:
                    from app.ml import predict_ml as _predict_ml
                    _ml_res = _predict_ml(u.phone, u.profile, feats)
                    if _ml_res is not None:
                        ml_score_val = _ml_res["probability"]
                        classical_score = int(getattr(rr, "score", 0))
                        blended = max(0, min(100, int(round(0.6 * classical_score + 0.4 * ml_score_val))))
                        rr.score = blended
                        rr.reasons = list(getattr(rr, "reasons", [])) + [
                            f"🧠 ML predykcja: {ml_score_val}% (na podstawie Twoich indywidualnych danych)"
                        ]
                        log.debug(f"[ML] {u.phone} classical={classical_score} ml={ml_score_val} blended={blended}")
                except Exception as _ml_e:
                    log.debug(f"[ML] predict_ml failed for {u.phone}: {_ml_e}")

            # Record reading (always, best-effort)
            try:
                record_reading(
                    phone=u.phone,
                    profile=u.profile,
                    location=getattr(loc, "name", str(loc)),
                    score=int(getattr(rr, "score", 0)),
                    base_score=int(getattr(rr, "base_score", getattr(rr, "score", 0))),
                    threshold=int(th),
                    label=str(getattr(rr, "label", "")),
                    reasons=list(getattr(rr, "reasons", [])),
                    feats=feats,
                    ml_score=ml_score_val,
                )
            except Exception:
                pass

            # Predictive forecast: score future windows and record if above threshold
            try:
                for offset in FORECAST_OFFSETS:
                    f_feats = extract_features_at_offset(w, a, offset)
                    if wellbeing:
                        f_feats.update(wellbeing)
                    f_rr = pick_risk_fn(u.profile)(f_feats)
                    # Only record if forecast crosses threshold while current is below
                    if f_rr.score >= th:
                        forecast_msg = (
                            f"⚠ Prognoza: ryzyko może wzrosnąć do ~{f_rr.score} w ciągu {offset}h"
                        )
                        if f_rr.reasons:
                            forecast_msg += f" ({f_rr.reasons[0]})"
                        if not args.dry_run:
                            record_forecast_alert(
                                phone=u.phone,
                                profile=u.profile,
                                hour_offset=offset,
                                forecast_score=int(f_rr.score),
                                current_score=int(rr.score),
                                threshold=int(th),
                                message=forecast_msg,
                            )
                        if rr.score < th:
                            # Queue push for predictive alert (only when current < threshold)
                            if not args.dry_run:
                                record_alert_queue(
                                    phone=u.phone,
                                    profile=u.profile,
                                    score=int(f_rr.score),
                                    threshold=int(th),
                                    message=forecast_msg,
                                )
                            log.info(f"[FORECAST] {u.phone} ({u.profile}) +{offset}h score={f_rr.score} th={th}")
                        break  # queue only the first crossing window
            except Exception as e:
                log.debug(f"[FORECAST SKIP] {u.phone}: {e}")

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

            if args.dry_run:
                log.info(f"[DRY-RUN] to={u.phone} profile={u.profile} loc={loc.name} score={rr.score} th={th}")
                log.info(msg)
                continue

            # Queue push notification instead of SMS
            try:
                record_alert_queue(
                    phone=u.phone,
                    profile=u.profile,
                    score=int(rr.score),
                    threshold=int(th),
                    message=msg,
                )
            except Exception as e:
                log.warning(f"[WARN] record_alert_queue failed: {e}")

            st.mark_sent(u.phone, u.profile, rr.score)
            log.info(f"[QUEUED] to={u.phone} profile={u.profile} loc={loc.name} score={rr.score} th={th}")

            # Record alert history (for display in Zdrowa alerts page)
            try:
                record_alert(
                    phone=u.phone,
                    profile=u.profile,
                    location=getattr(loc, "name", str(loc)),
                    score=int(getattr(rr, "score", 0)),
                    threshold=int(th),
                    label=str(getattr(rr, "label", "")),
                    reasons=list(getattr(rr, "reasons", [])),
                    sid=None,
                )
            except Exception as e:
                log.warning(f"[WARN] record_alert failed: {e}")

        except Exception as e:
            log.exception(f"[ERROR] user={u.phone} profile={u.profile} location={u.location}: {e}")

    st.close()


if __name__ == "__main__":
    main()
