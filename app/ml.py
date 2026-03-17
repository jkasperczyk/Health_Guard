"""app/ml.py — Personalised ML risk prediction.

Tables used in feedback.db:
  ml_models        — trained RandomForest per (phone, profile)
  ml_training_data — (optional cache, not used by default — data fetched live)

Usage:
  from app.ml import predict_ml, train_model, retrain_all_models, get_model_info
"""
from __future__ import annotations

import io
import json
import logging
import os
import sqlite3
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

log = logging.getLogger("weatherguard.ml")

# Ordered feature names — must stay stable across train/predict
FEATURE_NAMES: List[str] = [
    "pressure_delta_3h",
    "pressure_delta_24h",
    "temperature",
    "temp_delta",
    "humidity",
    "dew_point",
    "aqi",
    "pm25",
    "pollen_max",
    "wind_gusts",
    "kp_index",
    "moon_phase",
    "cycle_day",
    "stress_1_10",
    "exercise_1_10",
    "sleep_quality_1_10",
    "hydration_1_10",
    "headache_1_10",
    "hour_of_day",
    "day_of_week",
]

NUM_FEATURES = len(FEATURE_NAMES)
MIN_SAMPLES = 30  # minimum positive symptom samples required before training


# ── DB helpers ─────────────────────────────────────────────────────────────────

def _db_path() -> str:
    from app.feedback_store import _db_path as _fbp
    return _fbp()


def _conn() -> sqlite3.Connection:
    c = sqlite3.connect(_db_path())
    c.execute("PRAGMA journal_mode=WAL;")
    c.execute("PRAGMA synchronous=NORMAL;")
    return c


def ensure_ml_schema() -> None:
    """Create ML-specific tables if they don't exist."""
    c = _conn()
    try:
        c.execute("""
            CREATE TABLE IF NOT EXISTS ml_models (
                phone                    TEXT NOT NULL,
                profile                  TEXT NOT NULL,
                model_blob               BLOB NOT NULL,
                accuracy                 REAL,
                f1                       REAL,
                feature_importances_json TEXT,
                trained_at               TEXT NOT NULL,
                sample_count             INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (phone, profile)
            )
        """)
        c.commit()
    finally:
        c.close()


# ── Feature extraction ─────────────────────────────────────────────────────────

def _extract_features(feats: Dict[str, Any], ts: Optional[int] = None) -> List[float]:
    """Extract a fixed-length float vector from a feats dict.

    Unknown / missing values default to neutral values so the model
    degrades gracefully rather than erroring.
    """
    def _get(*keys, default: float = 0.0) -> float:
        for k in keys:
            v = feats.get(k)
            if v is not None:
                try:
                    return float(v)
                except (TypeError, ValueError):
                    continue
        return default

    # Hour / day-of-week: prefer explicit feats fields, fall back to ts
    hour: int = 12
    dow: int = 0
    if ts:
        try:
            dt = datetime.fromtimestamp(int(ts), tz=timezone.utc)
            hour = dt.hour
            dow = dt.weekday()
        except Exception:
            pass
    if "hour_of_day" in feats:
        try:
            hour = int(feats["hour_of_day"])
        except Exception:
            pass
    if "day_of_week" in feats:
        try:
            dow = int(feats["day_of_week"])
        except Exception:
            pass

    return [
        _get("pressure_delta_3h", default=0.0),
        _get("pressure_delta_24h", "pressure_delta_6h", default=0.0),
        _get("temp_now", "temperature_2m", "temperature", default=15.0),
        _get("temp_delta_6h", "temp_delta_3h", "temp_delta", default=0.0),
        _get("humidity_now", "relative_humidity_2m", "humidity", default=60.0),
        _get("dew_point_now", "dew_point_2m", "dew_point", default=10.0),
        _get("aqi_us_max_6h", "aqi_us_now", "aqi", default=50.0),
        _get("pm2_5_max_6h", "pm2_5_now", "pm25", default=10.0),
        _get("google_pollen_max", "pollen_max_6h", "pollen_max", default=0.0),
        _get("gust_max_6h", "wind_gusts_max", "wind_gusts", default=0.0),
        _get("kp_index", default=1.0),
        _get("moon_phase", default=0.25),
        _get("cycle_day", default=-1.0),         # -1 = not applicable / unknown
        _get("stress_1_10", default=0.0),
        _get("exercise_1_10", default=0.0),
        _get("sleep_quality_1_10", default=0.0),
        _get("hydration_1_10", default=0.0),
        _get("headache_1_10", default=0.0),
        float(hour),
        float(dow),
    ]


# ── Training data ──────────────────────────────────────────────────────────────

def prepare_training_data(
    phone: str,
    profile: str,
) -> Tuple[Optional[Any], Optional[Any]]:
    """Build (X, y) numpy arrays for training.

    Positive samples (label=1): symptom_log rows with feats_json.
    Negative samples (label=0): readings from days with NO symptom report,
        one per day (highest base_score day).

    Returns (None, None) if fewer than MIN_SAMPLES positive examples exist.
    """
    try:
        import numpy as np
    except ImportError:
        log.error("[ML] numpy not installed — cannot prepare training data")
        return None, None

    c = _conn()
    try:
        # ── Positive samples ────────────────────────────────────────────────
        sym_rows = c.execute(
            "SELECT timestamp, feats_json FROM symptom_log "
            "WHERE phone=? AND profile=? AND feats_json IS NOT NULL "
            "ORDER BY timestamp",
            (phone, profile),
        ).fetchall()

        if len(sym_rows) < MIN_SAMPLES:
            log.info(
                f"[ML] {phone}/{profile}: {len(sym_rows)} symptom samples "
                f"(need {MIN_SAMPLES})"
            )
            return None, None

        symptom_days: set = set()
        X_pos: List[List[float]] = []
        y_pos: List[int] = []

        for ts_str, feats_json in sym_rows:
            try:
                feats = json.loads(feats_json)
            except Exception:
                feats = {}
            try:
                dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                ts_int = int(dt.timestamp())
                day_str = dt.strftime("%Y-%m-%d")
            except Exception:
                ts_int = 0
                day_str = (ts_str or "")[:10]
            symptom_days.add(day_str)
            X_pos.append(_extract_features(feats, ts_int))
            y_pos.append(1)

        # ── Negative samples ────────────────────────────────────────────────
        cols_info = {r[1] for r in c.execute("PRAGMA table_info(readings)").fetchall()}
        if "feats_json" not in cols_info:
            log.info(f"[ML] {phone}/{profile}: readings.feats_json column missing")
            return None, None

        base_col = "base_score" if "base_score" in cols_info else "score"
        read_rows = c.execute(
            f"SELECT ts, feats_json, {base_col} FROM readings "
            "WHERE phone=? AND profile=? AND feats_json IS NOT NULL "
            "ORDER BY ts",
            (phone, profile),
        ).fetchall()

        # Group by day, keep highest-score reading, exclude symptom days
        day_best: Dict[str, Tuple[int, str, int]] = {}
        for ts_int, feats_json, score in read_rows:
            try:
                day_str = datetime.fromtimestamp(
                    int(ts_int), tz=timezone.utc
                ).strftime("%Y-%m-%d")
            except Exception:
                continue
            if day_str in symptom_days:
                continue
            score = int(score or 0)
            if day_str not in day_best or score > day_best[day_str][2]:
                day_best[day_str] = (int(ts_int), feats_json, score)

        X_neg: List[List[float]] = []
        y_neg: List[int] = []
        for ts_int, feats_json, _ in day_best.values():
            try:
                feats = json.loads(feats_json)
            except Exception:
                feats = {}
            X_neg.append(_extract_features(feats, ts_int))
            y_neg.append(0)

        if not X_neg:
            log.info(f"[ML] {phone}/{profile}: no negative samples available")
            return None, None

        X = np.array(X_pos + X_neg, dtype=np.float32)
        y = np.array(y_pos + y_neg, dtype=np.int32)
        log.info(
            f"[ML] {phone}/{profile}: {len(y_pos)} positives, "
            f"{len(y_neg)} negatives, {len(y)} total"
        )
        return X, y

    except Exception as e:
        log.error(f"[ML] prepare_training_data error for {phone}/{profile}: {e}")
        return None, None
    finally:
        c.close()


# ── Model training ─────────────────────────────────────────────────────────────

def train_model(phone: str, profile: str) -> Optional[Dict[str, Any]]:
    """Train RandomForestClassifier for (phone, profile) and persist to DB.

    Returns metrics dict on success, None on failure or insufficient data.
    """
    try:
        import numpy as np
        import joblib
        from sklearn.ensemble import RandomForestClassifier
        from sklearn.model_selection import train_test_split
        from sklearn.metrics import accuracy_score, f1_score
    except ImportError as e:
        log.error(f"[ML] Missing ML dependency: {e}")
        return None

    ensure_ml_schema()

    X, y = prepare_training_data(phone, profile)
    if X is None or y is None:
        return None

    n_samples = len(y)
    if n_samples < MIN_SAMPLES:
        return None

    try:
        # For very small datasets skip stratified split to avoid class issues
        if n_samples < 40 or len(set(y)) < 2:
            X_train, y_train = X, y
            X_test, y_test = X, y
        else:
            X_train, X_test, y_train, y_test = train_test_split(
                X, y, test_size=0.2, random_state=42, stratify=y
            )

        clf = RandomForestClassifier(
            n_estimators=100,
            max_depth=10,
            random_state=42,
            class_weight="balanced",
            n_jobs=1,
        )
        clf.fit(X_train, y_train)

        y_pred = clf.predict(X_test)
        acc = float(accuracy_score(y_test, y_pred))
        f1 = float(f1_score(y_test, y_pred, zero_division=0))

        importances = {
            name: float(imp)
            for name, imp in zip(FEATURE_NAMES, clf.feature_importances_)
        }

        # Serialise model to bytes
        buf = io.BytesIO()
        joblib.dump(clf, buf)
        model_bytes = buf.getvalue()

        trained_at = datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        c = _conn()
        try:
            c.execute(
                """
                INSERT OR REPLACE INTO ml_models
                  (phone, profile, model_blob, accuracy, f1,
                   feature_importances_json, trained_at, sample_count)
                VALUES (?,?,?,?,?,?,?,?)
                """,
                (
                    phone, profile, model_bytes, acc, f1,
                    json.dumps(importances, ensure_ascii=False),
                    trained_at, n_samples,
                ),
            )
            c.commit()
        finally:
            c.close()

        log.info(
            f"[ML] Trained {phone}/{profile}: "
            f"acc={acc:.2f} f1={f1:.2f} n={n_samples}"
        )
        return {
            "accuracy": acc,
            "f1": f1,
            "feature_importances": importances,
            "trained_at": trained_at,
            "sample_count": n_samples,
        }

    except Exception as e:
        log.error(f"[ML] train_model error for {phone}/{profile}: {e}")
        return None


# ── Prediction ─────────────────────────────────────────────────────────────────

def predict_ml(
    phone: str,
    profile: str,
    features_dict: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    """Load model from DB and return ML prediction.

    Returns:
        {probability: 0-100, top_factors: [...], accuracy: float,
         sample_count: int, trained_at: str}
        or None if no model or import error.
    """
    try:
        import numpy as np
        import joblib
    except ImportError:
        return None

    ensure_ml_schema()

    c = _conn()
    try:
        row = c.execute(
            "SELECT model_blob, feature_importances_json, accuracy, sample_count, trained_at "
            "FROM ml_models WHERE phone=? AND profile=?",
            (phone, profile),
        ).fetchone()
    finally:
        c.close()

    if not row:
        return None

    model_blob, importances_json, accuracy, sample_count, trained_at = row
    try:
        clf = joblib.load(io.BytesIO(model_blob))
    except Exception as e:
        log.warning(f"[ML] Cannot deserialise model for {phone}/{profile}: {e}")
        return None

    try:
        fv = np.array([_extract_features(features_dict)], dtype=np.float32)
        proba = clf.predict_proba(fv)[0]
        # class order: [0, 1] — proba[1] = P(symptom)
        if len(proba) < 2:
            return None
        probability = int(round(float(proba[1]) * 100))

        # Top-3 feature importances as human-readable strings
        top_factors: List[str] = []
        try:
            importances = json.loads(importances_json or "{}")
            sorted_imp = sorted(
                importances.items(), key=lambda x: x[1], reverse=True
            )[:3]
            top_factors = [
                f"{name} ({int(round(imp * 100))}%)"
                for name, imp in sorted_imp
                if imp > 0.01
            ]
        except Exception:
            pass

        return {
            "probability": probability,
            "top_factors": top_factors,
            "accuracy": accuracy,
            "sample_count": sample_count,
            "trained_at": trained_at,
        }

    except Exception as e:
        log.warning(f"[ML] predict_ml error for {phone}/{profile}: {e}")
        return None


# ── Batch retraining (cron) ────────────────────────────────────────────────────

def retrain_all_models() -> None:
    """Retrain models for all ML-enabled users. Intended for weekly cron."""
    ensure_ml_schema()

    c = _conn()
    try:
        cols = {r[1] for r in c.execute("PRAGMA table_info(wg_users)").fetchall()}
        if "use_ml" not in cols:
            log.info("[ML] use_ml column missing from wg_users — nothing to retrain")
            return
        rows = c.execute(
            "SELECT phone, profiles_json FROM wg_users WHERE enabled=1 AND use_ml=1"
        ).fetchall()
    finally:
        c.close()

    log.info(f"[ML] retrain_all_models: {len(rows)} ML-enabled user row(s)")
    for phone, profiles_json in rows:
        try:
            profiles = json.loads(profiles_json or '["migraine"]')
        except Exception:
            profiles = ["migraine"]
        for profile in (profiles or ["migraine"]):
            try:
                metrics = train_model(phone, profile)
                if metrics:
                    log.info(
                        f"[ML] Retrained {phone}/{profile}: "
                        f"acc={metrics['accuracy']:.2f} n={metrics['sample_count']}"
                    )
                else:
                    log.info(
                        f"[ML] Skipped {phone}/{profile} "
                        "(not enough data or training error)"
                    )
            except Exception as e:
                log.warning(f"[ML] Retrain failed for {phone}/{profile}: {e}")


def get_model_info(phone: str, profile: str) -> Optional[Dict[str, Any]]:
    """Return metadata about a stored model, or None if none exists."""
    ensure_ml_schema()
    c = _conn()
    try:
        row = c.execute(
            "SELECT accuracy, f1, trained_at, sample_count, feature_importances_json "
            "FROM ml_models WHERE phone=? AND profile=?",
            (phone, profile),
        ).fetchone()
    finally:
        c.close()

    if not row:
        return None

    accuracy, f1, trained_at, sample_count, importances_json = row
    importances: Dict[str, float] = {}
    try:
        if importances_json:
            importances = json.loads(importances_json)
    except Exception:
        pass

    top_factors = sorted(importances.items(), key=lambda x: x[1], reverse=True)[:3]
    return {
        "accuracy": accuracy,
        "f1": f1,
        "trained_at": trained_at,
        "sample_count": sample_count,
        "top_factors": [
            (name, int(round(v * 100))) for name, v in top_factors if v > 0.01
        ],
    }
