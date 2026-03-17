# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Project Does

Health_Guard (also called WeatherGuard) monitors weather and environmental conditions to predict health risks. It supports three health profiles: **migraine**, **heart**, and **allergy**. Alerts are delivered via browser **Web Push notifications** (PWA) through Zdrowa — no SMS/Twilio.

## Running the Application

```bash
# Single test run (no alerts queued)
python -m app.runner --dry-run

# Single cycle (queues real alerts to alerts_queue in feedback.db)
python -m app.runner --once

# Continuous mode with explicit env file
python -m app.runner --env /opt/weatherguard/config/.env
```

## Admin & Reports

```bash
./bin/wg-admin health
./bin/wg-admin alerts --phone +48XXXXXXXXX --profile migraine
./bin/wg-admin trend --phone +48XXXXXXXXX --profile migraine --days 7

./bin/wg-report daily
./bin/wg-report monthly --month 2026-02
```

## Systemd (Production)

```bash
systemctl start weatherguard.service     # oneshot runner
systemctl list-timers weatherguard.timer # fires hourly
```

Logs: `/opt/weatherguard/logs/weatherguard.log` (rotating, 2MB × 5)

## Architecture

### Core Data Flow

```
Open-Meteo + GIOŚ + IMGW + NOAA + Google Pollen
        ↓
   weather.py  (extract_features → 40+ metrics; extract_features_at_offset for predictive)
        ↓
    risk.py   (score 0-100 per profile; two-layer: base_score × personal_modifier)
        ↓ if score ≥ threshold and cooldown elapsed
     ai.py    (Claude Haiku, Polish, max 300 tokens)
        ↓
  runner.py   (writes to alerts_queue in feedback.db)
        ↓
  Zdrowa      (reads alerts_queue, sends Web Push notifications)
        ↓
  feedback_store.py (SQLite: alerts, readings, alerts_queue, forecast_alerts tables)
```

### Key Modules

| File | Role |
|------|------|
| `app/runner.py` | Entry point: iterates users, orchestrates full pipeline |
| `app/config.py` | Loads `.env`, parses `users.txt`, exposes typed settings |
| `app/weather.py` | Fetches 7 external APIs; `extract_features()` + `extract_features_at_offset()` |
| `app/risk.py` | `migraine_risk()`, `heart_risk()`, `allergy_risk()` → `RiskResult(score, base_score, label, reasons)` |
| `app/ai.py` | Claude Haiku call with fallback deterministic message; respects `AI_MODE` env var |
| `app/ml.py` | ML personalized risk prediction: train/predict RandomForestClassifier per user+profile |
| `app/feedback_store.py` | All SQLite operations; WAL mode; alerts_queue, push_subscriptions, forecast_alerts |
| `app/state.py` | JSON-based cooldown tracking keyed by `phone::profile` |

### Configuration

**`config/users.txt`** — one user per line:
```
+48XXXXXXXXX|migraine|Zawiercie,PL|50|22-7
```
Fields: `phone | profile | location | threshold | quiet_hours`
Valid profiles: `migraine`, `heart`, `allergy`, `both`

**`config/.env`** — secrets (never commit):
- `ANTHROPIC_API_KEY`, `CLAUDE_MODEL_FAST` (default: `claude-haiku-4-5-20251001`), `CLAUDE_MODEL_SMART` (default: `claude-sonnet-4-6`)
- `BASE_DIR` (default: `/opt/weatherguard`)
- `COOLDOWN_MINUTES` (default: `360`)
- `DEFAULT_THRESHOLD_MIGRAINE/HEART/ALLERGY` (defaults: 65/70/60)
- `AI_MODE`: `off` | `auto` | `on`

### Data Storage

| Store | Path | Contents |
|-------|------|----------|
| SQLite | `data/feedback.db` | `alerts`, `readings`, `alerts_queue`, `forecast_alerts`, `push_subscriptions` tables |
| JSON | `state.json` | Alert cooldown timestamps + last scores |
| JSON | `data/gios_stations_cache.json` | GIOŚ stations (refreshed weekly) |

### External APIs

- **Open-Meteo** — forecast + air quality (PM2.5, AQI, pollen)
- **GIOŚ** — Polish national air quality index
- **IMGW** — Polish meteorological warnings
- **NOAA** — geomagnetic Kp index
- **Google Pollen API** — optional, high-quality pollen data
- **Anthropic** — Claude Haiku for Polish alert text generation

### ML Personalized Risk Prediction

Users with `use_ml=1` in `wg_users` get personalized ML scoring:
- Model: `RandomForestClassifier(n_estimators=100, max_depth=10, class_weight="balanced")`
- 20 features from `feats_json` (weather, air quality, wellbeing, temporal)
- Training data: `symptom_log` (positives) + `readings` on days without symptoms (negatives)
- Minimum 30 positive samples to train
- Blending: `final_score = 0.6 * classical + 0.4 * ml_probability`
- Models stored in `ml_models` table as joblib BLOB
- Weekly retrain: Sunday 03:00 via cron

### Dependencies

```
anthropic>=0.40.0
requests>=2.31.0
python-dotenv>=1.0.1
scikit-learn>=1.3.0
numpy>=1.24.0
joblib>=1.3.0
```

Install: `pip install -r requirements.txt` (use the venv at `venv/`)

## Production Deployment

- Base dir: `/opt/weatherguard`
- Venv: `/opt/weatherguard/venv`
- Systemd user: `weatherguard`

- The `systemd/` directory contains `.service` and `.timer` unit files to install

## Related Project: Zdrowa (/PROJECTS/Zdrowa)
User/admin panel application. See /PROJECTS/Zdrowa/CLAUDE.md for details.
These two projects share `feedback.db`. Zdrowa reads `alerts_queue` and sends Web Push
notifications to users who have subscribed via the PWA.
When making changes that affect the interface between them, check both codebases.
