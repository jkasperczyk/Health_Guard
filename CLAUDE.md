# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Project Does

Health_Guard (also called WeatherGuard) monitors weather and environmental conditions to predict health risks and sends Polish-language alerts via WhatsApp/SMS. It supports three health profiles: **migraine**, **heart**, and **allergy**.

## Running the Application

```bash
# Single test run (no messages sent)
python -m app.runner --dry-run

# Single cycle (sends real alerts)
python -m app.runner --once

# Continuous mode with explicit env file
python -m app.runner --env /opt/weatherguard/config/.env

# WhatsApp feedback webhook (port 8020)
python -m app.webhook --host 127.0.0.1 --port 8020

# SMS feedback webhook
python -m weatherguard.sms_webhook_server
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

### Two Subsystems

**1. WhatsApp Alert System (`app/`)** — scheduled runner + webhook feedback
**2. SMS System (`weatherguard/`)** — Twilio SMS inbound webhook + user state

### Core Data Flow

```
Open-Meteo + GIOŚ + IMGW + NOAA + Google Pollen
        ↓
   weather.py  (extract_features → 40+ metrics)
        ↓
    risk.py   (score 0-100 per profile)
        ↓ if score ≥ threshold and cooldown elapsed
     ai.py    (GPT-4o-mini, Polish, max 220 tokens)
        ↓
  whatsapp.py (Twilio send)
        ↓
  feedback_store.py (SQLite: alerts + readings tables)
        ↑
  webhook.py  (Twilio inbound → feedback table)
```

### Key Modules

| File | Role |
|------|------|
| `app/runner.py` | Entry point: iterates users, orchestrates full pipeline |
| `app/config.py` | Loads `.env`, parses `users.txt`, exposes typed settings |
| `app/weather.py` | Fetches 7 external APIs, produces feature dict via `extract_features()` |
| `app/risk.py` | `migraine_risk()`, `heart_risk()`, `allergy_risk()` → `RiskResult(score, label, reasons)` |
| `app/ai.py` | OpenAI call with fallback deterministic message; respects `AI_MODE` env var |
| `app/whatsapp.py` | Thin Twilio wrapper |
| `app/webhook.py` | HTTP server for WhatsApp replies (TwiML response) |
| `app/feedback_store.py` | All SQLite operations; WAL mode; auto-links feedback to last alert within 24h |
| `app/state.py` | JSON-based cooldown tracking keyed by `phone::profile` |
| `weatherguard/sms_parser.py` | Parses inbound SMS commands (help/status/stop/start/ack/no/delay) |
| `weatherguard/sms_state.py` | JSON user DB at `data/sms_users.json` |

### Configuration

**`config/users.txt`** — one user per line:
```
+48XXXXXXXXX|migraine|Zawiercie,PL|50|22-7
```
Fields: `phone | profile | location | threshold | quiet_hours`
Valid profiles: `migraine`, `heart`, `allergy`, `both`

**`config/.env`** — secrets (never commit):
- `OPENAI_API_KEY`, `OPENAI_MODEL` (default: `gpt-4o-mini`)
- `TWILIO_ACCOUNT_SID`, `TWILIO_AUTH_TOKEN`, Twilio from-numbers
- `BASE_DIR` (default: `/opt/weatherguard`)
- `COOLDOWN_MINUTES` (default: `360`)
- `DEFAULT_THRESHOLD_MIGRAINE/HEART/ALLERGY` (defaults: 65/70/60)
- `AI_MODE`: `off` | `auto` | `on`

### Data Storage

| Store | Path | Contents |
|-------|------|----------|
| SQLite | `data/feedback.db` | `alerts`, `feedback`, `readings` tables |
| JSON | `state.json` | Alert cooldown timestamps + last scores |
| JSON | `data/sms_users.json` | SMS subscriber state |
| JSON | `data/gios_stations_cache.json` | GIOŚ stations (refreshed weekly) |

### External APIs

- **Open-Meteo** — forecast + air quality (PM2.5, AQI, pollen)
- **GIOŚ** — Polish national air quality index
- **IMGW** — Polish meteorological warnings
- **NOAA** — geomagnetic Kp index
- **Google Pollen API** — optional, high-quality pollen data
- **Twilio** — WhatsApp + SMS messaging
- **OpenAI** — GPT-4o-mini for Polish alert text generation

### Dependencies

```
openai>=1.30.0
twilio>=9.0.0
requests>=2.31.0
python-dotenv>=1.0.1
```

Install: `pip install -r requirements.txt` (use the venv at `venv/`)

## Production Deployment

- Base dir: `/opt/weatherguard`
- Venv: `/opt/weatherguard/venv`
- Systemd user: `weatherguard`

- The `systemd/` directory contains `.service` and `.timer` unit files to install

## Related Project: Zdrowa (/PROJECTS/Zdrowa)
User/admin panel application. See /PROJECTS/Zdrowa/CLAUDE.md for details.
These two projects may share a database and/or API — treat them as one system.
When making changes that affect the interface between them, check both codebases.
