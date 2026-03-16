import argparse
import logging
import os
import unicodedata
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs

from app.feedback_store import ensure_schema, record_feedback

log = logging.getLogger('weatherguard.webhook')
VERSION = 'WeatherGuardWebhook/1.3'

def load_env(path: str) -> None:
    if not path or not os.path.exists(path):
        return
    with open(path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#') or '=' not in line:
                continue
            k, v = line.split('=', 1)
            os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))

def normalize(text: str) -> str:
    t = (text or '').strip().upper()
    t = unicodedata.normalize('NFKD', t)
    t = ''.join(ch for ch in t if not unicodedata.combining(ch))
    return t

def parse_answer_detail(body: str):
    raw = (body or '').strip()
    n = normalize(raw)
    parts = [p for p in n.split() if p]
    if not parts:
        return None, None, raw
    first, rest = parts[0], parts[1:]
    ans, detail = None, None
    if first.startswith('SYGN'):
        ans, detail = 'TAK', 'SYGNALY'
    elif first.startswith('OBJA'):
        ans, detail = 'TAK', 'OBJAWY'
    elif first in ('TAK','YES','Y'):
        ans = 'TAK'
    elif first in ('NIE','NO','N'):
        ans = 'NIE'
    if ans == 'TAK' and detail is None:
        if any(t.startswith('SYGN') or t == 'AURA' for t in rest):
            detail = 'SYGNALY'
        elif any(t.startswith('OBJA') or t in ('BOL','PAIN') for t in rest):
            detail = 'OBJAWY'
    return ans, detail, raw

def twiml(msg: str) -> bytes:
    s = f'<?xml version="1.0" encoding="UTF-8"?><Response><Message>{msg}</Message></Response>'
    return s.encode('utf-8')

class Handler(BaseHTTPRequestHandler):
    server_version = VERSION
    protocol_version = 'HTTP/1.0'

    def do_POST(self):
        try:
            length = int(self.headers.get('Content-Length', '0'))
            body = self.rfile.read(length).decode('utf-8', errors='replace')
            form = parse_qs(body)
            phone = form.get('From', [''])[0]
            text = form.get('Body', [''])[0]
            ua = self.headers.get('User-Agent', '')
            rip = self.client_address[0] if self.client_address else ''
            ans, detail, raw = parse_answer_detail(text)
            if ans is None:
                msg = 'Odpisz proszę: NIE / TAK SYGNAŁY / TAK OBJAWY'
            else:
                record_feedback(phone=phone, answer=ans, detail=detail, raw=raw, remote_addr=rip, user_agent=ua, profile_hint='migraine')
                if ans == 'TAK' and detail is None:
                    msg = 'Dzięki! Doprecyzuj: SYGNAŁY czy OBJAWY? Odpisz: SYGNAŁY albo OBJAWY.'
                elif ans == 'TAK' and detail == 'SYGNALY':
                    msg = 'Dzięki! Zapisane: TAK (sygnały).'
                elif ans == 'TAK' and detail == 'OBJAWY':
                    msg = 'Dzięki! Zapisane: TAK (objawy).'
                else:
                    msg = 'Dzięki! Zapisane: NIE.'
            payload = twiml(msg)
            self.send_response(200)
            self.send_header('Content-Type', 'application/xml; charset=utf-8')
            self.send_header('Content-Length', str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
        except Exception:
            log.exception('Webhook error')
            payload = twiml('Wystąpił błąd po stronie serwera. Spróbuj ponownie później.')
            self.send_response(200)
            self.send_header('Content-Type', 'application/xml; charset=utf-8')
            self.send_header('Content-Length', str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

    def log_message(self, fmt, *args):
        log.info('%s - %s', self.address_string(), fmt % args)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--env', default='')
    ap.add_argument('--host', default='127.0.0.1')
    ap.add_argument('--port', type=int, default=8020)
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')
    load_env(args.env)
    ensure_schema()
    httpd = HTTPServer((args.host, args.port), Handler)
    log.info('Listening on %s:%s', args.host, args.port)
    httpd.serve_forever()

if __name__ == '__main__':
    main()
