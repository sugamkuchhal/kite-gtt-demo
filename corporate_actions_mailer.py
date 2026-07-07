#!/usr/bin/env python3
"""
corporate_actions_mailer.py

Fetches NSE corporate actions (dividends, splits, bonuses, rights, buybacks,
demergers, ...) for the tickers in nse_stock_list.txt and mails a report.

Design:
- ONE bulk call to the NSE corporate actions API for a date window
  (today - RECENT_DAYS .. today + UPCOMING_DAYS), then filter locally
  against the ticker file. No per-symbol hammering.
- Actions are classified into:
    Upcoming (ex-date >= today)  -> advance warning; GTT-critical actions
                                    (split/bonus/rights/demerger/...) highlighted
    Recent   (ex-date <  today)  -> for record/quantity adjustments
- If nothing is found in either bucket, email and Telegram are skipped.
- Telegram failures are non-fatal (email is the primary channel).
"""
import argparse
import getpass
import json
import logging
import os
import random
import re
import sys
import time
import urllib.parse
import urllib.request
from datetime import date, datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib
from html import escape as _esc

import requests

from runtime_paths import get_smtp_token_path, get_telegram_token_path, repo_root

import atexit
from script_logger import log_start, log_end

_RUN_CTX = log_start("corporate_actions_mailer")
atexit.register(log_end, _RUN_CTX)

SMTP_TOKEN_FILE = str(get_smtp_token_path())

# ==========================
# Config (constants)
# ==========================
TICKER_FILE = "nse_stock_list.txt"     # repo file, one symbol per line
UPCOMING_DAYS = 30                     # look-ahead window for announced actions
RECENT_DAYS = 7                        # look-back window for completed actions

NSE_BASE = "https://www.nseindia.com"
NSE_CA_API = NSE_BASE + "/api/corporates-corporateActions"
NSE_CA_PAGE = NSE_BASE + "/companies-listing/corporate-filings-actions"
NSE_HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/124.0.0.0 Safari/537.36"),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": NSE_CA_PAGE,
}
FETCH_RETRIES = 3
FETCH_BACKOFF_BASE_SECS = 5

# Actions whose ex-date changes the traded price materially enough to
# invalidate GTT trigger levels. Matched case-insensitively on the
# "subject"/purpose text from NSE.
GTT_CRITICAL_RE = re.compile(
    r"split|bonus|right|demerger|de-merger|consolidat|reduction|scheme of arrangement|buy\s*back|buyback",
    re.IGNORECASE,
)

# Email / SMTP settings
FROM_EMAIL = "sugamkuchhal@gmail.com"
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
SMTP_USER = "sugamkuchhal@gmail.com"

# Telegram settings
TELEGRAM_CHAT_ID = "182871861"
TELEGRAM_TOKEN_FILE = str(get_telegram_token_path())

# ==========================
# Token helpers (same pattern as algo_tickers_mailer.py)
# ==========================

def load_smtp_token(path=SMTP_TOKEN_FILE):
    """Return stored SMTP password string, or None if file missing/invalid."""
    try:
        if not os.path.exists(path):
            return None
        with open(path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
        pwd = data.get("smtp_password")
        return pwd if pwd else None
    except Exception:
        return None


def save_smtp_token(smtp_password, path=SMTP_TOKEN_FILE):
    """Save SMTP password to JSON file with minimal permissions."""
    data = {"smtp_password": smtp_password}
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(data, fh)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)
    try:
        os.chmod(path, 0o600)
    except Exception:
        pass


def load_telegram_token(path=TELEGRAM_TOKEN_FILE):
    """Return stored Telegram bot token, or None if file missing/invalid."""
    try:
        if not os.path.exists(path):
            return None
        with open(path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
        tok = data.get("telegram_token")
        return tok if tok else None
    except Exception:
        return None


def save_telegram_token(token, path=TELEGRAM_TOKEN_FILE):
    """Save Telegram bot token to JSON file with minimal permissions."""
    data = {"telegram_token": token}
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(data, fh)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)
    try:
        os.chmod(path, 0o600)
    except Exception:
        pass

# ==========================
# Ticker file
# ==========================

def read_ticker_file(path):
    """Load symbols from a txt file: one per line, tolerant of blanks,
    '#' comments, and stray .NS/.BO suffixes. Returns an uppercase set."""
    if not os.path.exists(path):
        raise FileNotFoundError(f"Ticker file not found: {path}")
    symbols = set()
    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
            s = line.strip().upper()
            if not s or s.startswith("#"):
                continue
            for suffix in (".NS", ".BO"):
                if s.endswith(suffix):
                    s = s[: -len(suffix)]
            if s.startswith("NSE:"):
                s = s[len("NSE:"):]
            if s:
                symbols.add(s)
    logging.info("Loaded %d symbols from %s", len(symbols), path)
    return symbols

# ==========================
# NSE fetch
# ==========================

def fetch_nse_corporate_actions(from_date, to_date):
    """
    Single bulk fetch of ALL equity corporate actions in [from_date, to_date].
    NSE needs a warmed-up session (homepage/listing page sets cookies) and
    browser-like headers. Retries with backoff and a fresh session each time.
    Returns the raw list of action dicts.
    """
    params = {
        "index": "equities",
        "from_date": from_date.strftime("%d-%m-%Y"),
        "to_date": to_date.strftime("%d-%m-%Y"),
    }
    url = NSE_CA_API + "?" + urllib.parse.urlencode(params)
    last_err = None
    for attempt in range(1, FETCH_RETRIES + 1):
        try:
            session = requests.Session()
            session.headers.update(NSE_HEADERS)
            # Warm-up: pick up NSE cookies before hitting the API.
            session.get(NSE_CA_PAGE, timeout=15)
            time.sleep(1.0 + random.random())
            resp = session.get(url, timeout=30)
            resp.raise_for_status()
            payload = resp.json()
            # API returns either a bare list or {"data": [...]}.
            data = payload.get("data") if isinstance(payload, dict) else payload
            if data is None:
                data = []
            if not isinstance(data, list):
                raise ValueError(f"Unexpected NSE payload type: {type(data)}")
            logging.info("NSE returned %d corporate action rows for %s -> %s.",
                         len(data), params["from_date"], params["to_date"])
            return data
        except Exception as e:
            last_err = e
            logging.warning("NSE fetch attempt %d/%d failed: %s",
                            attempt, FETCH_RETRIES, e)
            if attempt < FETCH_RETRIES:
                sleep_secs = FETCH_BACKOFF_BASE_SECS * attempt + random.random() * 2
                logging.info("Retrying in %.1f seconds...", sleep_secs)
                time.sleep(sleep_secs)
    raise RuntimeError(f"NSE corporate actions fetch failed after "
                       f"{FETCH_RETRIES} attempts: {last_err}")

# ==========================
# Classification
# ==========================

def parse_nse_date(raw):
    """Parse NSE date strings like '26-Jun-2026'. Returns date or None."""
    if not raw:
        return None
    raw = str(raw).strip()
    for fmt in ("%d-%b-%Y", "%d-%m-%Y", "%d %b %Y"):
        try:
            return datetime.strptime(raw, fmt).date()
        except ValueError:
            continue
    return None


def classify_actions(raw_rows, my_symbols, today):
    """
    Filter raw NSE rows to our symbols and split into buckets.
    Returns (upcoming, recent, unparsed), each a list of dicts:
      {symbol, company, subject, ex_date, record_date, critical}
    sorted by ex-date (upcoming ascending: nearest deadline first;
    recent descending: newest first). Deduped on (symbol, subject, ex-date).
    """
    upcoming, recent, unparsed = [], [], []
    seen = set()
    for row in raw_rows:
        if not isinstance(row, dict):
            continue
        symbol = str(row.get("symbol", "")).strip().upper()
        if symbol not in my_symbols:
            continue
        subject = str(row.get("subject", "") or row.get("purpose", "")).strip()
        ex_raw = row.get("exDate") or row.get("exdate")
        rec_raw = row.get("recDate") or row.get("recordDate")
        key = (symbol, subject.lower(), str(ex_raw))
        if key in seen:
            continue
        seen.add(key)
        entry = {
            "symbol": symbol,
            "company": str(row.get("comp", "") or row.get("company", "")).strip(),
            "subject": subject,
            "ex_date": parse_nse_date(ex_raw),
            "ex_date_raw": str(ex_raw or ""),
            "record_date": str(rec_raw or ""),
            "critical": bool(GTT_CRITICAL_RE.search(subject)),
        }
        if entry["ex_date"] is None:
            unparsed.append(entry)
        elif entry["ex_date"] >= today:
            upcoming.append(entry)
        else:
            recent.append(entry)

    upcoming.sort(key=lambda e: (e["ex_date"], e["symbol"]))
    recent.sort(key=lambda e: (e["ex_date"], e["symbol"]), reverse=True)
    logging.info("Matched actions -> upcoming: %d, recent: %d, unparsed: %d",
                 len(upcoming), len(recent), len(unparsed))
    return upcoming, recent, unparsed

# ==========================
# Formatting
# ==========================

def _section_title(text):
    return (f'<p style="font-family:Arial,Helvetica,sans-serif; font-size:14px; '
            f'margin:18px 0 6px 0; border-bottom:2px solid #444;"><b>{text}</b></p>')


def _make_actions_table(entries, highlight_critical):
    """Render action entries as an HTML table (algo mailer styling)."""
    TH = 'style="border:1px solid #ccc; padding:8px; text-align:left; background:#f2f2f2;"'
    base_td = 'border:1px solid #ddd; padding:8px;'
    headers = ["Symbol", "Company", "Action", "Ex-Date", "Record Date"]

    html = []
    html.append('<table style="border-collapse:collapse; width:100%; '
                'font-family: Arial, Helvetica, sans-serif; margin-bottom:16px;">')
    html.append("<thead><tr>")
    for h in headers:
        html.append(f'<th {TH}>{h}</th>')
    html.append("</tr></thead><tbody>")

    for e in entries:
        if e["critical"] and highlight_critical:
            row_style = "background:#fde8e8;"  # red tint: GTT-critical
        elif e["critical"]:
            row_style = "background:#fff8e1;"  # yellow tint
        else:
            row_style = "background:#fff;"
        ex_label = e["ex_date"].strftime("%d-%b-%Y") if e["ex_date"] else e["ex_date_raw"]
        symbol_html = _esc(e["symbol"])
        if e["critical"]:
            symbol_html = f"<b>{symbol_html}</b> &#9888;"  # warning sign
        html.append(f'<tr style="{row_style}">')
        html.append(f'<td style="{base_td}">{symbol_html}</td>')
        html.append(f'<td style="{base_td}">{_esc(e["company"])}</td>')
        html.append(f'<td style="{base_td}">{_esc(e["subject"])}</td>')
        html.append(f'<td style="{base_td} white-space:nowrap;">{_esc(ex_label)}</td>')
        html.append(f'<td style="{base_td} white-space:nowrap;">{_esc(e["record_date"])}</td>')
        html.append("</tr>")

    html.append("</tbody></table>")
    return "\n".join(html)


def format_email(upcoming, recent, unparsed, subject_date, window_from, window_to):
    """Build full HTML email body."""
    n_critical = sum(1 for e in upcoming if e["critical"])
    html = []
    html.append(f'<p style="font-family:Arial,Helvetica,sans-serif; font-size:14px;">'
                f'<b>Corporate actions for portfolio tickers</b><br>'
                f'Window: {window_from.strftime("%d-%b-%Y")} &rarr; '
                f'{window_to.strftime("%d-%b-%Y")} &nbsp;|&nbsp; Date: {subject_date}<br>'
                f'Red rows are <b>GTT-critical</b> (split / bonus / rights / demerger / '
                f'buyback) &mdash; trigger prices will need review before the ex-date.'
                f'</p>')

    html.append(_section_title(
        f"1. Upcoming actions — next {UPCOMING_DAYS} days "
        f"({len(upcoming)} total, {n_critical} GTT-critical)"))
    if upcoming:
        html.append(_make_actions_table(upcoming, highlight_critical=True))
    else:
        html.append('<p style="font-family:Arial,Helvetica,sans-serif; color:#555;">'
                    'No upcoming actions in window.</p>')

    html.append(_section_title(
        f"2. Recent actions — last {RECENT_DAYS} days ({len(recent)})"))
    if recent:
        html.append(_make_actions_table(recent, highlight_critical=False))
    else:
        html.append('<p style="font-family:Arial,Helvetica,sans-serif; color:#555;">'
                    'No recent actions in window.</p>')

    if unparsed:
        html.append(_section_title(f"3. Actions with unparseable ex-date ({len(unparsed)})"))
        html.append(_make_actions_table(unparsed, highlight_critical=True))

    return "\n".join(html)


def format_telegram_message(upcoming, recent, subject_date):
    """Compact Telegram-HTML summary (bold/italic only)."""
    lines = []
    lines.append(f"<b>CORPORATE ACTIONS {subject_date}</b>")
    lines.append("")
    if upcoming:
        lines.append(f"<b>Upcoming ({len(upcoming)}):</b>")
        for e in upcoming:
            ex_label = e["ex_date"].strftime("%d-%b") if e["ex_date"] else "?"
            mark = " ⚠️" if e["critical"] else ""
            lines.append(f"• <b>{_esc(e['symbol'])}</b> — {_esc(e['subject'])} — ex {ex_label}{mark}")
    else:
        lines.append("No upcoming actions in window.")
    if recent:
        lines.append("")
        lines.append(f"<i>Recent ({len(recent)}):</i>")
        for e in recent:
            ex_label = e["ex_date"].strftime("%d-%b") if e["ex_date"] else "?"
            lines.append(f"• {_esc(e['symbol'])} — {_esc(e['subject'])} — ex {ex_label}")
    return "\n".join(lines)

# ==========================
# Senders (same pattern as algo_tickers_mailer.py)
# ==========================

def send_via_smtp(from_email, to_list, subject, html_body,
                  smtp_server, smtp_port, smtp_user, smtp_password):
    logging.info("Sending via SMTP server %s:%s as user %s", smtp_server, smtp_port, smtp_user)
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["To"] = ", ".join(to_list)
    msg["From"] = from_email or smtp_user

    part1 = MIMEText("This email contains HTML. Please view in an HTML-capable client.", "plain")
    part2 = MIMEText(html_body, "html")
    msg.attach(part1)
    msg.attach(part2)

    with smtplib.SMTP(smtp_server, smtp_port, timeout=30) as server:
        server.ehlo()
        server.starttls()
        server.ehlo()
        server.login(smtp_user, smtp_password)
        server.sendmail(msg["From"], to_list, msg.as_string())

    logging.info("SMTP send completed successfully.")


def send_via_telegram(bot_token, chat_id, text):
    """Send a message via Telegram Bot API using stdlib only."""
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = urllib.parse.urlencode({
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": "true",
    }).encode()
    req = urllib.request.Request(url, data=payload, method="POST")
    with urllib.request.urlopen(req, timeout=15) as resp:
        result = json.loads(resp.read())
    if not result.get("ok"):
        raise RuntimeError(f"Telegram API error: {result}")
    logging.info("Telegram message sent successfully.")

# ==========================
# Main
# ==========================

def parse_args():
    p = argparse.ArgumentParser(description="Send corporate actions email for portfolio tickers")
    p.add_argument("--emails", required=True, help="Comma-separated list of recipient emails")
    p.add_argument("--ticker-file", default=None,
                   help=f"Path to ticker list (default: repo {TICKER_FILE})")
    p.add_argument("--dry-run", action="store_true",
                   help="Fetch and print the report; skip email and Telegram")
    return p.parse_args()


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

    args = parse_args()
    recipients = [e.strip() for e in args.emails.split(",") if e.strip()]

    ticker_path = args.ticker_file or str(repo_root() / TICKER_FILE)
    my_symbols = read_ticker_file(ticker_path)

    today = date.today()
    window_from = today - timedelta(days=RECENT_DAYS)
    window_to = today + timedelta(days=UPCOMING_DAYS)

    raw_rows = fetch_nse_corporate_actions(window_from, window_to)
    upcoming, recent, unparsed = classify_actions(raw_rows, my_symbols, today)

    if not upcoming and not recent and not unparsed:
        logging.info("No corporate actions for portfolio tickers in window — "
                     "skipping email and Telegram.")
        return

    subject_date = datetime.today().strftime("%d-%b-%Y")
    n_critical = sum(1 for e in upcoming if e["critical"])
    subject = f"CORPORATE ACTIONS {subject_date}"
    if n_critical:
        subject += f" — {n_critical} GTT-CRITICAL"

    html_body = format_email(upcoming, recent, unparsed, subject_date, window_from, window_to)
    tg_text = format_telegram_message(upcoming, recent, subject_date)

    if args.dry_run:
        print("\n===== DRY RUN: email subject =====")
        print(subject)
        print("\n===== DRY RUN: email HTML =====")
        print(html_body)
        print("\n===== DRY RUN: telegram text =====")
        print(tg_text)
        return

    logging.info("Prepared email subject: %s", subject)
    logging.info("Recipients: %s", ", ".join(recipients))

    # Email (primary channel — failure is fatal)
    smtp_password = load_smtp_token()
    if smtp_password:
        logging.info("Loaded SMTP password from %s", SMTP_TOKEN_FILE)
    else:
        smtp_password = getpass.getpass("Enter SMTP password (App Password for Gmail): ")
        if smtp_password:
            try:
                save_smtp_token(smtp_password)
                logging.info("Saved SMTP password to %s (permission 600).", SMTP_TOKEN_FILE)
            except Exception as e:
                logging.warning("Could not save SMTP token to %s: %s", SMTP_TOKEN_FILE, e)

    try:
        send_via_smtp(
            FROM_EMAIL, recipients, subject, html_body,
            SMTP_SERVER, SMTP_PORT, SMTP_USER, smtp_password
        )
        logging.info("Email sent successfully.")
    except Exception as e:
        logging.exception("Failed to send email: %s", e)
        sys.exit(1)

    # Telegram (non-fatal)
    telegram_token = load_telegram_token()
    if not telegram_token:
        telegram_token = getpass.getpass("Enter Telegram bot token: ")
        if telegram_token:
            try:
                save_telegram_token(telegram_token)
                logging.info("Saved Telegram token to %s (permission 600).", TELEGRAM_TOKEN_FILE)
            except Exception as e:
                logging.warning("Could not save Telegram token: %s", e)

    if telegram_token:
        try:
            send_via_telegram(telegram_token, TELEGRAM_CHAT_ID, tg_text)
        except Exception as e:
            logging.warning("Telegram send failed (non-fatal): %s", e)
    else:
        logging.warning("No Telegram token available — skipping Telegram.")


if __name__ == "__main__":
    try:
        main()
        raise SystemExit(0)
    except KeyboardInterrupt:
        logging.warning("Interrupted by user.")
        raise SystemExit(130)
