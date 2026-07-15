#!/usr/bin/env python3
"""
dividend_action_mailer.py

Reports dividends LOCKED IN at today's close: stocks held in the portfolio
whose dividend ex-date is the NEXT TRADING DAY. Holding at today's close
means the dividend is earned regardless of any sale afterwards.

- Holdings come from the PORTFOLIO sheet, "Portfolio" tab:
    column A = ticker, column C = actual quantity (header row skipped).
- Dividends come from ONE bulk NSE corporate actions call for the next
  trading day (weekends + holidays.txt respected).
- Per-share amount is parsed from NSE's free-text subject; multiple
  amounts in one subject (special dividends) are SUMMED. Unparseable
  amounts are reported with "?" rather than dropped.
- Earnings are appended to the "Dividend_Ledger" tab on the PORTFOLIO
  sheet, deduped on (Symbol, Ex-Date) so re-runs never double-count.
- Email is the primary channel (failure fatal); Telegram is non-fatal.
- Nothing matched -> no email, no Telegram, no ledger rows.
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
import gspread
from google.oauth2.service_account import Credentials

from runtime_paths import (get_creds_path, get_smtp_token_path,
                           get_telegram_token_path, repo_root,
                           SMTP_FROM, SMTP_USER, SMTP_SERVER, SMTP_PORT, TELEGRAM_CHAT_ID)
from ref_sheets_utils import resolve_sheet_id

import atexit
from script_logger import log_start, log_end

_RUN_CTX = log_start("dividend_action_mailer")
atexit.register(log_end, _RUN_CTX)

SMTP_TOKEN_FILE = str(get_smtp_token_path())
SERVICE_CREDS = str(get_creds_path())

# ==========================
# Config (constants)
# ==========================
ref_sheets = "PORTFOLIO"
HOLDINGS_TAB = "Portfolio"          # col A = ticker, col C = actual quantity
LEDGER_TAB = "Dividend_Ledger"
LEDGER_HEADERS = ["Locked Date", "Ex-Date", "Symbol", "Qty",
                  "Div/Share", "Amount", "Remarks"]
HOLIDAYS_FILE = "holidays.txt"      # repo file, DD-MM-YYYY per line

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

DIVIDEND_RE = re.compile(r"dividend", re.IGNORECASE)
# Amounts immediately preceding "Per Share" (Rs/Re prefix optional — NSE
# sometimes writes "Special Dividend - 8 Per Share").
_AMT_PER_SHARE_RE = re.compile(
    r"(?:R(?:s|e)\.?\s*)?(\d+(?:\.\d+)?)\s*(?:/-)?\s*per\s*share",
    re.IGNORECASE,
)
# Fallback: any amount right after Rs/Re.
_AMT_RS_RE = re.compile(r"R(?:s|e)\.?\s*(\d+(?:\.\d+)?)", re.IGNORECASE)
# Percent-of-face-value dividends (e.g. 'Dividend - 50%').
_AMT_PCT_RE = re.compile(r"(\d+(?:\.\d+)?)\s*%")

# Email / SMTP settings


# Telegram settings

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
# Calendar
# ==========================

def load_holidays(path):
    """Load market holidays (DD-MM-YYYY per line) into a set of dates."""
    holidays = set()
    if not os.path.exists(path):
        logging.warning("Holidays file not found at %s — using weekends only.", path)
        return holidays
    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
            s = line.strip()
            if not s or s.startswith("#"):
                continue
            try:
                holidays.add(datetime.strptime(s, "%d-%m-%Y").date())
            except ValueError:
                logging.warning("Skipping unparseable holiday line: %r", s)
    logging.info("Loaded %d market holidays.", len(holidays))
    return holidays


def next_trading_day(today, holidays):
    """First day after `today` that is neither a weekend nor a holiday."""
    d = today + timedelta(days=1)
    while d.weekday() >= 5 or d in holidays:
        d += timedelta(days=1)
    return d

# ==========================
# Holdings (Google Sheet)
# ==========================

def _gs_client():
    scope = ["https://spreadsheets.google.com/feeds",
             "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_file(SERVICE_CREDS, scopes=scope)
    return gspread.authorize(creds)


def normalize_symbol(raw):
    """'NSE:RELIANCE' / 'reliance.ns' / ' RELIANCE ' -> 'RELIANCE'."""
    s = str(raw).strip().upper()
    if s.startswith("NSE:"):
        s = s[len("NSE:"):]
    for suffix in (".NS", ".BO"):
        if s.endswith(suffix):
            s = s[: -len(suffix)]
    return s.strip()


def read_holdings(sheet_id):
    """
    Read holdings from the Portfolio tab: col A = ticker, col C = quantity.
    Header row skipped; blank/zero/non-numeric quantities skipped;
    duplicate tickers have their quantities summed.
    Returns {symbol: qty}.
    """
    client = _gs_client()
    ws = client.open_by_key(sheet_id).worksheet(HOLDINGS_TAB)
    rows = ws.get_all_values()
    holdings = {}
    for row in rows[1:]:  # skip header
        symbol = normalize_symbol(row[0]) if len(row) > 0 else ""
        if not symbol:
            continue
        qty_raw = str(row[2]).replace(",", "").strip() if len(row) > 2 else ""
        try:
            qty = int(float(qty_raw))
        except (TypeError, ValueError):
            continue
        if qty <= 0:
            continue
        holdings[symbol] = holdings.get(symbol, 0) + qty
    logging.info("Read %d holdings with positive quantity from %s!%s.",
                 len(holdings), ref_sheets, HOLDINGS_TAB)
    return holdings

# ==========================
# NSE fetch (same approach as corporate_actions_mailer.py)
# ==========================

def fetch_nse_corporate_actions(from_date, to_date):
    """Single bulk fetch of equity corporate actions in [from_date, to_date]."""
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
            session.get(NSE_CA_PAGE, timeout=15)  # warm-up: cookies
            time.sleep(1.0 + random.random())
            resp = session.get(url, timeout=30)
            resp.raise_for_status()
            payload = resp.json()
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
# Dividend extraction
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


def parse_dividend_per_share(subject, face_value=None):
    """
    Extract the total per-share dividend from NSE's free-text subject,
    summing multiple amounts (e.g. dividend + special dividend).
    Handles percent-of-face-value amounts (e.g. 'Dividend - 50%' with
    face value 2 -> Rs 1). Returns float or None if nothing parseable.

    Primary: amounts followed by "Per Share" (Rs/Re prefix optional).
    Fallback: any amounts right after Rs/Re.
    """
    amounts = [float(m) for m in _AMT_PER_SHARE_RE.findall(subject)]
    if not amounts:
        amounts = [float(m) for m in _AMT_RS_RE.findall(subject)]
    pcts = [float(m) for m in _AMT_PCT_RE.findall(subject)]
    if pcts and face_value:
        amounts.extend(p * face_value / 100.0 for p in pcts)
    if not amounts:
        return None
    return round(sum(amounts), 4)


def parse_face_value(row):
    try:
        return float(str(row.get("faceVal", "")).replace(",", "").strip())
    except (TypeError, ValueError):
        return None


def match_dividends(raw_rows, holdings, target_ex_date):
    """
    Filter NSE rows to: symbol held, subject mentions dividend,
    ex-date == target_ex_date. Dedup on (symbol, subject).
    Returns list of dicts:
      {symbol, company, subject, ex_date, qty, per_share, amount}
    sorted by amount descending (unparseable last).
    """
    entries = []
    seen = set()
    for row in raw_rows:
        if not isinstance(row, dict):
            continue
        symbol = str(row.get("symbol", "")).strip().upper()
        if symbol not in holdings:
            continue
        subject = str(row.get("subject", "") or row.get("purpose", "")).strip()
        if not DIVIDEND_RE.search(subject):
            continue
        ex_date = parse_nse_date(row.get("exDate") or row.get("exdate"))
        if ex_date != target_ex_date:
            continue
        key = (symbol, subject.lower())
        if key in seen:
            continue
        seen.add(key)
        qty = holdings[symbol]
        per_share = parse_dividend_per_share(subject, parse_face_value(row))
        amount = round(per_share * qty, 2) if per_share is not None else None
        entries.append({
            "symbol": symbol,
            "company": str(row.get("comp", "") or row.get("company", "")).strip(),
            "subject": subject,
            "ex_date": ex_date,
            "qty": qty,
            "per_share": per_share,
            "amount": amount,
        })
    entries.sort(key=lambda e: (e["amount"] is None, -(e["amount"] or 0)))
    logging.info("Matched %d dividend entries for holdings with ex-date %s.",
                 len(entries), target_ex_date.strftime("%d-%b-%Y"))
    return entries

# ==========================
# Ledger (Google Sheet)
# ==========================

def append_to_ledger(sheet_id, entries, locked_date):
    """
    Append dividend entries to the Dividend_Ledger tab, deduped on
    (Symbol, Ex-Date) against existing rows. Writes headers if the tab
    is empty. Returns (appended_count, skipped_count).
    """
    client = _gs_client()
    ws = client.open_by_key(sheet_id).worksheet(LEDGER_TAB)
    existing = ws.get_all_values()

    if not existing:
        ws.update(values=[LEDGER_HEADERS], range_name="A1")
        existing = [LEDGER_HEADERS]
        logging.info("Wrote headers to empty %s tab.", LEDGER_TAB)

    # Existing (Symbol, Ex-Date) pairs; column positions per LEDGER_HEADERS.
    seen = set()
    for row in existing[1:]:
        sym = str(row[2]).strip().upper() if len(row) > 2 else ""
        exd = str(row[1]).strip() if len(row) > 1 else ""
        if sym:
            seen.add((sym, exd))

    new_rows, skipped = [], 0
    for e in entries:
        ex_label = e["ex_date"].strftime("%d-%b-%Y")
        if (e["symbol"], ex_label) in seen:
            skipped += 1
            continue
        new_rows.append([
            locked_date.strftime("%d-%b-%Y"),
            ex_label,
            e["symbol"],
            e["qty"],
            e["per_share"] if e["per_share"] is not None else "",
            e["amount"] if e["amount"] is not None else "",
            e["subject"],
        ])

    if new_rows:
        ws.append_rows(new_rows, value_input_option="USER_ENTERED")
    logging.info("Ledger: appended %d row(s), skipped %d duplicate(s).",
                 len(new_rows), skipped)
    return len(new_rows), skipped

# ==========================
# Formatting
# ==========================

def _fmt_inr(value):
    """Indian-style grouping: 1234567.5 -> '12,34,567.50'."""
    if value is None:
        return "?"
    neg = value < 0
    value = abs(value)
    whole = int(value)
    frac = f"{value - whole:.2f}"[1:]  # '.xx'
    s = str(whole)
    if len(s) > 3:
        head, tail = s[:-3], s[-3:]
        parts = []
        while len(head) > 2:
            parts.insert(0, head[-2:])
            head = head[:-2]
        if head:
            parts.insert(0, head)
        s = ",".join(parts) + "," + tail
    return ("-" if neg else "") + s + frac


def format_email(entries, total, unparsed_count, locked_date, ex_date):
    """Build full HTML email body."""
    TH = 'style="border:1px solid #ccc; padding:8px; text-align:left; background:#f2f2f2;"'
    base_td = 'border:1px solid #ddd; padding:8px;'

    html = []
    html.append(f'<p style="font-family:Arial,Helvetica,sans-serif; font-size:14px;">'
                f'<b>Dividends locked in at close of {locked_date.strftime("%d-%b-%Y")}</b><br>'
                f'Ex-date: <b>{ex_date.strftime("%d-%b-%Y")}</b> &mdash; these are yours '
                f'even if sold on or after the ex-date. Cash typically arrives within '
                f'30 days of the record date.'
                f'</p>')

    html.append(f'<p style="font-family:Arial,Helvetica,sans-serif; font-size:16px;">'
                f'Total earned: <b>&#8377;{_fmt_inr(total)}</b>'
                + (f' <span style="color:#c00;">(+ {unparsed_count} unparsed — see below)</span>'
                   if unparsed_count else '')
                + '</p>')

    html.append('<table style="border-collapse:collapse; width:100%; '
                'font-family: Arial, Helvetica, sans-serif; margin-bottom:16px;">')
    html.append("<thead><tr>")
    for h in ["Symbol", "Company", "Dividend", "Div/Share", "Qty", "Amount"]:
        html.append(f'<th {TH}>{h}</th>')
    html.append("</tr></thead><tbody>")

    for e in entries:
        row_style = "background:#fde8e8;" if e["amount"] is None else "background:#fff;"
        per_share = f'&#8377;{_fmt_inr(e["per_share"])}' if e["per_share"] is not None else "?"
        amount = f'&#8377;{_fmt_inr(e["amount"])}' if e["amount"] is not None else "?"
        html.append(f'<tr style="{row_style}">')
        html.append(f'<td style="{base_td}"><b>{_esc(e["symbol"])}</b></td>')
        html.append(f'<td style="{base_td}">{_esc(e["company"])}</td>')
        html.append(f'<td style="{base_td}">{_esc(e["subject"])}</td>')
        html.append(f'<td style="{base_td} text-align:right; white-space:nowrap;">{per_share}</td>')
        html.append(f'<td style="{base_td} text-align:right;">{e["qty"]}</td>')
        html.append(f'<td style="{base_td} text-align:right; white-space:nowrap; font-weight:bold;">{amount}</td>')
        html.append("</tr>")

    html.append("</tbody></table>")
    if unparsed_count:
        html.append('<p style="font-family:Arial,Helvetica,sans-serif; font-size:12px; color:#c00;">'
                    'Red rows: per-share amount could not be parsed from the NSE text — '
                    'amount excluded from the total; fix manually in the ledger.</p>')
    return "\n".join(html)


def format_telegram_message(entries, total, unparsed_count, locked_date):
    """Compact Telegram-HTML summary."""
    lines = []
    lines.append(f"<b>DIVIDENDS LOCKED {locked_date.strftime('%d-%b-%Y')}</b>")
    lines.append(f"Total earned: <b>₹{_fmt_inr(total)}</b>"
                 + (f" (+{unparsed_count} unparsed)" if unparsed_count else ""))
    lines.append("")
    for e in entries:
        amount = f"₹{_fmt_inr(e['amount'])}" if e["amount"] is not None else "?"
        lines.append(f"• <b>{_esc(e['symbol'])}</b> × {e['qty']} — {amount}")
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
    p = argparse.ArgumentParser(description="Send dividends-earned email for portfolio holdings")
    p.add_argument("--emails", required=True, help="Comma-separated list of recipient emails")
    p.add_argument("--dry-run", action="store_true",
                   help="Fetch and print the report; skip email, Telegram, and ledger write")
    return p.parse_args()


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

    args = parse_args()
    recipients = [e.strip() for e in args.emails.split(",") if e.strip()]

    today = date.today()
    holidays = load_holidays(str(repo_root() / HOLIDAYS_FILE))
    ex_date = next_trading_day(today, holidays)
    logging.info("Locked date: %s | next trading day (target ex-date): %s",
                 today.strftime("%d-%b-%Y"), ex_date.strftime("%d-%b-%Y"))

    sheet_id = resolve_sheet_id(ref_sheets)
    holdings = read_holdings(sheet_id)
    if not holdings:
        logging.warning("No holdings with positive quantity — nothing to do.")
        return

    raw_rows = fetch_nse_corporate_actions(ex_date, ex_date)
    entries = match_dividends(raw_rows, holdings, ex_date)

    if not entries:
        logging.info("No dividends locked for holdings with ex-date %s — "
                     "skipping email, Telegram, and ledger.",
                     ex_date.strftime("%d-%b-%Y"))
        return

    total = round(sum(e["amount"] for e in entries if e["amount"] is not None), 2)
    unparsed_count = sum(1 for e in entries if e["amount"] is None)

    subject = (f"DIVIDENDS LOCKED {today.strftime('%d-%b-%Y')} — "
               f"Rs {_fmt_inr(total)}")
    html_body = format_email(entries, total, unparsed_count, today, ex_date)
    tg_text = format_telegram_message(entries, total, unparsed_count, today)

    if args.dry_run:
        print("\n===== DRY RUN: email subject =====")
        print(subject)
        print("\n===== DRY RUN: email HTML =====")
        print(html_body)
        print("\n===== DRY RUN: telegram text =====")
        print(tg_text)
        print("\n===== DRY RUN: ledger rows (not written) =====")
        for e in entries:
            print([today.strftime("%d-%b-%Y"), e["ex_date"].strftime("%d-%b-%Y"),
                   e["symbol"], e["qty"], e["per_share"], e["amount"], e["subject"]])
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

    # Ledger append (failure surfaces as a red run, after Telegram attempt)
    ledger_failed = False
    try:
        append_to_ledger(sheet_id, entries, today)
    except Exception as e:
        logging.exception("Ledger append failed: %s", e)
        ledger_failed = True

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

    if ledger_failed:
        sys.exit(1)


if __name__ == "__main__":
    try:
        main()
        raise SystemExit(0)
    except KeyboardInterrupt:
        logging.warning("Interrupted by user.")
        raise SystemExit(130)
