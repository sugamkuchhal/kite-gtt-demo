#!/usr/bin/env python3
import argparse
import logging
import sys
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib
import getpass
import json
import os
import urllib.request
import urllib.parse

from runtime_paths import get_creds_path, get_smtp_token_path, get_telegram_token_path
from ref_sheets_utils import resolve_sheet_id

import atexit
from script_logger import log_start, log_end

_RUN_CTX = log_start("algo_winners_mailer")
atexit.register(log_end, _RUN_CTX)
SMTP_TOKEN_FILE = str(get_smtp_token_path())

# ==========================
# Config (constants)
# ==========================
ref_sheets = "HEDGE_PORTFOLIO"
tab_name = "Portfolio_BALANCE"
SERVICE_CREDS = str(get_creds_path())

# Column letters -> 0-indexed positions on the Portfolio_BALANCE tab
COL_TICKER = 0   # A
COL_WINNERS = 12  # M
COL_JTBD = 17     # R
COL_UNITS = 18    # S

# Email / SMTP settings
FROM_EMAIL = "sugamkuchhal@gmail.com"
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
SMTP_USER = "sugamkuchhal@gmail.com"

# Telegram settings
TELEGRAM_CHAT_ID = "182871861"
TELEGRAM_TOKEN_FILE = str(get_telegram_token_path())

# Sheet link is built from the resolved sheet id at runtime (see main())
SHEET_URL_TEMPLATE = "https://docs.google.com/spreadsheets/d/{sheet_id}/edit"

# ==========================
# Helpers
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


def read_sheet(sheet_id, tab_name, service_creds):
    logging.info("Authenticating to Google Sheets with service account: %s", service_creds)
    scope = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
    creds = Credentials.from_service_account_file(service_creds, scopes=scope)
    client = gspread.authorize(creds)
    ws = client.open_by_key(sheet_id).worksheet(tab_name)
    rows = ws.get_all_values()
    logging.info("Read %d total rows (including header/empty rows) from tab '%s'.", len(rows), tab_name)
    return rows


def validate_header(header_row):
    """
    Fail loudly if the expected columns aren't where we think they are,
    rather than silently misreading a reshuffled sheet.
    """
    expected = {
        COL_TICKER: "TICKER",
        COL_WINNERS: "WINNERS",
        COL_JTBD: "JTBD",
        COL_UNITS: "UNITS",
    }
    mismatches = []
    for idx, expected_label in expected.items():
        actual = header_row[idx].strip().upper() if len(header_row) > idx else ""
        if actual != expected_label:
            mismatches.append(f"column index {idx}: expected '{expected_label}', found '{actual}'")

    if mismatches:
        raise RuntimeError(
            "Header validation failed on '%s' tab — sheet layout may have changed:\n  %s"
            % (tab_name, "\n  ".join(mismatches))
        )

    logging.info("Header validation passed: TICKER/WINNERS/JTBD/UNITS columns confirmed.")


def extract_winners(rows):
    """
    Sheet columns (0-indexed) relevant to this script:
      A=0   ticker
      M=12  winners flag (TRUE/FALSE)
      R=17  jtbd (BUY/SELL)
      S=18  units (delta units to trade)

    Returns a list of (ticker, jtbd, units) tuples for rows where WINNERS == TRUE.
    """
    winners = []
    for row in rows:
        winners_flag = row[COL_WINNERS].strip().upper() if len(row) > COL_WINNERS else ""
        if winners_flag != "TRUE":
            continue

        ticker = row[COL_TICKER].strip() if len(row) > COL_TICKER else ""
        jtbd = row[COL_JTBD].strip() if len(row) > COL_JTBD else ""
        units_raw = row[COL_UNITS].strip() if len(row) > COL_UNITS else ""
        try:
            units = int(units_raw)
        except ValueError:
            units = units_raw  # leave as-is if not a clean int

        winners.append((ticker, jtbd, units))

    return winners


def _make_winners_table(winners):
    """Render a list of (ticker, jtbd, units) tuples as an HTML table."""
    TH = 'style="border:1px solid #ccc; padding:8px; text-align:left; background:#f2f2f2;"'
    base_td = 'border:1px solid #ddd; padding:8px;'
    headers = ["Ticker", "JTBD", "Units"]

    html = []
    html.append('<table style="border-collapse:collapse; width:100%; font-family: Arial, Helvetica, sans-serif; margin-bottom:16px;">')
    html.append("<thead><tr>")
    for h in headers:
        html.append(f'<th {TH}>{h}</th>')
    html.append("</tr></thead><tbody>")

    for ticker, jtbd, units in winners:
        if str(jtbd).strip().upper() == "BUY":
            row_style = "background:#e8f5e9;"  # green tint
        elif str(jtbd).strip().upper() == "SELL":
            row_style = "background:#fde8e8;"  # red tint
        else:
            row_style = "background:#fff8e1;"  # yellow tint (unknown)
        html.append(f'<tr style="{row_style}">')
        html.append(f'<td style="{base_td}">{ticker}</td>')
        html.append(f'<td style="{base_td} font-weight:bold;">{jtbd}</td>')
        html.append(f'<td style="{base_td} text-align:right;">{units}</td>')
        html.append("</tr>")

    html.append("</tbody></table>")
    return "\n".join(html)


def format_winners_email(winners, subject_date, sheet_url):
    html = []
    html.append(f'<p style="font-family:Arial,Helvetica,sans-serif; font-size:14px;">'
                f'<b>ALGO WINNERS {subject_date}</b><br>'
                f'Sheet: <b>{tab_name}</b> &nbsp;|&nbsp; Date: {subject_date}<br>'
                f'<a href="{sheet_url}">View Sheet &rarr;</a>'
                f'</p>')
    html.append(_make_winners_table(winners))
    return "\n".join(html)


def format_telegram_message(winners, subject_date, sheet_url):
    """Build a Telegram-compatible HTML message (bold, italic, links only)."""
    lines = []
    lines.append(f"<b>ALGO WINNERS {subject_date}</b>")
    lines.append(f'<a href="{sheet_url}">View Sheet →</a>')
    lines.append("")
    lines.append("<b>Winners:</b>")
    for ticker, jtbd, units in winners:
        lines.append(f"• {ticker} — <b>{jtbd}</b> — {units}")
    return "\n".join(lines)


def send_via_smtp(from_email, to_list, subject, html_body, smtp_server, smtp_port, smtp_user, smtp_password):
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

# ==========================
# Main
# ==========================

def parse_args():
    p = argparse.ArgumentParser(description="Send ALGO WINNERS email + Telegram alert")
    p.add_argument("--emails", required=True, help="Comma-separated list of recipient emails")
    return p.parse_args()


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

    args = parse_args()
    recipients = [e.strip() for e in args.emails.split(",") if e.strip()]

    sheet_id = resolve_sheet_id(ref_sheets)
    sheet_url = SHEET_URL_TEMPLATE.format(sheet_id=sheet_id)
    rows = read_sheet(sheet_id, tab_name, SERVICE_CREDS)
    if not rows:
        logging.error("No rows read from '%s' tab — aborting.", tab_name)
        sys.exit(1)

    validate_header(rows[0])
    data = rows[1:]  # skip header row

    subject_date = datetime.today().strftime("%d-%b-%Y")
    subject = f"ALGO WINNERS {subject_date}"
    logging.info("Prepared email subject: %s", subject)
    logging.info("Recipients: %s", ", ".join(recipients))
    logging.info("Total rows read from %s tab: %d", tab_name, len(data))

    winners = extract_winners(data)
    logging.info("Found %d winner row(s).", len(winners))

    if not winners:
        logging.info("No winners flagged — skipping email and Telegram.")
        return

    html_body = format_winners_email(winners, subject_date, sheet_url)

    # try to load saved token first
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

    # Telegram (non-fatal on failure)
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
            tg_text = format_telegram_message(winners, subject_date, sheet_url)
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
