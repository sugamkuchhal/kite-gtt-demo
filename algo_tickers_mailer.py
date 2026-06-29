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

from runtime_paths import get_creds_path, get_smtp_token_path
from ref_sheets_utils import resolve_sheet_id

import atexit
from script_logger import log_start, log_end

_RUN_CTX = log_start("algo_tickers_mailer")
atexit.register(log_end, _RUN_CTX)
SMTP_TOKEN_FILE = str(get_smtp_token_path())

# ==========================
# Config (constants)
# ==========================
ref_sheets = "TICKER"
tab_name = "Checklist"
CHECKLIST_URL = "https://docs.google.com/spreadsheets/d/143py3t5oTsz0gAfp8VpSJlpR5VS8Z4tfl067pMtW1EE/edit?gid=844019911"
SERVICE_CREDS = str(get_creds_path())


# Email / SMTP settings
FROM_EMAIL = "sugamkuchhal@gmail.com"
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
SMTP_USER = "sugamkuchhal@gmail.com"

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
        # do not crash on malformed file; treat as absent
        return None

def save_smtp_token(smtp_password, path=SMTP_TOKEN_FILE):
    """Save SMTP password to JSON file with minimal permissions."""
    data = {"smtp_password": smtp_password}
    # write atomically
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(data, fh)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)
    try:
        # restrict file permissions to owner only (Unix)
        os.chmod(path, 0o600)
    except Exception:
        pass

def read_sheet(sheet_id, tab_name, service_creds):
    logging.info("Authenticating to Google Sheets with service account: %s", service_creds)
    scope = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
    creds = Credentials.from_service_account_file(service_creds, scopes=scope)
    client = gspread.authorize(creds)
    ws = client.open_by_key(sheet_id).worksheet(tab_name)
    rows = ws.get_all_values()
    logging.info("Read %d total rows (including header/empty rows) from tab '%s'.", len(rows), tab_name)
    return rows

def _make_table(rows, grey=False):
    """Render a list of (check, sheet, tab, count) tuples as an HTML table."""
    TH = 'style="border:1px solid #ccc; padding:8px; text-align:left; background:#f2f2f2;"'
    base_td = 'border:1px solid #ddd; padding:8px;'
    headers = ["Check", "Sheet", "Tab", "Count"]

    html = []
    html.append('<table style="border-collapse:collapse; width:100%; font-family: Arial, Helvetica, sans-serif; margin-bottom:16px;">')
    html.append("<thead><tr>")
    for h in headers:
        html.append(f'<th {TH}>{h}</th>')
    html.append("</tr></thead><tbody>")

    for check, sheet, tab, count in rows:
        if grey:
            row_style = "background:#f9f9f9; color:#999;"
        elif count >= 50:
            row_style = "background:#fde8e8;"  # red tint
        else:
            row_style = "background:#fff8e1;"  # yellow tint
        html.append(f'<tr style="{row_style}">')
        html.append(f'<td style="{base_td}">{check}</td>')
        html.append(f'<td style="{base_td}">{sheet}</td>')
        html.append(f'<td style="{base_td}">{tab}</td>')
        html.append(f'<td style="{base_td} text-align:right; font-weight:bold;">{count}</td>')
        html.append("</tr>")

    html.append("</tbody></table>")
    return "\n".join(html)


def format_checklist_email(rows, subject_date):
    """
    Build the full HTML email body from the Check sheet rows.

    Sheet columns (0-indexed):
      A=0  count
      B=1  enabled flag (1=active, 0=inactive)
      C=2  row index
      D=3  check description
      E=4  sheet name
      F=5  tab name
    """
    active_flagged = []
    inactive_flagged = []

    for row in rows:
        try:
            count = int(str(row[0]).strip()) if row[0] else 0
        except ValueError:
            count = 0
        if count == 0:
            continue

        try:
            enabled = int(str(row[1]).strip()) if len(row) > 1 and row[1] else 0
        except ValueError:
            enabled = 0

        check = row[3] if len(row) > 3 else ""
        sheet = row[4] if len(row) > 4 else ""
        tab   = row[5] if len(row) > 5 else ""

        entry = (check, sheet, tab, count)
        if enabled:
            active_flagged.append(entry)
        else:
            inactive_flagged.append(entry)

    # Sort each section by count descending
    active_flagged.sort(key=lambda x: x[3], reverse=True)
    inactive_flagged.sort(key=lambda x: x[3], reverse=True)

    html = []
    html.append(f'<p style="font-family:Arial,Helvetica,sans-serif; font-size:14px;">'
                f'<b>ALGO Checklist requiring attention</b><br>'
                f'Sheet: <b>{tab_name}</b> &nbsp;|&nbsp; Date: {subject_date}<br>'
                f'<a href="{CHECKLIST_URL}">View Checklist Sheet &rarr;</a>'
                f'</p>')

    if active_flagged:
        html.append('<p style="font-family:Arial,Helvetica,sans-serif; font-size:13px; margin-bottom:4px;"><b>Active flagged checks</b></p>')
        html.append(_make_table(active_flagged, grey=False))
    else:
        html.append('<p style="font-family:Arial,Helvetica,sans-serif; color:#555;">No active checks flagged.</p>')

    if inactive_flagged:
        html.append('<p style="font-family:Arial,Helvetica,sans-serif; font-size:13px; margin-bottom:4px; color:#999;"><b>Inactive checks with data</b></p>')
        html.append(_make_table(inactive_flagged, grey=True))

    return "\n".join(html), bool(active_flagged)

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
    p = argparse.ArgumentParser(description="Send ALGO TICKERS email")
    p.add_argument("--emails", required=True, help="Comma-separated list of recipient emails")
    return p.parse_args()

def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

    args = parse_args()
    recipients = [e.strip() for e in args.emails.split(",") if e.strip()]

    sheet_id = resolve_sheet_id(ref_sheets)
    rows = read_sheet(sheet_id, tab_name, SERVICE_CREDS)
    data = rows[1:]  # skip header row

    subject_date = datetime.today().strftime("%d-%b-%Y")
    subject = f"ALGO CHECKLIST {subject_date}"
    logging.info("Prepared email subject: %s", subject)
    logging.info("Recipients: %s", ", ".join(recipients))
    logging.info("Total rows read from Check sheet: %d", len(data))

    html_body, has_active = format_checklist_email(data, subject_date)

    if not has_active:
        logging.info("No active flagged checks — skipping email.")
        return

    # try to load saved token first
    smtp_password = load_smtp_token()
    if smtp_password:
        logging.info("Loaded SMTP password from %s", SMTP_TOKEN_FILE)
    else:
        # not found — prompt user securely and persist
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


if __name__ == "__main__":
    try:
        main()
        raise SystemExit(0)
    except KeyboardInterrupt:
        logging.warning("Interrupted by user.")
        raise SystemExit(130)
