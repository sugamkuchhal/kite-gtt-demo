#!/usr/bin/env python3
import argparse
import logging
import sys
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib
import getpass
import json
import os

from algo_sheets_lookup import get_sheet_id
from google_sheets_utils import DEFAULT_READONLY_SCOPES, get_gsheet_client, open_worksheet
from runtime_paths import get_creds_path, get_smtp_token_path

SMTP_TOKEN_FILE = str(get_smtp_token_path())

# ==========================
# Config (constants)
# ==========================
ALGO_NAME = "GTT_MASTER"
TAB_NAME = "Action_Mailing_List"
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
    client = get_gsheet_client(scopes=DEFAULT_READONLY_SCOPES, creds_path=service_creds)
    ws = open_worksheet(client, tab_name, spreadsheet_id=sheet_id)
    rows = ws.get_all_values()
    logging.info("Read %d total rows (including header/empty rows) from tab '%s'.", len(rows), tab_name)
    return rows

def format_html_table(rows):
    # Columns A,B,D,O -> TICKER, TYPE, GTT_PRICE, ACTION
    header = ["TICKER", "TYPE", "GTT_PRICE", "ACTION"]

    color_map = {
        "insert": "#d4f8d4",  # light green
        "update": "#fffacd",  # light yellow
        "delete": "#f8d4d4",  # light red
    }

    html = []
    html.append('<table style="border-collapse:collapse; width:100%; font-family: Arial, Helvetica, sans-serif;">')
    html.append("<thead><tr>")
    for h in header:
        html.append(f'<th style="border:1px solid #ddd; padding:8px; text-align:left; background:#f2f2f2;">{h}</th>')
    html.append("</tr></thead><tbody>")

    for row in rows:
        ticker, type_, gtt_price, action = row
        bg = ""
        for key, color in color_map.items():
            if key in action.lower():
                bg = f' background:{color};'
                break
        html.append("<tr>")
        html.append(f'<td style="border:1px solid #ddd; padding:8px; text-align:left;">{ticker}</td>')
        html.append(f'<td style="border:1px solid #ddd; padding:8px; text-align:left;">{type_}</td>')
        html.append(f'<td style="border:1px solid #ddd; padding:8px; text-align:right;">{gtt_price}</td>')
        html.append(f'<td style="border:1px solid #ddd; padding:8px; text-align:left;{bg}">{action}</td>')
        html.append("</tr>")

    html.append("</tbody></table>")
    return "\n".join(html)

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

    rows = read_sheet(get_sheet_id(ALGO_NAME), TAB_NAME, SERVICE_CREDS)
    header, data = rows[0], rows[1:]

    # Columns: A,B,D,O
    filtered = []
    for r in data:
        if len(r) >= 15 and r[14]:  # ACTION column (O = index 14)
            filtered.append([r[0], r[1], r[3], r[14]])

    logging.info("Filtered -> %d rows with non-empty ACTION.", len(filtered))

    subject_date = datetime.today().strftime("%d-%b-%Y")
    subject = f"ALGO TICKERS {subject_date}"
    logging.info("Prepared email subject: %s", subject)
    logging.info("Recipients: %s", ", ".join(recipients))
    logging.info("Rows included: %d", len(filtered))

    html_body = (
        f'<p>ALGO tickers requiring attention (sheet: <b>{TAB_NAME}</b>, date: {subject_date})</p>'
        + format_html_table(filtered)
    )

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
    main()
