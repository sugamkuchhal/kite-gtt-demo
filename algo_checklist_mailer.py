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
import subprocess
import time
import urllib.request
import urllib.parse
from html import escape as _esc

from runtime_paths import get_creds_path, get_smtp_token_path, get_telegram_token_path, repo_root, SMTP_FROM, SMTP_USER, SMTP_SERVER, SMTP_PORT, TELEGRAM_CHAT_ID
from ref_sheets_utils import resolve_sheet_id
from remover_old_tickers import run_removals
from remover_profitable_sip_reg import run_sip_reg
from remover_delisted import run_delisted

import atexit
from script_logger import log_start, log_end

_RUN_CTX = log_start("algo_checklist_mailer")
atexit.register(log_end, _RUN_CTX)
SMTP_TOKEN_FILE = str(get_smtp_token_path())

# ==========================
# Config (constants)
# ==========================
ref_sheets = "TICKER"
tab_name = "Checklist"
CHECKLIST_URL = "https://docs.google.com/spreadsheets/d/143py3t5oTsz0gAfp8VpSJlpR5VS8Z4tfl067pMtW1EE/edit?gid=844019911"
SERVICE_CREDS = str(get_creds_path())

# Healing framework: each healer has a numeric trigger cell ("signal")
# maintained by sheet formulas. When any signal > 0, the mailer runs the
# triggered healers, waits for formulas to heal, re-reads the checklist,
# and sends a single combined before/healing/after email.
REMOVALS_TAB = "Master_Live"
REMOVALS_SIGNAL_CELL = "H2"          # count of tickers pending removal
NORMALIZE_SIGNAL_CELL = "D1"         # on the Checklist tab; SUMPRODUCT trigger
SIP_SIGNAL_REF = "KWK"               # profitable SIP REG signal lives on the KWK sheet
SIP_SIGNAL_TAB = "OLD_SIP_REG_List"
SIP_SIGNAL_CELL = "Q1"
NORMALIZE_SCRIPT = "combined_normalize_run.sh"
DELISTED_SIGNAL_REF  = "PORTFOLIO"
DELISTED_SIGNAL_TAB  = "DELISTED"
DELISTED_SIGNAL_CELL = "A1"
HEAL_WAIT_SECS = 60
HEALER_LOG_TAIL = 10                 # log lines per healer shown in comms


# Email / SMTP settings


# Telegram settings

TELEGRAM_TOKEN_FILE = str(get_telegram_token_path())

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

def format_telegram_message(active_flagged, inactive_flagged, subject_date):
    """Build a Telegram-compatible HTML message (bold, italic, links only)."""
    lines = []
    lines.append(f"<b>ALGO CHECKLIST {subject_date}</b>")
    lines.append(f'<a href="{CHECKLIST_URL}">View Checklist Sheet →</a>')
    lines.append("")

    if active_flagged:
        lines.append("<b>Active flagged checks:</b>")
        for check, sheet, tab, count in active_flagged:
            lines.append(f"• {check} — {tab} — <b>{count}</b>")
    else:
        lines.append("No active checks flagged.")

    if inactive_flagged:
        lines.append("")
        lines.append("<i>Inactive checks with data:</i>")
        for check, sheet, tab, count in inactive_flagged:
            lines.append(f"• {check} — {tab} — {count}")

    return "\n".join(lines)

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

    return "\n".join(html), bool(active_flagged), active_flagged, inactive_flagged

# ==========================
# Healing framework
#
# Each healer is a pure actuator with a numeric trigger cell on the
# sheet ("signal"). When any signal > 0, the mailer runs the triggered
# healers in registry order, waits for the sheet formulas to heal,
# re-reads the checklist, and sends ONE combined email/Telegram:
# checklist before -> one section per healer (its log lines) ->
# checklist after. The combined message always sends, even on failure.
# Only the last HEALER_LOG_TAIL log lines per healer go to comms; full
# logs remain in the console (GitHub Actions log).
# ==========================

def read_signal_cell(sheet_id, tab, cell, service_creds):
    """Read a numeric healing-trigger cell. Non-numeric/blank -> 0."""
    scope = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
    creds = Credentials.from_service_account_file(service_creds, scopes=scope)
    client = gspread.authorize(creds)
    ws = client.open_by_key(sheet_id).worksheet(tab)
    raw = ws.acell(cell).value
    try:
        val = float(str(raw).replace(",", "").strip())
    except (TypeError, ValueError):
        val = 0.0
    logging.info("Healing signal %s!%s = %r -> %s", tab, cell, raw, val)
    return val


class _ListLogHandler(logging.Handler):
    """Captures log records into a list (used to collect healer logs)."""
    def __init__(self, sink):
        super().__init__()
        self.sink = sink

    def emit(self, record):
        try:
            self.sink.append(self.format(record))
        except Exception:
            pass


def run_removals_healer():
    """Run the removals processor, capturing its log output as the report."""
    lines = []
    handler = _ListLogHandler(lines)
    handler.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
    root = logging.getLogger()
    root.addHandler(handler)
    try:
        result = run_removals()
    finally:
        root.removeHandler(handler)
    error = bool(result.get("error")) or any(
        t.get("error") for t in result.get("tabs", [])
    ) or any(
        f.get("error") for f in result.get("nse_lists", [])
    ) or any(
        t.get("error") for t in result.get("ticker_tabs", [])
    ) or bool(result.get("tick_size_error"))
    return lines, error


def run_sip_reg_healer():
    """Run the profitable SIP REG remover, capturing its log output."""
    lines = []
    handler = _ListLogHandler(lines)
    handler.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
    root = logging.getLogger()
    root.addHandler(handler)
    try:
        result = run_sip_reg()
    finally:
        root.removeHandler(handler)
    return lines, bool(result.get("error"))


def run_normalize_healer():
    """Run combined_normalize_run.sh, streaming output to console and
    capturing it as the report. Non-zero exit code -> error."""
    lines = []
    logging.info("Launching %s ...", NORMALIZE_SCRIPT)
    proc = subprocess.Popen(
        ["bash", NORMALIZE_SCRIPT],
        cwd=str(repo_root()),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    for line in proc.stdout:
        line = line.rstrip("\n")
        print(line, flush=True)  # live in the Actions console
        lines.append(line)
    rc = proc.wait()
    lines.append(f"exit code: {rc}")
    logging.info("%s finished with exit code %d.", NORMALIZE_SCRIPT, rc)
    return lines, rc != 0


def run_delisted_healer():
    """Run the delisted ticker mover, capturing its log output."""
    lines = []
    handler = _ListLogHandler(lines)
    handler.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
    root = logging.getLogger()
    root.addHandler(handler)
    try:
        result = run_delisted()
    finally:
        root.removeHandler(handler)
    return lines, bool(result.get("error"))


HEALERS = [
    {
        "name": "Removals",
        "detect": lambda sheet_id: read_signal_cell(
            sheet_id, REMOVALS_TAB, REMOVALS_SIGNAL_CELL, SERVICE_CREDS) > 0,
        "run": run_removals_healer,
    },
    {
        "name": "Profitable SIP REG",
        "detect": lambda sheet_id: read_signal_cell(
            resolve_sheet_id(SIP_SIGNAL_REF), SIP_SIGNAL_TAB,
            SIP_SIGNAL_CELL, SERVICE_CREDS) > 0,
        "run": run_sip_reg_healer,
    },
    {
        "name": "Normalize",
        "detect": lambda sheet_id: read_signal_cell(
            sheet_id, tab_name, NORMALIZE_SIGNAL_CELL, SERVICE_CREDS) > 0,
        "run": run_normalize_healer,
    },
    {
        "name": "Order Tickers w/o Category",
        "detect": lambda sheet_id: read_signal_cell(
            resolve_sheet_id(DELISTED_SIGNAL_REF), DELISTED_SIGNAL_TAB,
            DELISTED_SIGNAL_CELL, SERVICE_CREDS) > 0,
        "run": run_delisted_healer,
    },
]


def _section_title(text):
    return (f'<p style="font-family:Arial,Helvetica,sans-serif; font-size:14px; '
            f'margin:18px 0 6px 0; border-bottom:2px solid #444;"><b>{text}</b></p>')


def format_healer_html(result):
    """Render one healer result {name, lines, error} as an HTML fragment."""
    tail = result["lines"][-HEALER_LOG_TAIL:]
    hidden = len(result["lines"]) - len(tail)
    if result["error"]:
        status = '<span style="color:#c00; font-weight:bold;">FAILED</span>'
    else:
        status = '<span style="color:#2e7d32; font-weight:bold;">OK</span>'

    html = []
    html.append(f'<p style="font-family:Arial,Helvetica,sans-serif; font-size:13px;">Status: {status}</p>')
    if hidden > 0:
        html.append(f'<p style="font-family:Arial,Helvetica,sans-serif; font-size:11px; color:#999;">'
                    f'... {hidden} earlier log line(s) omitted — full logs in the run console.</p>')
    body = _esc("\n".join(tail)) if tail else "(no log output)"
    html.append(f'<pre style="background:#f6f6f6; border:1px solid #ddd; padding:10px; '
                f'font-size:12px; white-space:pre-wrap;">{body}</pre>')
    return "\n".join(html)


def format_combined_email(html_before, healer_results, html_after):
    """Single email body: checklist before, healer sections, checklist after."""
    html = []
    html.append(_section_title("1. Checklist — before healing"))
    html.append(html_before)
    n = 2
    for result in healer_results:
        html.append(_section_title(f"{n}. Healing — {result['name']}"))
        html.append(format_healer_html(result))
        n += 1
    html.append(_section_title(f"{n}. Checklist — after healing"))
    html.append(html_after)
    return "\n".join(html)


def format_combined_telegram(before, healer_results, after, subject_date):
    """
    Single Telegram message: checklist before, healer reports, checklist after.
    `before`/`after` are (active_flagged, inactive_flagged) tuples.
    """
    def _checklist_lines(label, active):
        out = ["", f"<b>{label}:</b>"]
        if active:
            for check, sheet, tab, count in active:
                out.append(f"• {check} — {tab} — <b>{count}</b>")
        else:
            out.append("No active checks flagged.")
        return out

    lines = []
    lines.append(f"<b>ALGO CHECKLIST {subject_date}</b>")
    lines.append(f'<a href="{CHECKLIST_URL}">View Checklist Sheet →</a>')
    lines.extend(_checklist_lines("Before healing", before[0]))
    for result in healer_results:
        status = "⚠️ FAILED" if result["error"] else "OK"
        lines.append("")
        lines.append(f"<b>Healing — {result['name']}:</b> {status}")
        for l in result["lines"][-HEALER_LOG_TAIL:]:
            lines.append(_esc(l))
    lines.extend(_checklist_lines("After healing", after[0]))
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
    p = argparse.ArgumentParser(description="Send ALGO TICKERS email")
    p.add_argument("--emails", required=True, help="Comma-separated list of recipient emails")
    return p.parse_args()


# ==========================
# GID + URL population
# ==========================
COL_F = 5   # Spreadsheet ID (0-indexed)
COL_G = 6   # Tab name
COL_I = 8   # GID — populated here
COL_J = 9   # URL — populated here
GID_DATA_START_ROW = 2

def populate_gids_and_urls(ws):
    """
    Step 0 — run before reading the checklist.
    For each row where column F (spreadsheet ID) and G (tab name) are filled
    but column I (GID) or J (URL) are empty, fetch the GID and write both
    I and J in a single batch update.
    Spreadsheets are opened once and cached.
    """
    scope = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file(SERVICE_CREDS, scopes=scope)
    client = gspread.authorize(creds)

    all_rows = ws.get_all_values()
    data_rows = all_rows[GID_DATA_START_ROW - 1:]

    cache = {}   # spreadsheet_id -> {tab_title: gid}
    updates = [] # gspread.Cell list

    for i, row in enumerate(data_rows):
        sheet_row = GID_DATA_START_ROW + i
        padded = row + [""] * max(0, COL_J + 1 - len(row))

        spreadsheet_id = padded[COL_F].strip()
        tab_name_cell  = padded[COL_G].strip()
        current_i      = padded[COL_I].strip()
        current_j      = padded[COL_J].strip()

        if not spreadsheet_id or not tab_name_cell:
            continue
        if current_i and current_j:
            continue  # both already filled

        if spreadsheet_id not in cache:
            logging.info("GID populate: opening spreadsheet %s", spreadsheet_id)
            try:
                wb = client.open_by_key(spreadsheet_id)
                cache[spreadsheet_id] = {s.title: s.id for s in wb.worksheets()}
            except Exception as e:
                logging.warning("GID populate: could not open %s: %s", spreadsheet_id, e)
                cache[spreadsheet_id] = {}

        gid = cache[spreadsheet_id].get(tab_name_cell)
        if gid is None:
            logging.warning("GID populate: tab '%s' not found in %s", tab_name_cell, spreadsheet_id)
            continue

        url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit#gid={gid}"
        updates.append(gspread.Cell(row=sheet_row, col=COL_I + 1, value=str(gid)))
        updates.append(gspread.Cell(row=sheet_row, col=COL_J + 1, value=url))
        logging.info("GID populate: row %d -> GID=%s", sheet_row, gid)

    if updates:
        ws.update_cells(updates, value_input_option="RAW")
        logging.info("GID populate: wrote %d cell(s).", len(updates))
    else:
        logging.info("GID populate: nothing to update.")

def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

    args = parse_args()
    recipients = [e.strip() for e in args.emails.split(",") if e.strip()]

    sheet_id = resolve_sheet_id(ref_sheets)

    # Step 0: populate GIDs and URLs in the Checklist tab before reading
    logging.info("Step 0: populating GIDs and URLs in Checklist tab...")
    _scope = ["https://www.googleapis.com/auth/spreadsheets"]
    _creds = Credentials.from_service_account_file(SERVICE_CREDS, scopes=_scope)
    _client = gspread.authorize(_creds)
    _ws = _client.open_by_key(sheet_id).worksheet(tab_name)
    populate_gids_and_urls(_ws)

    rows = read_sheet(sheet_id, tab_name, SERVICE_CREDS)
    data = rows[1:]  # skip header row

    subject_date = datetime.today().strftime("%d-%b-%Y")
    subject = f"ALGO CHECKLIST {subject_date}"
    logging.info("Prepared email subject: %s", subject)
    logging.info("Recipients: %s", ", ".join(recipients))
    logging.info("Total rows read from Check sheet: %d", len(data))

    html_before, has_active, active_flagged, inactive_flagged = format_checklist_email(data, subject_date)

    # Healing runs up to MAX_HEAL_ROUNDS times. After each round the script
    # waits for sheet formulas to settle, then re-detects to catch any new
    # issues that surfaced. All rounds are labelled and reported in one email.
    MAX_HEAL_ROUNDS = 2
    all_healer_results = []
    any_healing_ran = False

    for round_num in range(1, MAX_HEAL_ROUNDS + 1):
        round_label = f"Round {round_num}"
        triggered = []
        for healer in HEALERS:
            try:
                if healer["detect"](sheet_id):
                    triggered.append(healer)
            except Exception as e:
                logging.exception(
                    "Healer '%s' detection failed (round %d): %s", healer["name"], round_num, e
                )
                all_healer_results.append({
                    "name": f"[{round_label}] {healer['name']}",
                    "lines": [f"Detection failed: {e}"],
                    "error": True,
                })
                any_healing_ran = True

        if not triggered:
            logging.info("%s: no healers triggered — stopping.", round_label)
            break

        any_healing_ran = True
        for healer in triggered:
            logging.info("%s: running '%s' ...", round_label, healer["name"])
            try:
                lines, error = healer["run"]()
            except Exception as e:
                logging.exception(
                    "Healer '%s' crashed (round %d): %s", healer["name"], round_num, e
                )
                lines, error = [f"Healer crashed: {e}"], True
            all_healer_results.append({
                "name": f"[{round_label}] {healer['name']}",
                "lines": lines,
                "error": error,
            })

        logging.info(
            "%s: waiting %d seconds for sheet formulas to settle...", round_label, HEAL_WAIT_SECS
        )
        time.sleep(HEAL_WAIT_SECS)

    if any_healing_ran:
        rows_after = read_sheet(sheet_id, tab_name, SERVICE_CREDS)
        html_after, _, active_after, inactive_after = format_checklist_email(rows_after[1:], subject_date)

        html_body = format_combined_email(html_before, all_healer_results, html_after)
        tg_text = format_combined_telegram(
            (active_flagged, inactive_flagged),
            all_healer_results,
            (active_after, inactive_after),
            subject_date,
        )
    else:
        if not has_active:
            logging.info("No active flagged checks — skipping email and Telegram.")
            return
        html_body = html_before
        tg_text = format_telegram_message(active_flagged, inactive_flagged, subject_date)

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
            SMTP_FROM, recipients, subject, html_body,
            SMTP_SERVER, SMTP_PORT, SMTP_USER, smtp_password
        )
        logging.info("Email sent successfully.")
    except Exception as e:
        logging.exception("Failed to send email: %s", e)
        sys.exit(1)

    # Telegram
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
