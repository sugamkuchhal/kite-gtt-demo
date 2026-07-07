#!/usr/bin/env python3
"""
dividend_ledger_backfill.py

ONE-TIME backfill of the Dividend_Ledger tab from trade history.

- Trades come from PORTFOLIO!ALL_ORDERS (TICKER, DATE, TYPE, UNITS).
- For each historical dividend (NSE corporate actions, fetched in ~90-day
  bulk chunks from the first trade date to today), the entitled quantity
  is reconstructed as: net units of trades dated STRICTLY BEFORE the
  ex-date (held at close of the prior day).
- Rows are appended to PORTFOLIO!Dividend_Ledger chronologically,
  deduped on (Symbol, Ex-Date) so they coexist with rows written by
  dividend_action_mailer.py. Locked Date is left blank for backfill;
  Remarks are prefixed "BACKFILL: ".

Reconstruction caveats are surfaced, not hidden:
- Splits/bonuses/demergers change demat quantity without an ALL_ORDERS
  trade. Dividend rows dated AFTER such an event for the same symbol
  get a "CHECK QTY" flag in Remarks.
- Sanity gate: reconstructed current quantity is compared against the
  Portfolio tab (col A ticker, col C qty). Mismatching symbols are
  reported and their rows flagged "QTY MISMATCH".

Usage:
  python3 dividend_ledger_backfill.py --dry-run   # print everything, write nothing
  python3 dividend_ledger_backfill.py             # append to the ledger
"""
import argparse
import json
import logging
import random
import re
import sys
import time
import urllib.parse
from collections import defaultdict
from datetime import date, datetime, timedelta

import requests
import gspread
from google.oauth2.service_account import Credentials

from runtime_paths import get_creds_path
from ref_sheets_utils import resolve_sheet_id

import atexit
from script_logger import log_start, log_end

_RUN_CTX = log_start("dividend_ledger_backfill")
atexit.register(log_end, _RUN_CTX)

SERVICE_CREDS = str(get_creds_path())

# ==========================
# Config (constants)
# ==========================
ref_sheets = "PORTFOLIO"
ORDERS_TAB = "ALL_ORDERS"
HOLDINGS_TAB = "Portfolio"          # col A = ticker, col C = qty (sanity gate)
LEDGER_TAB = "Dividend_Ledger"
LEDGER_HEADERS = ["Locked Date", "Ex-Date", "Symbol", "Qty",
                  "Div/Share", "Amount", "Remarks"]

CHUNK_DAYS = 90                     # NSE bulk fetch window size
CHUNK_PAUSE_SECS = 3                # polite pause between chunks

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
# Events that change demat quantity without an ALL_ORDERS trade.
QTY_EVENT_RE = re.compile(
    r"split|bonus|demerger|de-merger|consolidat|reduction|scheme of arrangement",
    re.IGNORECASE,
)
_AMT_PER_SHARE_RE = re.compile(
    r"(?:R(?:s|e)\.?\s*)?(\d+(?:\.\d+)?)\s*(?:/-)?\s*per\s*share",
    re.IGNORECASE,
)
_AMT_RS_RE = re.compile(r"R(?:s|e)\.?\s*(\d+(?:\.\d+)?)", re.IGNORECASE)
_AMT_PCT_RE = re.compile(r"(\d+(?:\.\d+)?)\s*%")

# ==========================
# Sheet helpers
# ==========================

def _gs_client():
    scope = ["https://spreadsheets.google.com/feeds",
             "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_file(SERVICE_CREDS, scopes=scope)
    return gspread.authorize(creds)


def normalize_symbol(raw):
    s = str(raw).strip().upper()
    if s.startswith("NSE:"):
        s = s[len("NSE:"):]
    for suffix in (".NS", ".BO"):
        if s.endswith(suffix):
            s = s[: -len(suffix)]
    return s.strip()


def parse_trade_date(raw):
    """Parse ALL_ORDERS dates like '19-Mar-2025' (tolerant of a few formats)."""
    raw = str(raw).strip()
    for fmt in ("%d-%b-%Y", "%d-%m-%Y", "%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(raw, fmt).date()
        except ValueError:
            continue
    return None


def read_trades(client, sheet_id):
    """
    Read ALL_ORDERS into {symbol: [(date, signed_units), ...]} sorted by date.
    BUY -> +units, SELL -> -units. Rows with bad date/units are counted
    and reported, not silently dropped.
    """
    ws = client.open_by_key(sheet_id).worksheet(ORDERS_TAB)
    records = ws.get_all_records()
    trades = defaultdict(list)
    bad_rows = 0
    first_date = None
    for rec in records:
        symbol = normalize_symbol(rec.get("TICKER", ""))
        d = parse_trade_date(rec.get("DATE", ""))
        ttype = str(rec.get("TYPE", "")).strip().upper()
        try:
            units = int(float(str(rec.get("UNITS", "")).replace(",", "").strip()))
        except (TypeError, ValueError):
            units = None
        if not symbol or d is None or units is None or ttype not in ("BUY", "SELL"):
            bad_rows += 1
            continue
        signed = units if ttype == "BUY" else -units
        trades[symbol].append((d, signed))
        if first_date is None or d < first_date:
            first_date = d
    for symbol in trades:
        trades[symbol].sort(key=lambda t: t[0])
    logging.info("Read %d trade rows across %d symbols (skipped %d bad rows). "
                 "First trade: %s",
                 sum(len(v) for v in trades.values()), len(trades), bad_rows,
                 first_date.strftime("%d-%b-%Y") if first_date else "n/a")
    return trades, first_date, bad_rows


def qty_on_ex_date(trade_list, ex_date):
    """Net units from trades dated strictly before ex_date."""
    return sum(units for d, units in trade_list if d < ex_date)


def read_portfolio_qty(client, sheet_id):
    """Current quantities from the Portfolio tab (sanity gate)."""
    ws = client.open_by_key(sheet_id).worksheet(HOLDINGS_TAB)
    rows = ws.get_all_values()
    qty = {}
    for row in rows[1:]:
        symbol = normalize_symbol(row[0]) if len(row) > 0 else ""
        if not symbol:
            continue
        raw = str(row[2]).replace(",", "").strip() if len(row) > 2 else ""
        try:
            qty[symbol] = qty.get(symbol, 0) + int(float(raw))
        except (TypeError, ValueError):
            continue
    return qty

# ==========================
# NSE fetch (chunked bulk)
# ==========================

def fetch_chunk(session, from_date, to_date):
    params = {
        "index": "equities",
        "from_date": from_date.strftime("%d-%m-%Y"),
        "to_date": to_date.strftime("%d-%m-%Y"),
    }
    url = NSE_CA_API + "?" + urllib.parse.urlencode(params)
    resp = session.get(url, timeout=30)
    resp.raise_for_status()
    payload = resp.json()
    data = payload.get("data") if isinstance(payload, dict) else payload
    if data is None:
        data = []
    if not isinstance(data, list):
        raise ValueError(f"Unexpected NSE payload type: {type(data)}")
    return data


def fetch_all_history(from_date, to_date):
    """
    Fetch all equity corporate actions in [from_date, to_date] in
    CHUNK_DAYS windows. Each chunk retries with a fresh warmed-up
    session; a chunk that fails all retries aborts the run (a ledger
    with silent holes is worse than no ledger).
    """
    all_rows = []
    chunk_start = from_date
    n_chunks = 0
    while chunk_start <= to_date:
        chunk_end = min(chunk_start + timedelta(days=CHUNK_DAYS - 1), to_date)
        n_chunks += 1
        last_err = None
        for attempt in range(1, FETCH_RETRIES + 1):
            try:
                session = requests.Session()
                session.headers.update(NSE_HEADERS)
                session.get(NSE_CA_PAGE, timeout=15)
                time.sleep(1.0 + random.random())
                rows = fetch_chunk(session, chunk_start, chunk_end)
                logging.info("Chunk %d: %s -> %s: %d rows.",
                             n_chunks, chunk_start.strftime("%d-%b-%Y"),
                             chunk_end.strftime("%d-%b-%Y"), len(rows))
                all_rows.extend(rows)
                last_err = None
                break
            except Exception as e:
                last_err = e
                logging.warning("Chunk %d attempt %d/%d failed: %s",
                                n_chunks, attempt, FETCH_RETRIES, e)
                if attempt < FETCH_RETRIES:
                    time.sleep(FETCH_BACKOFF_BASE_SECS * attempt + random.random() * 2)
        if last_err is not None:
            raise RuntimeError(
                f"Chunk {chunk_start} -> {chunk_end} failed after "
                f"{FETCH_RETRIES} attempts: {last_err}")
        chunk_start = chunk_end + timedelta(days=1)
        if chunk_start <= to_date:
            time.sleep(CHUNK_PAUSE_SECS + random.random())
    logging.info("Fetched %d total rows across %d chunks.", len(all_rows), n_chunks)
    return all_rows

# ==========================
# Parsing / classification
# ==========================

def parse_nse_date(raw):
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
    Total per-share dividend from NSE free text. Sums multiple amounts.
    Handles rupee amounts and percent-of-face-value amounts
    (e.g. 'Dividend - 50%' with faceVal 2 -> Rs 1). Returns float or None.
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


def build_ledger_rows(raw_rows, trades, portfolio_qty, today):
    """
    Returns (ledger_entries, qty_event_dates, mismatches).
    ledger_entries: dicts sorted by ex-date ascending.
    """
    # First pass: earliest quantity-changing event per held-ever symbol.
    qty_event = {}
    for row in raw_rows:
        if not isinstance(row, dict):
            continue
        symbol = str(row.get("symbol", "")).strip().upper()
        if symbol not in trades:
            continue
        subject = str(row.get("subject", "") or row.get("purpose", "")).strip()
        if not QTY_EVENT_RE.search(subject):
            continue
        ex_date = parse_nse_date(row.get("exDate") or row.get("exdate"))
        if ex_date is None:
            continue
        # Only matters if we actually held shares when it happened.
        if qty_on_ex_date(trades[symbol], ex_date) <= 0:
            continue
        if symbol not in qty_event or ex_date < qty_event[symbol]:
            qty_event[symbol] = ex_date

    # Sanity gate: reconstructed current qty vs Portfolio tab.
    mismatches = {}
    for symbol, tlist in trades.items():
        reconstructed = sum(u for _, u in tlist)
        actual = portfolio_qty.get(symbol, 0)
        if reconstructed != actual:
            mismatches[symbol] = (reconstructed, actual)

    # Second pass: dividends.
    entries = []
    seen = set()
    for row in raw_rows:
        if not isinstance(row, dict):
            continue
        symbol = str(row.get("symbol", "")).strip().upper()
        if symbol not in trades:
            continue
        subject = str(row.get("subject", "") or row.get("purpose", "")).strip()
        if not DIVIDEND_RE.search(subject):
            continue
        ex_date = parse_nse_date(row.get("exDate") or row.get("exdate"))
        if ex_date is None or ex_date > today:
            continue
        key = (symbol, subject.lower(), ex_date)
        if key in seen:
            continue
        seen.add(key)
        qty = qty_on_ex_date(trades[symbol], ex_date)
        if qty <= 0:
            continue
        per_share = parse_dividend_per_share(subject, parse_face_value(row))
        amount = round(per_share * qty, 2) if per_share is not None else None

        flags = []
        if symbol in qty_event and ex_date >= qty_event[symbol]:
            flags.append(f"CHECK QTY (split/bonus ex "
                         f"{qty_event[symbol].strftime('%d-%b-%Y')})")
        if symbol in mismatches:
            r, a = mismatches[symbol]
            flags.append(f"QTY MISMATCH (trades={r}, portfolio={a})")
        if per_share is None:
            flags.append("AMOUNT UNPARSED")
        remarks = "BACKFILL: " + subject
        if flags:
            remarks = "BACKFILL [" + "; ".join(flags) + "]: " + subject

        entries.append({
            "symbol": symbol,
            "subject": subject,
            "ex_date": ex_date,
            "qty": qty,
            "per_share": per_share,
            "amount": amount,
            "remarks": remarks,
            "flagged": bool(flags),
        })

    entries.sort(key=lambda e: (e["ex_date"], e["symbol"]))
    return entries, qty_event, mismatches

# ==========================
# Ledger write
# ==========================

def append_to_ledger(client, sheet_id, entries):
    """Append entries (deduped on Symbol+Ex-Date vs existing rows)."""
    ws = client.open_by_key(sheet_id).worksheet(LEDGER_TAB)
    existing = ws.get_all_values()
    if not existing:
        ws.update(values=[LEDGER_HEADERS], range_name="A1")
        existing = [LEDGER_HEADERS]
        logging.info("Wrote headers to empty %s tab.", LEDGER_TAB)

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
            "",  # Locked Date blank for backfill
            ex_label,
            e["symbol"],
            e["qty"],
            e["per_share"] if e["per_share"] is not None else "",
            e["amount"] if e["amount"] is not None else "",
            e["remarks"],
        ])

    if new_rows:
        ws.append_rows(new_rows, value_input_option="USER_ENTERED")
    logging.info("Ledger: appended %d row(s), skipped %d duplicate(s).",
                 len(new_rows), skipped)
    return len(new_rows), skipped

# ==========================
# Main
# ==========================

def _fmt_inr(value):
    if value is None:
        return "?"
    neg = value < 0
    value = abs(value)
    whole = int(value)
    frac = f"{value - whole:.2f}"[1:]
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


def parse_args():
    p = argparse.ArgumentParser(description="One-time dividend ledger backfill from trade history")
    p.add_argument("--dry-run", action="store_true",
                   help="Print rows and reports; write nothing to the sheet")
    return p.parse_args()


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
    args = parse_args()

    today = date.today()
    sheet_id = resolve_sheet_id(ref_sheets)
    client = _gs_client()

    trades, first_date, bad_rows = read_trades(client, sheet_id)
    if not trades or first_date is None:
        logging.error("No usable trades in %s — aborting.", ORDERS_TAB)
        sys.exit(1)

    portfolio_qty = read_portfolio_qty(client, sheet_id)

    raw_rows = fetch_all_history(first_date, today)
    entries, qty_event, mismatches = build_ledger_rows(
        raw_rows, trades, portfolio_qty, today)

    total = round(sum(e["amount"] for e in entries if e["amount"] is not None), 2)
    n_flagged = sum(1 for e in entries if e["flagged"])

    print("\n===== BACKFILL SUMMARY =====")
    print(f"Trade rows skipped (bad data): {bad_rows}")
    print(f"Dividend rows reconstructed:   {len(entries)} "
          f"({n_flagged} flagged for review)")
    print(f"Total (unflagged + flagged parseable): Rs {_fmt_inr(total)}")

    if qty_event:
        print("\nSymbols with split/bonus/demerger while held "
              "(rows on/after these dates are flagged CHECK QTY):")
        for sym, d in sorted(qty_event.items()):
            print(f"  {sym}: {d.strftime('%d-%b-%Y')}")

    if mismatches:
        print("\nQTY MISMATCH vs Portfolio tab (trade history incomplete "
              "or split/bonus not in trades):")
        for sym, (r, a) in sorted(mismatches.items()):
            print(f"  {sym}: reconstructed={r}, portfolio={a}")
    else:
        print("\nSanity gate: reconstructed quantities match the Portfolio tab for all symbols.")

    print("\n===== LEDGER ROWS =====")
    for e in entries:
        print([e["ex_date"].strftime("%d-%b-%Y"), e["symbol"], e["qty"],
               e["per_share"], e["amount"], e["remarks"]])

    if args.dry_run:
        print("\nDRY RUN — nothing written to the sheet.")
        return

    appended, skipped = append_to_ledger(client, sheet_id, entries)
    print(f"\nDone: {appended} row(s) appended to {LEDGER_TAB}, "
          f"{skipped} duplicate(s) skipped.")


if __name__ == "__main__":
    try:
        main()
        raise SystemExit(0)
    except KeyboardInterrupt:
        logging.warning("Interrupted by user.")
        raise SystemExit(130)
