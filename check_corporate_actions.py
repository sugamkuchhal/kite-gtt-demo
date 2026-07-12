#!/usr/bin/env python3
"""
check_corporate_actions.py

One-off checker: for each (date, ticker) pair below, query NSE for
corporate actions with ex-date within +/- WINDOW_DAYS and print them.
Console output only; nothing written anywhere.

Run: python3 check_corporate_actions.py
"""
import logging
import random
import time
import urllib.parse
from datetime import datetime, timedelta

import requests

import atexit
from script_logger import log_start, log_end

_RUN_CTX = log_start("check_corporate_actions")
atexit.register(log_end, _RUN_CTX)

# (date the alert fired, ticker)
CHECKS = [
    ("21-Nov-2024", "NSE:ADANIENT"),
    ("29-Nov-2024", "NSE:ADANIGREEN"),
    ("24-Jan-2025", "NSE:CYIENT"),
    ("27-Feb-2025", "NSE:KEI"),
    ("11-Mar-2025", "NSE:INDUSINDBK"),
    ("24-Jul-2025", "NSE:IEX"),
    ("04-Sep-2025", "NSE:NETWEB"),
    ("24-Sep-2025", "NSE:TATAINVEST"),
    ("02-Feb-2026", "NSE:SILVERBEES"),
    ("10-Apr-2026", "NSE:ABDL"),
    ("10-Apr-2026", "NSE:COHANCE"),
    ("10-Apr-2026", "NSE:NIACL"),
    ("10-Apr-2026", "NSE:OLAELEC"),
]
WINDOW_DAYS = 5

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


def normalize(t):
    return t.strip().upper().replace("NSE:", "")


def fetch_symbol_actions(session, symbol, from_date, to_date):
    params = {
        "index": "equities",
        "symbol": symbol,
        "from_date": from_date.strftime("%d-%m-%Y"),
        "to_date": to_date.strftime("%d-%m-%Y"),
    }
    url = NSE_CA_API + "?" + urllib.parse.urlencode(params)
    last_err = None
    for attempt in range(1, FETCH_RETRIES + 1):
        try:
            resp = session.get(url, timeout=30)
            resp.raise_for_status()
            payload = resp.json()
            data = payload.get("data") if isinstance(payload, dict) else payload
            return data or []
        except Exception as e:
            last_err = e
            if attempt < FETCH_RETRIES:
                time.sleep(5 * attempt + random.random() * 2)
                # re-warm cookies
                try:
                    session.get(NSE_CA_PAGE, timeout=15)
                except Exception:
                    pass
    raise RuntimeError(f"fetch failed for {symbol}: {last_err}")


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
    session = requests.Session()
    session.headers.update(NSE_HEADERS)
    session.get(NSE_CA_PAGE, timeout=15)
    time.sleep(1.0 + random.random())

    print(f"\n{'Alert Date':<12} {'Symbol':<12} Result (ex-dates within +/-{WINDOW_DAYS} days)")
    print("-" * 90)
    for date_str, raw_ticker in CHECKS:
        symbol = normalize(raw_ticker)
        alert_date = datetime.strptime(date_str, "%d-%b-%Y").date()
        frm = alert_date - timedelta(days=WINDOW_DAYS)
        to = alert_date + timedelta(days=WINDOW_DAYS)
        try:
            rows = fetch_symbol_actions(session, symbol, frm, to)
        except Exception as e:
            print(f"{date_str:<12} {symbol:<12} FETCH FAILED: {e}")
            continue
        if not rows:
            print(f"{date_str:<12} {symbol:<12} no corporate action in window "
                  f"-> price move was news/results, not a CA")
        else:
            for r in rows:
                subj = str(r.get("subject", "")).strip()
                exd = str(r.get("exDate", "")).strip()
                marker = "  <== ON ALERT DATE" if exd == date_str else ""
                print(f"{date_str:<12} {symbol:<12} {exd:<12} {subj}{marker}")
        time.sleep(1.5 + random.random())
    print()


if __name__ == "__main__":
    main()
