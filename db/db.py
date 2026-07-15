"""
db.py — shared SQLite access module.

Usage:
    from db.db import get_conn

    with get_conn() as conn:
        conn.execute("SELECT * FROM market_data WHERE symbol = 'NSE:RELIANCE'")
"""

import sqlite3
import os
from pathlib import Path
from datetime import datetime, timezone


# ── Path resolution ───────────────────────────────────────────────────────────

def get_db_path() -> Path:
    """
    Returns the path to trading.db.
    - GitHub Actions : $GITHUB_WORKSPACE/db/trading.db
    - Local          : <repo_root>/db/trading.db
    """
    workspace = os.environ.get("GITHUB_WORKSPACE")
    if workspace:
        db_dir = Path(workspace) / "db"
    else:
        db_dir = Path(__file__).resolve().parent

    db_dir.mkdir(parents=True, exist_ok=True)
    return db_dir / "trading.db"


def get_conn() -> sqlite3.Connection:
    """
    Returns a sqlite3 connection with:
    - WAL mode (safe for concurrent readers)
    - Row factory set to sqlite3.Row (column-name access)
    - Foreign keys enabled
    """
    conn = sqlite3.connect(get_db_path())
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


# ── Schema ────────────────────────────────────────────────────────────────────

SCHEMA = """

-- ── Market Data ──────────────────────────────────────────────────────────────
-- Daily OHLCV for all tickers (stocks + ETFs + delisted).
-- Backfilled from 2020-01-01 via db/backfill_market_data.py.
-- Updated daily via db/fetch_market_data.py (upsert).
-- Scale: ~1,250,000 rows (1000 tickers × 250 days × 5 years)

CREATE TABLE IF NOT EXISTS market_data (
    date        TEXT    NOT NULL,           -- YYYY-MM-DD
    symbol      TEXT    NOT NULL,           -- NSE:RELIANCE
    close       REAL,
    low         REAL,
    high        REAL,
    volume      REAL,                       -- Volume in Cr. (volume × close / 1e7)
    type        TEXT,                       -- STOCK | ETF
    updated_at  TEXT    NOT NULL,           -- ISO8601 UTC

    PRIMARY KEY (date, symbol)
);

CREATE INDEX IF NOT EXISTS idx_market_data_symbol_date
    ON market_data (symbol, date);

CREATE INDEX IF NOT EXISTS idx_market_data_date
    ON market_data (date);

-- ── GTTs ─────────────────────────────────────────────────────────────────────
-- One row per active GTT order fetched from kite.get_gtts().
-- Fully replaced on each fetch (DELETE + INSERT).

CREATE TABLE IF NOT EXISTS gtts (
    gtt_id              INTEGER PRIMARY KEY,
    symbol              TEXT    NOT NULL,
    exchange            TEXT    NOT NULL,
    trigger_type        TEXT,
    trigger_value       REAL,
    last_price          REAL,
    order_price         REAL,
    order_qty           INTEGER,
    order_type          TEXT,
    product             TEXT,
    transaction_type    TEXT,
    status              TEXT,
    fetched_at          TEXT    NOT NULL
);

-- ── Orders ───────────────────────────────────────────────────────────────────
-- One row per order fetched from kite.orders() for the current trading day.
-- Fully replaced on each fetch (DELETE + INSERT).

CREATE TABLE IF NOT EXISTS orders (
    order_id            TEXT    PRIMARY KEY,
    exchange_order_id   TEXT,
    instrument_token    INTEGER,
    tradingsymbol       TEXT    NOT NULL,
    transaction_type    TEXT,
    order_type          TEXT,
    product             TEXT,
    quantity            INTEGER,
    filled_qty          INTEGER,
    price               REAL,
    average_price       REAL,
    status              TEXT,
    order_timestamp     TEXT,
    fetched_at          TEXT    NOT NULL
);

-- ── Holdings ─────────────────────────────────────────────────────────────────
-- One row per holding fetched from kite.holdings().
-- Fully replaced on each fetch (DELETE + INSERT).

CREATE TABLE IF NOT EXISTS holdings (
    tradingsymbol       TEXT    PRIMARY KEY,
    isin                TEXT,
    quantity            INTEGER,
    used_quantity       INTEGER,
    t1_quantity         INTEGER,
    average_price       REAL,
    last_price          REAL,
    pnl                 REAL,
    product             TEXT,
    exchange            TEXT,
    fetched_at          TEXT    NOT NULL
);

-- ── Corporate Actions ───────────────────────────────────────────────────────
-- All corporate actions fetched from NSE API.
-- Scanned daily: today -7 to today +30.
-- Emailed: today -7 to today +7 only.
-- Primary key (symbol, subject, ex_date) deduplicates across daily runs.

CREATE TABLE IF NOT EXISTS corporate_actions (
    symbol          TEXT    NOT NULL,   -- RELIANCE (no NSE: prefix)
    company         TEXT,               -- Reliance Industries Ltd
    subject         TEXT    NOT NULL,   -- Bonus 1:1
    ex_date         TEXT    NOT NULL,   -- YYYY-MM-DD
    record_date     TEXT,               -- YYYY-MM-DD or raw string
    critical        INTEGER NOT NULL DEFAULT 0,  -- 1 if GTT-critical
    fetched_at      TEXT    NOT NULL,   -- ISO8601 UTC

    PRIMARY KEY (symbol, subject, ex_date)
);

CREATE INDEX IF NOT EXISTS idx_corporate_actions_ex_date
    ON corporate_actions (ex_date);

CREATE INDEX IF NOT EXISTS idx_corporate_actions_symbol
    ON corporate_actions (symbol);

-- ── Meta ─────────────────────────────────────────────────────────────────────
-- Tracks last successful fetch time per table.

CREATE TABLE IF NOT EXISTS _meta (
    table_name          TEXT    PRIMARY KEY,
    last_fetched_at     TEXT    NOT NULL,
    row_count           INTEGER
);

"""


def init_db():
    """Creates all tables if they don't exist. Safe to call on every startup."""
    with get_conn() as conn:
        conn.executescript(SCHEMA)
    print(f"[db] Initialised at {get_db_path()}")


# ── Meta helpers ──────────────────────────────────────────────────────────────

def update_meta(conn: sqlite3.Connection, table_name: str, row_count: int):
    """Updates _meta for the given table after a successful fetch."""
    now = datetime.now(timezone.utc).isoformat()
    conn.execute("""
        INSERT INTO _meta (table_name, last_fetched_at, row_count)
        VALUES (?, ?, ?)
        ON CONFLICT(table_name) DO UPDATE SET
            last_fetched_at = excluded.last_fetched_at,
            row_count       = excluded.row_count
    """, (table_name, now, row_count))


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    init_db()
