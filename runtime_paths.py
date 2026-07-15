from __future__ import annotations

from pathlib import Path
import os
from typing import Iterable, Optional

# This script has no Google Sheets dependency by design.

def repo_root() -> Path:
    return Path(__file__).resolve().parent


def _expand(path_str: str) -> Path:
    return Path(path_str).expanduser()


def _first_env_path(names: Iterable[str]) -> Optional[Path]:
    for name in names:
        value = os.getenv(name)
        if value:
            return _expand(value)
    return None


def resolve_path(filename: str, env_vars: Iterable[str] = ()) -> Path:
    """
    Resolve a file path that works both locally and in CI.
    Order of precedence:
      1) Explicit env vars (per-file).
      2) SECRETS_DIR / KITE_SECRETS_DIR (directory override).
      3) Repo root (same directory as this file).
      4) Legacy macOS path, if it exists.
      5) Fallback to repo root.
    """
    env_path = _first_env_path(env_vars)
    if env_path:
        return env_path

    secrets_dir = os.getenv("SECRETS_DIR") or os.getenv("KITE_SECRETS_DIR")
    if secrets_dir:
        return _expand(secrets_dir) / filename

    return repo_root() / filename


def get_creds_path() -> Path:
    return resolve_path(
        "creds.json",
        env_vars=("CREDS_JSON_PATH", "GOOGLE_CREDS_PATH", "GOOGLE_APPLICATION_CREDENTIALS"),
    )


def get_api_key_path() -> Path:
    return resolve_path("api_key.txt", env_vars=("API_KEY_PATH",))


def get_access_token_path() -> Path:
    return resolve_path("access_token.txt", env_vars=("ACCESS_TOKEN_PATH",))


def get_smtp_token_path() -> Path:
    return resolve_path("smtp_token.json", env_vars=("SMTP_TOKEN_PATH",))


def get_telegram_token_path() -> Path:
    return resolve_path("telegram_token.json", env_vars=("TELEGRAM_TOKEN_PATH",))


# ── Notification config ───────────────────────────────────────────────────────
# Central place for SMTP and Telegram constants.
# Import from here instead of redefining in each script.

SMTP_FROM        = "sugamkuchhal@gmail.com"
SMTP_USER        = "sugamkuchhal@gmail.com"
SMTP_SERVER      = "smtp.gmail.com"
SMTP_PORT        = 587

TELEGRAM_CHAT_ID = "182871861"

DEFAULT_RECIPIENT_EMAIL = "sugam.kuchhal.iimc@gmail.com"
