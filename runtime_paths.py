from __future__ import annotations

from pathlib import Path
import os
from typing import Iterable, Optional

_DEFAULT_LEGACY_ROOT = Path("/Users/sugamkuchhal/Documents/kite-gtt-demo")


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

    candidate = repo_root() / filename
    if candidate.exists():
        return candidate

    legacy_root = _expand(os.getenv("KITE_LEGACY_ROOT", str(_DEFAULT_LEGACY_ROOT)))
    legacy_candidate = legacy_root / filename
    if legacy_candidate.exists():
        return legacy_candidate

    return candidate


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
