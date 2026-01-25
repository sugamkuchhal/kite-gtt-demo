import argparse
import logging
import os
import subprocess
import sys
from pathlib import Path

from kiteconnect import KiteConnect

from runtime_paths import get_access_token_path, get_api_key_path


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")


def _read_api_key():
    api_key_path = get_api_key_path()
    if not api_key_path.exists():
        raise FileNotFoundError(f"Missing API key file: {api_key_path}")
    lines = api_key_path.read_text(encoding="utf-8").splitlines()
    if not lines or not lines[0].strip():
        raise ValueError(f"API key file is empty: {api_key_path}")
    return lines[0].strip()


def _read_access_token():
    token_path = get_access_token_path()
    if not token_path.exists():
        return None
    token = token_path.read_text(encoding="utf-8").strip()
    return token or None


def _token_is_valid(api_key, access_token):
    if not access_token:
        return False
    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)
    try:
        kite.margins()
        return True
    except Exception as e:
        logging.warning("Token validation failed: %s", e)
        return False


def _run_auto_login():
    result = subprocess.run([sys.executable, "auto_login.py"], check=False)
    if result.returncode != 0:
        logging.error("auto_login.py failed with code %s", result.returncode)
        return None
    token = _read_access_token()
    if not token:
        logging.error("access_token.txt missing or empty after auto_login")
        return None
    return token


def _update_github_secret(token):
    secret_name = os.getenv("GH_SECRET_NAME", "ACCESS_TOKEN")
    env = os.environ.copy()
    if "GH_TOKEN" not in env and "GH_PAT" in env:
        env["GH_TOKEN"] = env["GH_PAT"]
    if not env.get("GH_TOKEN"):
        logging.info("GH_TOKEN not set; using gh CLI auth if available.")
    try:
        subprocess.run(
            ["gh", "secret", "set", secret_name, "--body", token],
            check=True,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        logging.info("GitHub secret %s updated.", secret_name)
        return True
    except FileNotFoundError:
        logging.error("GitHub CLI (gh) not found. Install it to update secrets.")
        return False
    except subprocess.CalledProcessError as e:
        logging.error("Failed to update GitHub secret: %s", e.stderr.strip())
        return False


def main():
    parser = argparse.ArgumentParser(description="Ensure Kite access token is valid.")
    parser.add_argument("--force", action="store_true", help="Force auto_login even if token is valid.")
    args = parser.parse_args()

    try:
        api_key = _read_api_key()
    except Exception as e:
        logging.error("Cannot read API key: %s", e)
        return 2

    access_token = _read_access_token()
    if not args.force and _token_is_valid(api_key, access_token):
        logging.info("Access token is valid; no login needed.")
        return 0

    logging.info("Access token invalid or forced refresh; running auto_login.")
    new_token = _run_auto_login()
    if not new_token:
        return 1

    if not _update_github_secret(new_token):
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
