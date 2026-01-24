#kite_session.py

import os
import logging
from kiteconnect import KiteConnect
import subprocess

from runtime_paths import get_access_token_path, get_api_key_path
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')

API_KEY = None
API_SECRET = None

def load_credentials():
    global API_KEY, API_SECRET
    if API_KEY and API_SECRET:
        return API_KEY, API_SECRET

    with open(get_api_key_path(), "r", encoding="utf-8") as f:
        lines = [line.strip() for line in f.readlines()]
        API_KEY = lines[0]
        API_SECRET = lines[1]
    return API_KEY, API_SECRET

def is_token_valid(kite):
    try:
        kite.profile()
        return True
    except Exception as e:
        logging.warning(f"Token validation failed: {e}")
        return False

def run_auto_login():
    logging.info("🔁 Running auto_login.py to refresh session...")
    subprocess.run(["python3", "auto_login.py"], check=True)

def get_kite():
    api_key, api_secret = load_credentials()
    kite = KiteConnect(api_key=api_key)

    access_token_path = get_access_token_path()
    if os.path.exists(access_token_path):
        with open(access_token_path, "r", encoding="utf-8") as f:
            access_token = f.read().strip()
        kite.set_access_token(access_token)

        if is_token_valid(kite):
            logging.info("✅ Using existing valid access token.")
            return kite

    # fallback: get new token via auto_login
    run_auto_login()
    with open(access_token_path, "r", encoding="utf-8") as f:
        access_token = f.read().strip()
    kite.set_access_token(access_token)
    logging.info("✅ New access token set.")
    return kite

