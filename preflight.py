import sys
import subprocess

from runtime_paths import get_api_key_path, get_access_token_path

# This script has no Google Sheets dependency by design.

def ensure_kiteconnect():
    try:
        from kiteconnect import KiteConnect
        return KiteConnect
    except ImportError:
        print("kiteconnect not installed. Attempting to install...")
        try:
            subprocess.check_call([sys.executable, "-m", "pip", "install", "kiteconnect"])
            from kiteconnect import KiteConnect
            print("kiteconnect installed successfully.")
            return KiteConnect
        except Exception as exc:
            print(f"Unable to install kiteconnect automatically: {exc}")
            sys.exit(2)

def main():
    KiteConnect = ensure_kiteconnect()
    api_file = get_api_key_path()
    token_file = get_access_token_path()

    if not api_file.exists() or not token_file.exists():
        print("Missing key or token file.")
        sys.exit(3)

    api = api_file.read_text().splitlines()[0].strip()
    token = token_file.read_text().strip()

    kite = KiteConnect(api_key=api)
    kite.set_access_token(token)

    try:
        kite.margins()   # simple call; fails fast if token invalid
        print("Kite access token valid.")
        sys.exit(0)
    except Exception as e:
        print(f"Kite access token invalid: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
