from kiteconnect import KiteConnect
import sys

from runtime_paths import get_api_key_path, get_access_token_path

def main():
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
