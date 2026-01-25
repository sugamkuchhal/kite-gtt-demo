import os
import sys
import logging
import pyotp
import tempfile
from functools import lru_cache
from urllib.parse import urlparse, parse_qs
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
import shutil
from kiteconnect import KiteConnect
from webdriver_manager.chrome import ChromeDriverManager

from runtime_paths import get_access_token_path, get_api_key_path
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')

# Load secrets
with open(get_api_key_path(), "r", encoding="utf-8") as f:
    lines = [line.strip() for line in f.readlines()]
    API_KEY = lines[0]
    API_SECRET = lines[1]
    USER_ID = lines[2]
    PASSWORD = lines[3]
    TOTP_SECRET = lines[4]

LOGIN_URL = f"https://kite.zerodha.com/connect/login?api_key={API_KEY}&v=3"


def _env_float(name: str, default: float) -> float:
    val = os.getenv(name)
    if not val:
        return default
    try:
        return float(val)
    except ValueError:
        logging.warning("⚠️ Invalid %s=%s; using default %s", name, val, default)
        return default


LOGIN_WAIT_SECS = _env_float("LOGIN_WAIT_SECS", 8.0)
TOTP_WAIT_SECS = _env_float("TOTP_WAIT_SECS", 3.0)
SUBMIT_WAIT_SECS = _env_float("SUBMIT_WAIT_SECS", 3.0)
REDIRECT_WAIT_SECS = _env_float("REDIRECT_WAIT_SECS", 8.0)


@lru_cache(maxsize=1)
def _find_chrome_binary():
    """Detect Chrome binary path for macOS, Linux, or CI."""
    env_path = os.environ.get("CHROME_BINARY")
    if env_path:
        if os.path.exists(env_path):
            return env_path
        logging.warning("⚠️ CHROME_BINARY is set but not found: %s", env_path)

    attempted = []
    mac_candidates = [
        "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
        "/Applications/Google Chrome for Testing.app/Contents/MacOS/Google Chrome for Testing",
        "/Applications/Google Chrome Beta.app/Contents/MacOS/Google Chrome Beta",
        "/Applications/Google Chrome Canary.app/Contents/MacOS/Google Chrome Canary",
        "/Applications/Chromium.app/Contents/MacOS/Chromium",
    ]
    for p in mac_candidates:
        attempted.append(p)
        if os.path.exists(p):
            return p

    linux_candidates = [
        "/usr/bin/google-chrome",
        "/usr/bin/google-chrome-stable",
        "/usr/bin/chromium",
        "/usr/bin/chromium-browser",
    ]
    for p in linux_candidates:
        attempted.append(p)
        if os.path.exists(p):
            return p

    raise FileNotFoundError(
        "🚫 Could not find Chrome binary. "
        "Install Chrome or set CHROME_BINARY env var to its path. "
        f"Attempted: {attempted}"
    )


def build_driver():
    options = Options()
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)

    # ✅ Only headless when in CI or explicitly requested
    if os.environ.get("CI") == "true" or os.environ.get("HEADLESS") == "1":
        options.add_argument("--headless=new")

    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--incognito")
    options.add_argument("--window-size=1280,800")

    # ✅ Fix “user data directory is already in use” error
    options.add_argument(f"--user-data-dir={tempfile.mkdtemp(prefix='chrome-profile-')}")

    # ✅ Detect correct Chrome binary
    options.binary_location = _find_chrome_binary()

    # ✅ Setup ChromeDriver
    try:
        driver_path = ChromeDriverManager().install()
        logging.info(f"✅ Using ChromeDriver: {driver_path}")
    except Exception as e:
        logging.warning(f"⚠️ webdriver-manager failed: {e}")
        driver_path = shutil.which("chromedriver")

    if driver_path:
        return webdriver.Chrome(service=Service(driver_path), options=options)
    return webdriver.Chrome(options=options)


def login_page_1(driver, wait):
    # Wait for the password input to appear (common on both variants)
    try:
        password_element = wait.until(EC.presence_of_element_located((By.ID, "password")))
    except TimeoutException:
        logging.error("❌ Password input did not appear - login page might have changed.")
        return False

    # Check if userid input is present (fresh login or session active)
    userid_elements = driver.find_elements(By.ID, "userid")
    if userid_elements:
        logging.info("🆕 Fresh login detected - entering USER ID and PASSWORD")
        userid_element = userid_elements[0]
        userid_element.send_keys(USER_ID)
        logging.info("🔑 Entered username")

        password_element.send_keys(PASSWORD)
        logging.info("🔒 Entered password")

        submit_btn = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, 'button[type="submit"]')))
        submit_btn.click()
        logging.info("➡️ Clicked login button")

        try:
            WebDriverWait(driver, SUBMIT_WAIT_SECS).until(EC.staleness_of(userid_element))
            logging.info("⏳ Page 1 submitted, moving to TOTP page")
        except TimeoutException:
            logging.warning("⚠️ Page 1 userid element did not go stale after submit, proceeding cautiously")
    else:
        logging.info("🔄 Session active detected - entering PASSWORD only")
        password_element.send_keys(PASSWORD)
        logging.info("🔒 Entered password")

        driver.find_element(By.CSS_SELECTOR, 'button[type="submit"]').click()
        logging.info("➡️ Clicked login button (session active flow)")

        try:
            WebDriverWait(driver, SUBMIT_WAIT_SECS).until(EC.staleness_of(password_element))
            logging.info("⏳ Page 1 submitted, moving to TOTP page")
        except TimeoutException:
            logging.warning("⚠️ Password element did not go stale after submit, proceeding cautiously")
    return True


def login_totp(driver, totp_wait):
    # Now wait for TOTP input
    logging.info("⏳ Waiting for TOTP input field on page 2")
    try:
        totp_input = totp_wait.until(EC.presence_of_element_located((By.ID, "userid")))
    except TimeoutException:
        logging.error("❌ TOTP input field did not appear on page 2")
        return False

    # Enter TOTP
    totp_code = pyotp.TOTP(TOTP_SECRET).now()
    logging.info("📟 Generated TOTP")
    totp_input.clear()
    totp_input.send_keys(totp_code)
    logging.info("✅ Entered TOTP")

    driver.find_element(By.CSS_SELECTOR, 'button[type="submit"]').click()
    logging.info("➡️ Clicked continue after TOTP")
    return True


def extract_request_token(driver):
    # Wait for redirect URL after login success
    try:
        WebDriverWait(driver, REDIRECT_WAIT_SECS).until(
            lambda d: "request_token=" in d.current_url
        )
    except TimeoutException:
        logging.error("❌ Redirect did not include request_token within timeout")
    current_url = driver.current_url
    logging.info(f"🔄 Redirected to: {current_url}")

    parsed_url = urlparse(current_url)
    request_token = parse_qs(parsed_url.query).get("request_token", [None])[0]

    if not request_token:
        logging.error("❌ Could not extract request_token from URL")
        return None

    logging.info(f"✅ request_token: {request_token}")
    return request_token


def exchange_token(request_token):
    kite = KiteConnect(api_key=API_KEY)
    try:
        session_data = kite.generate_session(request_token, api_secret=API_SECRET)
        kite.set_access_token(session_data["access_token"])
        logging.info(f"✅ Access token: {session_data['access_token']}")

        access_token_path = get_access_token_path()
        with open(access_token_path, "w", encoding="utf-8") as f:
            f.write(session_data["access_token"])

        return kite, session_data["access_token"]

    except Exception as e:
        logging.error(f"❌ Failed to generate access token: {e}")
        return None, None


def auto_login_and_get_kite():
    logging.info("🚀 Starting auto login process")
    driver = None
    request_token = None
    try:
        driver = build_driver()
        wait = WebDriverWait(driver, LOGIN_WAIT_SECS)
        totp_wait = WebDriverWait(driver, TOTP_WAIT_SECS)

        driver.get(LOGIN_URL)
        logging.info(f"🌐 Opened login URL: {LOGIN_URL}")

        if not login_page_1(driver, wait):
            return None, None
        if not login_totp(driver, totp_wait):
            return None, None

        request_token = extract_request_token(driver)
        if not request_token:
            return None, None
    except Exception as e:
        logging.error(f"❌ Unexpected error during login flow: {e}")
        return None, None
    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass

    return exchange_token(request_token)


def main():
    kite, _ = auto_login_and_get_kite()
    if kite:
        profile = kite.profile()
        logging.info(f"👤 Logged in as: {profile['user_name']} (user_id={profile['user_id']})")
        return 0
    logging.error("❌ Auto-login failed.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
