import os
import time
import logging
import pyotp
import tempfile
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


logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')

# Load secrets
with open("api_key.txt") as f:
    lines = [line.strip() for line in f.readlines()]
    API_KEY = lines[0]
    API_SECRET = lines[1]
    USER_ID = lines[2]
    PASSWORD = lines[3]
    TOTP_SECRET = lines[4]

LOGIN_URL = f"https://kite.zerodha.com/connect/login?api_key={API_KEY}&v=3"

def auto_login_and_get_kite():
    logging.info("🚀 Starting auto login process")

    options = Options()
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    # options.add_argument("--headless")  # Optional: headless mode
    options.add_argument("--incognito")

    try:
        driver_path = ChromeDriverManager().install()
    except Exception as e:
        logging.warning(f"⚠️ webdriver-manager failed: {e}")
        driver_path = shutil.which("chromedriver")
    
    driver = webdriver.Chrome(service=Service(driver_path), options=options)

    options.add_argument('--incognito')
    driver.get(LOGIN_URL)
    wait = WebDriverWait(driver, 8)
    totp_wait = WebDriverWait(driver, 3)

    logging.info(f"🌐 Opened login URL: {LOGIN_URL}")

    # Wait for the password input to appear (common on both variants)
    try:
        password_element = wait.until(EC.presence_of_element_located((By.ID, "password")))
    except TimeoutException:
        logging.error("❌ Password input did not appear - login page might have changed.")
        driver.quit()
        return None, None

    # Check if userid input is present (fresh login page 1.1) or not (session active page 1.2)
    userid_elements = driver.find_elements(By.ID, "userid")
    if userid_elements:
        # fresh login flow: enter userid + password
        logging.info("🆕 Fresh login detected - entering USER ID and PASSWORD")
        userid_element = userid_elements[0]
        userid_element.send_keys(USER_ID)
        logging.info("🔑 Entered username")

        password_element.send_keys(PASSWORD)
        logging.info("🔒 Entered password")

        submit_btn = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, 'button[type="submit"]')))
        submit_btn.click()
        logging.info("➡️ Clicked login button")

        # Wait for page 1 userid element to become stale - ensures page 1 submission completed
        try:
            WebDriverWait(driver, 3).until(EC.staleness_of(userid_element))
            logging.info("⏳ Page 1 submitted, moving to TOTP page")
        except TimeoutException:
            logging.warning("⚠️ Page 1 userid element did not go stale after submit, proceeding cautiously")

    else:
        # session active flow: only password field present
        logging.info("🔄 Session active detected - entering PASSWORD only")
        password_element.send_keys(PASSWORD)
        logging.info("🔒 Entered password")

        driver.find_element(By.CSS_SELECTOR, 'button[type="submit"]').click()
        logging.info("➡️ Clicked login button (session active flow)")

        # Wait for password element to go stale (page transition)
        try:
            wait.until(EC.staleness_of(password_element))
            logging.info("⏳ Page 1 submitted, moving to TOTP page")
        except TimeoutException:
            logging.warning("⚠️ Password element did not go stale after submit, proceeding cautiously")

    # Now wait for page 2 (TOTP) userid input to appear
    logging.info("⏳ Waiting for TOTP input field on page 2")
    try:
        totp_input = totp_wait.until(EC.presence_of_element_located((By.ID, "userid")))
    except TimeoutException:
        logging.error("❌ TOTP input field did not appear on page 2")
        driver.quit()
        return None, None

    # Enter TOTP
    totp_code = pyotp.TOTP(TOTP_SECRET).now()
    logging.info(f"📟 Generated TOTP: {totp_code}")
    totp_input.clear()
    totp_input.send_keys(totp_code)
    logging.info("✅ Entered TOTP")

    driver.find_element(By.CSS_SELECTOR, 'button[type="submit"]').click()
    logging.info("➡️ Clicked continue after TOTP")

    # Wait for redirect URL after login success
    time.sleep(3)
    current_url = driver.current_url
    driver.quit()
    logging.info(f"🔄 Redirected to: {current_url}")

    parsed_url = urlparse(current_url)
    request_token = parse_qs(parsed_url.query).get("request_token", [None])[0]

    if not request_token:
        logging.error("❌ Could not extract request_token from URL")
        return None, None

    logging.info(f"✅ request_token: {request_token}")

    kite = KiteConnect(api_key=API_KEY)
    try:
        session_data = kite.generate_session(request_token, api_secret=API_SECRET)
        kite.set_access_token(session_data["access_token"])
        logging.info(f"✅ Access token: {session_data['access_token']}")

        with open("access_token.txt", "w") as f:
            f.write(session_data["access_token"])

        return kite, session_data["access_token"]

    except Exception as e:
        logging.error(f"❌ Failed to generate access token: {e}")
        return None, None

def main():
    kite, _ = auto_login_and_get_kite()
    if kite:
        profile = kite.profile()
        logging.info(f"👤 Logged in as: {profile['user_name']} (user_id={profile['user_id']})")

if __name__ == "__main__":
    main()
