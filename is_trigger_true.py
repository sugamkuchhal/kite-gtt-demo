#!/usr/bin/env python3

from runtime_paths import get_creds_path

SHEET_ID = "14G8Yinl28F9ZROedyhiH4p5jCz2bcfA2goVB21PVE1s"
RANGE = "ALL_OLD_GTTs!R1"

def is_trigger_true():
    try:
        import gspread
        from google.oauth2.service_account import Credentials

        scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
        creds = Credentials.from_service_account_file(str(get_creds_path()), scopes=scopes)
        gc = gspread.authorize(creds)
        sheet = gc.open_by_key(SHEET_ID)
        result = sheet.values_get(RANGE, params={"valueRenderOption": "FORMATTED_VALUE"})
        value = result.get("values", [[""]])[0][0]

        return str(value).strip().lower() == "true"
    except Exception:
        return False

if __name__ == "__main__":
    print(is_trigger_true())
