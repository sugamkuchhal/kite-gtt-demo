from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

from runtime_paths import get_creds_path
from ref_sheets_utils import resolve_sheet_id

# Path to your service account JSON file
SERVICE_ACCOUNT_FILE = str(get_creds_path())
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

REF_SHEETS = "PORTFOLIO"
SHEET_NAME = "ALL_OLD_GTTs"
CELL = "R1"

def main():
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    service = build("sheets", "v4", credentials=creds)

    body = {
        "values": [["FALSE"]]
    }

    spreadsheet_id = resolve_sheet_id(REF_SHEETS)
    result = service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=f"{SHEET_NAME}!{CELL}",
        valueInputOption="USER_ENTERED",
        body=body
    ).execute()

    print(f"Updated {CELL} in {SHEET_NAME} to FALSE")

if __name__ == "__main__":
    main()
