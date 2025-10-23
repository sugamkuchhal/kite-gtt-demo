from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# Path to your service account JSON file
SERVICE_ACCOUNT_FILE = "creds.json"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

SPREADSHEET_ID = "14G8Yinl28F9ZROedyhiH4p5jCz2bcfA2goVB21PVE1s"
SHEET_NAME = "ALL_OLD_GTTs"
CELL = "R1"

def main():
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    service = build("sheets", "v4", credentials=creds)

    body = {
        "values": [["FALSE"]]
    }

    result = service.spreadsheets().values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{SHEET_NAME}!{CELL}",
        valueInputOption="USER_ENTERED",
        body=body
    ).execute()

    print(f"Updated {CELL} in {SHEET_NAME} to FALSE")

if __name__ == "__main__":
    main()
