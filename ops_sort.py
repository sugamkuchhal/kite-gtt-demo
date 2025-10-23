import gspread
from datetime import datetime
import argparse

# --- CONFIGURATION ---
CREDENTIALS_PATH = "/Users/sugamkuchhal/Documents/kite-gtt-demo-vs/creds_vs.json"

# --- HELPERS ---
def log(msg):
    # print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}")
    print(f"{msg}")

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--sheet-name', required=True)
    parser.add_argument('--green-tab', required=True)
    parser.add_argument('--red-tab', required=True)
    parser.add_argument('--yellow-tab', required=True)
    parser.add_argument('--loose-update', action='store_true',
        help='If set, Red update matches ONLY on Col A. Default: matches on BOTH Col A and B.')
    return parser.parse_args()

def get_rows_with_action(data_rows, keyword):
    results = []
    for i, row in enumerate(data_rows, start=2):
        if len(row) >= 15 and keyword.lower() in row[14].lower():
            results.append((i, row))
    return results

def main():
    args = parse_args()
    log("")
    gc = gspread.service_account(filename=CREDENTIALS_PATH)
    wb = gc.open(args.sheet_name)

    green_ws = wb.worksheet(args.green_tab)
    red_ws = wb.worksheet(args.red_tab)
    yellow_ws = wb.worksheet(args.yellow_tab)

    green_rows = green_ws.get_values("A2:O")
    red_rows   = red_ws.get_values("A2:O")
    yellow_rows = yellow_ws.get_values("A2:O")

    # --------- 1. CLEAR ACTION SHEET ---------
    log("CLEAR TASK: Clearing Action Sheet")
    if yellow_rows:
        # Clear all except header
        yellow_ws.batch_clear([f"A2:O{yellow_ws.row_count}"])
        log("CLEAR TASK: Rows cleared from Action Sheet")
    else:
        log("CLEAR TASK: Nothing to clear")
    
    # --------- recompute next-append rows AFTER clear ---------
    # Yellow should start right under the header (row 2)
    yellow_colA_count = len(yellow_ws.col_values(1))
    yellow_next = max(2, yellow_colA_count + 1)
    
    # Red can be computed once here
    red_next = len(red_ws.col_values(1)) + 1

    log(f"⚙️ SCRIPT STARTED for {wb.title}")

    # --- TOUCH to force recalc without reading anything ---
    stamp = datetime.now().isoformat(timespec="seconds")  # already imported at top
    for ws, name in [(green_ws, "Green"), (red_ws, "Red"), (yellow_ws, "Yellow")]:
        try:
            ws.update_acell("F1", stamp)  # any write triggers recalc
            log(f"TOUCH: Triggered formula recalc for {name} Sheet.")
        except Exception as e:
            log(f"TOUCH: Could not touch {name} Sheet: {e}")

    import time
    log("WAIT: Sleeping 10 seconds for Sheets to refresh/recalculate.")
    time.sleep(10)

    # --------- 2. BATCH UPDATE TASK ---------
    log("UPDATE TASK: Looking for 'Update' rows in Green Sheet")
    updates = get_rows_with_action(green_rows, "update")

    yellow_update_rows = []
    red_update_idxs = []
    red_update_values = []
    for row_idx, green_row in updates:
        # Prepare Action/Yellow row (A, B, C, D, E, O)
        action_row = [green_row[0], green_row[1], green_row[2], green_row[3], green_row[4]] + [''] * 9 + [green_row[14]]
        yellow_update_rows.append(action_row)
        # Find and batch Red update by A & B match
        for i, r_row in enumerate(red_rows, start=2):
            if (not args.loose_update and r_row[0] == green_row[0] and r_row[1] == green_row[1]) or (args.loose_update and r_row[0] == green_row[0]):
                red_update_idxs.append(i)
                red_update_values.append([green_row[0], green_row[1], green_row[2], green_row[3], green_row[4]])
                break

    # Batch append to Yellow
    if yellow_update_rows:
        start_row = yellow_next
        yellow_ws.update(range_name=f"A{start_row}:O{start_row+len(yellow_update_rows)-1}", values=yellow_update_rows, value_input_option='USER_ENTERED')
        yellow_next += len(yellow_update_rows)

    # --- BATCHED: Batch update Red (all at once, not per-row) ---
    if red_update_idxs and red_update_values:
        requests = []
        for idx, vals in zip(red_update_idxs, red_update_values):
            requests.append({'range': f"A{idx}:E{idx}", 'values': [vals]})
        red_ws.batch_update(requests,value_input_option='USER_ENTERED')

    # --------- 3. BATCH INSERT TASK ---------
    log("INSERT TASK: Looking for 'Insert' rows in Green Sheet")
    inserts = get_rows_with_action(green_rows, "insert")

    yellow_insert_rows = []
    red_insert_rows = []
    for row_idx, green_row in inserts:
        action_row = [green_row[0], green_row[1], green_row[2], green_row[3], green_row[4]] + [''] * 9 + [green_row[14]]
        yellow_insert_rows.append(action_row)
        red_insert_rows.append([green_row[0], green_row[1], green_row[2], green_row[3], green_row[4]])

    if yellow_insert_rows:
        start_row = yellow_next
        yellow_ws.update(range_name=f"A{start_row}:O{start_row+len(yellow_insert_rows)-1}", values=yellow_insert_rows, value_input_option='USER_ENTERED')
        yellow_next += len(yellow_insert_rows)

    if red_insert_rows:
        start_row = red_next
        red_ws.update(range_name=f"A{start_row}:E{start_row+len(red_insert_rows)-1}", values=red_insert_rows, value_input_option='USER_ENTERED')
        red_next += len(red_insert_rows)

    # --------- 4. BATCH DELETE TASK ---------
    log("DELETE TASK: Looking for 'Delete' rows in Red Sheet")
    deletes = get_rows_with_action(red_rows, "delete")

    red_delete_idxs = []
    yellow_delete_rows = []
    for row_idx, red_row in deletes:
        action_row = [red_row[0], red_row[1], red_row[2], red_row[3], red_row[4]] + [''] * 9 + [red_row[14]]
        yellow_delete_rows.append(action_row)
        red_delete_idxs.append(row_idx)

    # Append all delete actions to Yellow at once
    if yellow_delete_rows:
        start_row = yellow_next
        yellow_ws.update(range_name=f"A{start_row}:O{start_row+len(yellow_delete_rows)-1}", values=yellow_delete_rows, value_input_option='USER_ENTERED')
        yellow_next += len(yellow_delete_rows)

    # --- BATCHED: Clear A–E of all relevant Red rows at once ---
    if red_delete_idxs:
        requests = []
        for idx in red_delete_idxs:
            requests.append({'range': f"A{idx}:E{idx}", 'values': [[""]*5]})
        red_ws.batch_update(requests,value_input_option='USER_ENTERED')

    # Sort Red Sheet by A (ascending), if needed (API supports basic sorts)
    if red_delete_idxs:
        red_ws.sort((1, 'asc'))  # sort by Col A

    log("✅ SCRIPT COMPLETED.")
    log("")

if __name__ == "__main__":
    main()
