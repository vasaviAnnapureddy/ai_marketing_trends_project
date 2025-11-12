# sheets_utils.py
import os, time, json
from dotenv import load_dotenv
from pathlib import Path
load_dotenv(dotenv_path=Path(__file__).resolve().parents[1] / ".env")

import gspread
from google.oauth2.service_account import Credentials

GSHEET_ID = os.getenv("GSHEET_ID")

def gc():
    creds = Credentials.from_service_account_file(
        str(Path(__file__).resolve().parents[1] / "service_account.json"),
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    return gspread.authorize(creds)

def get_sheet(name="WROTE_CLEAN_ALL"):
    wb = gc().open_by_key(GSHEET_ID)
    try:
        return wb.worksheet(name)
    except Exception:
        wss = wb.worksheets()
        if wss:
            print(f"Warning: worksheet '{name}' not found. Using first worksheet '{wss[0].title}'.")
            return wss[0]
        return wb.add_worksheet(title=name, rows="1000", cols="20")

def get_all_rows(name="WROTE_CLEAN_ALL"):
    ws = get_sheet(name)
    return ws.get_all_records()

def update_cell(name, row, col, val):
    ws = get_sheet(name)
    ws.update_cell(row, col, val)

def append_row(name, row_vals):
    ws = get_sheet(name)
    ws.append_row(row_vals)

def _safe_value(v):
    # Convert list/dict/NaN -> string so Google Sheets accepts it as single cell
    if v is None:
        return ""
    if isinstance(v, (list, dict)):
        return json.dumps(v, ensure_ascii=False)
    try:
        # allow primitives
        if isinstance(v, float) and (v != v):  # NaN check
            return ""
        return v
    except Exception:
        return str(v)

def write_rows(name, header, rows):
    """
    Replace worksheet contents (fast bulk update).
    header: list of column names
    rows: list of lists/iterables (length should match header)
    """
    wb = gc().open_by_key(GSHEET_ID)
    try:
        # remove existing sheet to avoid type/range issues
        try:
            ws = wb.worksheet(name)
            wb.del_worksheet(ws)
        except Exception:
            pass
        # create new
        rows_count = max(2, len(rows) + 1)
        ws = wb.add_worksheet(title=name, rows=str(rows_count + 10), cols=str(max(10, len(header))))
        # convert rows to strings and bulk update at A1
        values = []
        values.append([_safe_value(h) for h in header])
        for r in rows:
            # if row is dict, convert using header order
            if isinstance(r, dict):
                row_vals = [_safe_value(r.get(h, "")) for h in header]
            else:
                row_vals = [_safe_value(v) for v in r]
            values.append(row_vals)
        # bulk write
        ws.update("A1", values, value_input_option="USER_ENTERED")
    except Exception as e:
        raise RuntimeError(f"write_rows failed: {e}")



'''# sheets_utils.py
import os
import time
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(dotenv_path=Path(__file__).resolve().parents[1] / ".env")

import gspread
from google.oauth2.service_account import Credentials

GSHEET_ID = os.getenv("GSHEET_ID")
SERVICE_ACCOUNT_FILE = str(Path(__file__).resolve().parents[1] / "service_account.json")

# --- helpers ---
def gc():
    creds = Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    return gspread.authorize(creds)

def get_spreadsheet():
    if not GSHEET_ID:
        raise RuntimeError("GSHEET_ID not set in environment (.env).")
    return gc().open_by_key(GSHEET_ID)

def get_sheet(name="WROTE_CLEAN_ALL"):
    wb = get_spreadsheet()
    try:
        return wb.worksheet(name)
    except Exception:
        # fallback: return first worksheet or create
        wss = wb.worksheets()
        if wss:
            return wss[0]
        return wb.add_worksheet(title=name, rows="1000", cols="20")

def get_all_rows(name="WROTE_CLEAN_ALL"):
    ws = get_sheet(name)
    # returns list of dictionaries
    try:
        return ws.get_all_records()
    except Exception:
        return []

def update_cell(name, row, col, val):
    ws = get_sheet(name)
    ws.update_cell(row, col, val)

# --- bulk append / write helpers (use bulk update to avoid quota) ---
def append_rows(name, rows):
    """
    Append multiple rows in one API call where possible.
    `rows` should be a list of lists.
    """
    if not rows:
        return
    wb = get_spreadsheet()
    try:
        ws = wb.worksheet(name)
    except Exception:
        ws = wb.add_worksheet(title=name, rows=str(max(100, len(rows)+5)), cols=str(len(rows[0])+1))
    # find first empty row
    existing = ws.row_count
    # try to append using values_append which still does many requests if rows large;
    # instead use update with a calculated range to perform a single call.
    start_row = existing + 1
    try:
        # expand sheet if necessary
        needed_rows = start_row + len(rows) - 1
        if needed_rows > ws.row_count:
            ws.add_rows(needed_rows - ws.row_count)
        range_a1 = f"A{start_row}"
        ws.update(range_a1, rows, value_input_option="RAW")
    except Exception as e:
        # fallback to values_append (gspread will do internal batching)
        ws.append_rows(rows)

def write_rows(name, header, rows):
    """
    Replace worksheet (safe) and write header + rows in ONE bulk update.
    This avoids per-row append calls which hit quota limits.
    """
    wb = get_spreadsheet()
    # delete existing (if exists) and create fresh sheet
    try:
        ws = wb.worksheet(name)
        wb.del_worksheet(ws)
    except Exception:
        pass
    # create new sheet sized to data
    num_rows = max(2, len(rows) + 1)
    num_cols = max(len(header), 1)
    ws = wb.add_worksheet(title=name, rows=str(num_rows), cols=str(num_cols))
    # prepare values as list-of-lists and stringify everything
    def safe_cell(v):
        if v is None:
            return ""
        if isinstance(v, (list, tuple)):
            return ", ".join(str(x) for x in v)
        # numpy etc
        try:
            import numpy as _np
            if isinstance(v, _np.ndarray):
                return ", ".join(str(x) for x in v.tolist())
        except Exception:
            pass
        return str(v)

    values = [ [safe_cell(c) for c in header] ]
    for r in rows:
        values.append([safe_cell(r.get(h, "")) if isinstance(r, dict) else safe_cell(c) for h, c in zip(header, (r if isinstance(r, list) else []))] if isinstance(r, dict) else [safe_cell(c) for c in r])
    # In case rows is list of lists:
    if rows and isinstance(rows[0], list):
        values = [ [safe_cell(h) for h in header] ] + [[safe_cell(c) for c in row] for row in rows]

    # bulk update single call
    ws.update("A1", values, value_input_option="RAW")
'''



