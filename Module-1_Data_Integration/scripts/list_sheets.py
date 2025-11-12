# list_sheets.py
from dotenv import load_dotenv
import os
from pathlib import Path
load_dotenv(dotenv_path=Path(__file__).resolve().parents[1] / ".env")

from sheets_utils import gc
import traceback

try:
    sh = gc().open_by_key(os.getenv("GSHEET_ID"))
    print("Spreadsheet title:", sh.title)
    print("Worksheets found:")
    for ws in sh.worksheets():
        print(" -", ws.title)
except Exception as e:
    print("ERROR listing sheets:", type(e).__name__, e)
    traceback.print_exc()
