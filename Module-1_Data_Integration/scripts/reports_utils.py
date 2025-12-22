# scripts/report_utils.py
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt

REPORTS_DIR = Path("reports")

def date_window(days: int = 7, end: pd.Timestamp | None = None):
    end = pd.to_datetime(end) if end is not None else pd.Timestamp.utcnow()
    start = end - pd.Timedelta(days=days)
    # normalize to UTC-aware timestamps
    start = pd.to_datetime(start, utc=True)
    end = pd.to_datetime(end, utc=True)
    return start, end

def ensure_out(path: str):
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    return str(p)

def plot_barh(series, out_png, title=""):
    """
    series: pandas Series indexed by label with numeric values
    """
    out_png = ensure_out(out_png)
    plt.figure()
    # horizontal bar: sort descending
    s = series.sort_values(ascending=True)
    s.plot(kind="barh")
    plt.title(title)
    plt.tight_layout()
    plt.savefig(out_png, dpi=140)
    plt.close()
    return out_png

def plot_pie(labels, sizes, out_png, title=""):
    out_png = ensure_out(out_png)
    plt.figure()
    plt.pie(sizes, labels=labels, autopct="%1.0f%%")
    plt.title(title)
    plt.tight_layout()
    plt.savefig(out_png, dpi=140)
    plt.close()
    return out_png

def plot_line(dates, values, out_png, title=""):
    out_png = ensure_out(out_png)
    plt.figure()
    dates = pd.to_datetime(dates)
    df = pd.DataFrame({"x": dates, "y": values}).sort_values("x")
    plt.plot(df["x"], df["y"])
    plt.title(title)
    plt.tight_layout()
    plt.savefig(out_png, dpi=140)
    plt.close()
    return out_png
