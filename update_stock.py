import pandas as pd
import yfinance as yf
import re
import time
from datetime import datetime

# Public Google Sheet CSV export URL
GOOGLE_SHEET_CSV_URL = "https://docs.google.com/spreadsheets/d/1kqm-XeSSFBPPSriL78N_pevRY6vHuhmEgRUL1KrwT6s/export?format=csv&gid=1334185550"
OUTPUT_FILE = "SKV_Sheet_1_Updated.csv"

# Load the Google Sheet
try:
    df = pd.read_csv(GOOGLE_SHEET_CSV_URL)
    print("✅ Loaded data from Google Sheets")
except Exception as e:
    raise Exception(f"❌ Failed to load data from Google Sheets: {e}")

# === Clean Yahoo Stock Symbols ===
def clean_symbol(sym):
    if not isinstance(sym, str):
        return None
    sym = sym.strip().upper()
    sym = re.sub(r"^\$+", "", sym)
    sym = sym.replace("_", "-")
    sym = re.sub(r"[^A-Z0-9\-]", "", sym)
    return sym + ".NS"

df["Yahoo Symbol"] = df["Stock Name"].apply(clean_symbol)

# === Calculate Diff and Tgt in-place ===
df["Diff"] = df.apply(
    lambda row: row["Entry Price"] - row["Stop Loss"] 
    if pd.notna(row["Entry Price"]) and pd.notna(row["Stop Loss"]) else None,
    axis=1
)

df["Tgt"] = df["Diff"].apply(lambda x: round(x * 5, 2) if pd.notna(x) else None)

# === Fetch Current Prices & Apply New Highlight Logic ===
new_prices = []
highlight = []
failed_symbols = []

for _, row in df.iterrows():
    symbol = row.get("Yahoo Symbol")
    entry = row.get("Entry Price")

    if pd.isna(symbol):
        new_prices.append(None)
        highlight.append("")
        continue

    try:
        ticker = yf.Ticker(symbol)
        hist = ticker.history(period="1d")
        if hist.empty:
            hist = ticker.history(period="5d")
        if not hist.empty:
            close_price = round(hist["Close"].dropna().iloc[-1], 2)
            new_prices.append(close_price)

            # New Highlight logic: -2.5% to 0 = RED, 0 to +2.5% = GREEN
            if pd.notna(entry) and entry != 0:
                diff_pct = ((close_price - entry) / entry) * 100
                if -2.5 <= diff_pct < 0:
                    highlight.append("RED")
                elif 0 < diff_pct <= 2.5:
                    highlight.append("GREEN")
                else:
                    highlight.append("")
            else:
                highlight.append("")
        else:
            new_prices.append(None)
            highlight.append("No data")
            failed_symbols.append(symbol)
    except Exception:
        new_prices.append(None)
        highlight.append("Error")
        failed_symbols.append(symbol)

    time.sleep(0.3)

# Store highlight column in DataFrame
df["Highlight"] = highlight

# Remove last column (Highlight) before saving
df.drop(columns=["Highlight"]).to_csv(OUTPUT_FILE, index=False)

print(f"✅ Updated CSV saved at {datetime.now()}")

if failed_symbols:
    print("\n⚠️ Failed to fetch prices for:")
    for sym in sorted(set(failed_symbols)):
        print(" -", sym)
