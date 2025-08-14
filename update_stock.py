import pandas as pd
import yfinance as yf
import re
import time
from datetime import datetime
import requests

# Constants
CSV_URL = "https://docs.google.com/spreadsheets/d/1kqm-XeSSFBPPSriL78N_pevRY6vHuhmEgRUL1KrwT6s/export?format=csv&gid=1334185550"
INPUT_FILE = "SKV_Sheet-1.csv"
OUTPUT_FILE = "SKV_Sheet_1_Updated.csv"

# Download CSV from Google Sheets
response = requests.get(CSV_URL)
if response.status_code != 200:
    raise Exception(f"❌ Failed to download file from Google Drive (status: {response.status_code})")

# Save the CSV locally for processing
with open(INPUT_FILE, "wb") as f:
    f.write(response.content)

# Read the CSV into pandas DataFrame
df = pd.read_csv(INPUT_FILE)

# === Clean Yahoo Stock Symbols ===
def clean_symbol(sym):
    if not isinstance(sym, str):
        return None
    sym = sym.strip().upper()
    sym = re.sub(r"^\$+", "", sym)
    sym = sym.replace("_", "-")
    sym = re.sub(r"[^A-Z0-9\-]", "", sym)
    return sym + ".NS"  # NSE format

df["Yahoo Symbol"] = df["Stock Name"].apply(clean_symbol)

# === Fetch Updated Prices ===
new_prices = []
highlight = []
failed_symbols = []

for i, row in df.iterrows():
    symbol = row.get("Yahoo Symbol")
    entry = row.get("Entry Price", None)

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
            if pd.notna(entry):
                diff_pct = ((close_price - entry) / entry) * 100
                if diff_pct >= 2.5:
                    highlight.append("GREEN")
                elif diff_pct <= -2.5:
                    highlight.append("RED")
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

    time.sleep(0.3)  # prevent hitting API limits

# Add columns to DataFrame
df["Last Close Price"] = new_prices
df["Highlight"] = highlight

# Save updated DataFrame to CSV
df.to_csv(OUTPUT_FILE, index=False)

print(f"✅ Updated CSV saved at {datetime.now()}")

if failed_symbols:
    print("\n⚠️ Failed to fetch prices for:")
    for sym in sorted(set(failed_symbols)):
        print(" -", sym)
