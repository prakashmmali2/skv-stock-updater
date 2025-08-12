import pandas as pd
import yfinance as yf
import re
import time
from datetime import datetime

# === Config ===
FILE_PATH = "SKV Sheet-1.xlsx"   # Input Excel file
OUTPUT_FILE = "SKV_Sheet_1_Updated.csv"  # Output CSV file
DELAY_SEC = 0.3                  # Delay between API calls (avoid rate limits)

# === Load Excel ===
try:
    df = pd.read_excel(FILE_PATH)
except FileNotFoundError:
    raise SystemExit(f"❌ File not found: {FILE_PATH}. Make sure it’s in the repo.")

# === Clean Yahoo Stock Symbols ===
def clean_symbol(sym):
    if pd.isna(sym):
        return None
    sym = str(sym).strip().upper()
    sym = re.sub(r"^\$+", "", sym)         # Remove starting $
    sym = sym.replace("_", "-")            # Replace underscores with dash
    sym = re.sub(r"[^A-Z0-9\-]", "", sym)  # Keep only A-Z, 0-9, dash
    return sym + ".NS"  # NSE format

df["Yahoo Symbol"] = df["Stock Name"].apply(clean_symbol)

# === Fetch Updated Prices ===
new_prices = []
failed_symbols = []

for symbol in df["Yahoo Symbol"]:
    if pd.isna(symbol):
        new_prices.append(None)
        continue
    try:
        ticker = yf.Ticker(symbol)
        hist = ticker.history(period="1d")
        if hist.empty:
            hist = ticker.history(period="5d")
        if not hist.empty:
            last_close = round(hist["Close"].dropna().iloc[-1], 2)
            new_prices.append(last_close)
        else:
            new_prices.append(None)
            failed_symbols.append(symbol)
    except Exception as e:
        print(f"⚠️ Error fetching {symbol}: {e}")
        new_prices.append(None)
        failed_symbols.append(symbol)
    time.sleep(DELAY_SEC)

df["Last Close Price"] = new_prices

# === Save to CSV ===
df.to_csv(OUTPUT_FILE, index=False)
print(f"✅ Updated CSV saved as {OUTPUT_FILE} at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

if failed_symbols:
    print("\n⚠️ Failed to fetch for symbols:")
    for sym in sorted(set(failed_symbols)):
        print(f" - {sym}")
