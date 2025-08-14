import pandas as pd
import yfinance as yf
import re
import time
from datetime import datetime
import os

file_path = "SKV Sheet-1.csv"
output_file = "SKV_Sheet_1_Updated.csv"

if not os.path.exists(file_path):
    raise FileNotFoundError(f"❌ CSV file '{file_path}' not found in repo")

# Read the CSV correctly
df = pd.read_csv(file_path)

# === Clean Yahoo Stock Symbols ===
def clean_symbol(sym):
    if not isinstance(sym, str):
        return None
    sym = sym.strip().upper()
    sym = re.sub(r"^\$+", "", sym)
    sym = sym.replace("_", "-")
    sym = re.sub(r"[^A-Z0-9\-]", "", sym)
    return sym + ".NS"  # For NSE

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
    except Exception as e:
        new_prices.append(None)
        highlight.append("Error")
        failed_symbols.append(symbol)

    time.sleep(0.3)  # prevent rate limit

# Add columns
df["Last Close Price"] = new_prices
df["Highlight"] = highlight

# Save output
df.to_csv(output_file, index=False)

print(f"✅ Updated CSV saved at {datetime.now()}")

if failed_symbols:
    print("\n⚠️ Failed to fetch prices for:")
    for sym in sorted(set(failed_symbols)):
        print(" -", sym)
