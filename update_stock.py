import os
import requests
import pandas as pd
import yfinance as yf
import re
import time
from datetime import datetime

# === Configuration ===
GDRIVE_URL = "https://docs.google.com/spreadsheets/d/1kqm-XeSSFBPPSriL78N_pevRY6vHuhmEgRUL1KrwT6s/export?format=xlsx&gid=1334185550"
EXCEL_FILE = "temp_download.xlsx"
INPUT_FILE = "SKV Sheet-1.csv"
OUTPUT_FILE = "SKV_Sheet_1_Updated.csv"

# === Step 1: Download Excel from Google Drive ===
response = requests.get(GDRIVE_URL)
if response.status_code != 200:
    raise Exception(f"❌ Failed to download file from Google Drive (status: {response.status_code})")

with open(EXCEL_FILE, "wb") as f:
    f.write(response.content)

print("✅ Downloaded latest sheet from Google Drive")

# === Step 2: Read Excel and convert to CSV ===
df = pd.read_excel(EXCEL_FILE)
df.to_csv(INPUT_FILE, index=False)
print(f"✅ Saved downloaded Excel as CSV → {INPUT_FILE}")

# === Step 3: Clean stock symbols ===
def clean_symbol(sym):
    if isinstance(sym, str):
        sym = sym.strip().upper()
        sym = re.sub(r"^\$+", "", sym)
        sym = sym.replace("_", "-")
        sym = re.sub(r"[^A-Z0-9\-]", "", sym)
        return sym + ".NS"
    return None

df["Yahoo Symbol"] = df["Stock Name"].apply(clean_symbol)

# === Step 4: Fetch prices ===
new_prices, highlights, failures = [], [], []

for i, row in df.iterrows():
    symbol = row.get("Yahoo Symbol")
    entry = row.get("Entry Price", None)

    if pd.isna(symbol):
        new_prices.append(None)
        highlights.append("")
        continue

    try:
        hist = yf.Ticker(symbol).history(period="1d")
        if hist.empty:
            hist = yf.Ticker(symbol).history(period="5d")

        close_price = round(hist["Close"].dropna().iloc[-1], 2) if not hist.empty else None
        new_prices.append(close_price)

        if pd.notna(entry) and close_price is not None:
            diff = (close_price - entry) / entry * 100
            highlights.append("GREEN" if diff >= 2.5 else "RED" if diff <= -2.5 else "")
        else:
            highlights.append("")
    except Exception as e:
        new_prices.append(None)
        highlights.append("Error")
        failures.append(symbol)

    time.sleep(0.3)

df["Last Close Price"] = new_prices
df["Highlight"] = highlights

# === Step 5: Save updated CSV ===
df.to_csv(OUTPUT_FILE, index=False)
print(f"✅ Output saved: {OUTPUT_FILE} at {datetime.now()}")

if failures:
    print("\n⚠️ Failed to fetch:")
    for sym in sorted(set(failures)):
        print(" -", sym)
