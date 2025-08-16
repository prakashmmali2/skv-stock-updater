import pandas as pd
import yfinance as yf
import re
import time
from datetime import datetime
import urllib.error

# Public Google Sheet CSV export URL
GOOGLE_SHEET_CSV_URL = "https://docs.google.com/spreadsheets/d/1kqm-XeSSFBPPSriL78N_pevRY6vHuhmEgRUL1KrwT6s/export?format=csv&gid=1334185550"
OUTPUT_FILE = "SKV_Sheet_1_Updated.csv"

# === Load the Google Sheet with retry logic ===
MAX_RETRIES = 5
for attempt in range(1, MAX_RETRIES + 1):
    try:
        df = pd.read_csv(GOOGLE_SHEET_CSV_URL)
        if df.empty:
            raise ValueError("Google Sheet returned empty data")
        print("✅ Loaded data from Google Sheets")
        break
    except (urllib.error.URLError, Exception) as e:
        print(f"⚠️ Attempt {attempt} failed: {e}")
        if attempt == MAX_RETRIES:
            raise Exception(f"❌ Failed to load data from Google Sheets after {MAX_RETRIES} attempts: {e}")
        time.sleep(5)  # wait before retrying

# === Clean Yahoo Stock Symbols ===
def clean_symbol(sym):
    if not isinstance(sym, str) or not sym.strip():
        return None
    sym = sym.strip().upper()
    sym = re.sub(r"^\$+", "", sym)
    sym = sym.replace("_", "-")
    sym = re.sub(r"[^A-Z0-9\-]", "", sym)
    if not sym.endswith(".NS"):
        sym += ".NS"
    return sym

df["Yahoo Symbol"] = df["Stock Name"].apply(clean_symbol)

# === Calculate Diff and Tgt in-place ===
df["Diff"] = df.apply(
    lambda row: row["Entry Price"] - row["Stop Loss"]
    if pd.notna(row["Entry Price"]) and pd.notna(row["Stop Loss"]) else None,
    axis=1
)

df["Tgt"] = df.apply(
    lambda row: round(row["Entry Price"] + (row["Diff"] * 5), 2) 
    if pd.notna(row["Diff"]) and pd.notna(row["Entry Price"]) 
    else None,
    axis=1
)

# === Fetch Current Prices ===
new_prices = []
highlight_list = []
highligh_list = []
failed_symbols = []

for _, row in df.iterrows():
    symbol = row.get("Yahoo Symbol")
    entry = row.get("Entry Price")

    if pd.isna(symbol):
        new_prices.append(None)
        highlight_list.append("")
        highligh_list.append("")
        continue

    try:
        ticker = yf.Ticker(symbol)
        hist = ticker.history(period="1d")
        if hist.empty:
            hist = ticker.history(period="5d")

        if not hist.empty:
            close_price = round(hist["Close"].dropna().iloc[-1], 2)
            new_prices.append(close_price)

            # Apply same logic to both Highlight & Highligh
            if pd.notna(entry):
                diff_pct = ((close_price - entry) / entry) * 100
                if 0 <= diff_pct <= 2.5:
                    highlight_list.append("Green")
                    highligh_list.append("Green")
                elif -2.5 <= diff_pct <= 0:
                    highlight_list.append("Red")
                    highligh_list.append("Red")
                else:
                    highlight_list.append("")
                    highligh_list.append("")
            else:
                highlight_list.append("")
                highligh_list.append("")
        else:
            new_prices.append(None)
            highlight_list.append("No data")
            highligh_list.append("No data")
            failed_symbols.append(symbol)

    except Exception:
        new_prices.append(None)
        highlight_list.append("Error")
        highligh_list.append("Error")
        failed_symbols.append(symbol)

    time.sleep(0.3)

# === Update both columns ===
df["Last Close Price"] = new_prices
df["Highlight"] = highlight_list
df["Highligh"] = highligh_list

# Guarantee columns exist
if "Highlight" not in df.columns:
    df["Highlight"] = ""
if "Highligh" not in df.columns:
    df["Highligh"] = ""

# Save results
df.to_csv(OUTPUT_FILE, index=False)
print(f"✅ Updated CSV saved at {datetime.now()}")

# Print failed fetches
if failed_symbols:
    print("\n⚠️ Failed to fetch prices for:")
    for sym in sorted(set(failed_symbols)):
        print(" -", sym)
