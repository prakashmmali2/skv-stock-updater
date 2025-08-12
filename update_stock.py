{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 9,
   "id": "0a94f8ee-8541-4c5f-85ff-bd76f3870e5e",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "✅ CSV saved at 2025-08-12 08:26:42.622490 — open in Excel and apply filter on Last Close Price for ±2.5% from Entry Price.\n"
     ]
    }
   ],
   "source": [
    "import pandas as pd\n",
    "import yfinance as yf\n",
    "import re\n",
    "import time\n",
    "from datetime import datetime\n",
    "\n",
    "# === Load Excel ===\n",
    "file_path = \"SKV Sheet-1.xlsx\"  # Uploaded file name\n",
    "df = pd.read_excel(file_path)\n",
    "\n",
    "# === Clean Yahoo Stock Symbols ===\n",
    "def clean_symbol(sym):\n",
    "    if not isinstance(sym, str):\n",
    "        return None\n",
    "    sym = sym.strip().upper()\n",
    "    sym = re.sub(r\"^\\$+\", \"\", sym)\n",
    "    sym = sym.replace(\"_\", \"-\")\n",
    "    sym = re.sub(r\"[^A-Z0-9\\-]\", \"\", sym)\n",
    "    return sym + \".NS\"  # NSE format\n",
    "\n",
    "df[\"Yahoo Symbol\"] = df[\"Stock Name\"].apply(clean_symbol)\n",
    "\n",
    "# === Fetch Updated Prices ===\n",
    "new_prices = []\n",
    "failed_symbols = []\n",
    "\n",
    "for symbol in df[\"Yahoo Symbol\"]:\n",
    "    if pd.isna(symbol):\n",
    "        new_prices.append(None)\n",
    "        continue\n",
    "    try:\n",
    "        ticker = yf.Ticker(symbol)\n",
    "        hist = ticker.history(period=\"1d\")\n",
    "        if hist.empty:\n",
    "            hist = ticker.history(period=\"5d\")\n",
    "        if not hist.empty:\n",
    "            last_close = round(hist[\"Close\"].dropna().iloc[-1], 2)\n",
    "            new_prices.append(last_close)\n",
    "        else:\n",
    "            new_prices.append(None)\n",
    "            failed_symbols.append(symbol)\n",
    "    except Exception:\n",
    "        new_prices.append(None)\n",
    "        failed_symbols.append(symbol)\n",
    "    time.sleep(0.3)\n",
    "\n",
    "df[\"Last Close Price\"] = new_prices\n",
    "\n",
    "# === Save to CSV (ready for Excel filtering) ===\n",
    "output_file = \"SKV_Sheet_1_Updated.csv\"\n",
    "df.to_csv(output_file, index=False)\n",
    "\n",
    "print(f\"✅ CSV saved at {datetime.now()} — open in Excel and apply filter on Last Close Price for ±2.5% from Entry Price.\")\n",
    "\n",
    "if failed_symbols:\n",
    "    print(\"\\n⚠️ Failed to fetch:\")\n",
    "    for sym in sorted(set(failed_symbols)):\n",
    "        print(\" -\", sym)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "27d1d5d6-41ce-451a-846c-048f716c5a3f",
   "metadata": {},
   "outputs": [],
   "source": []
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3 (ipykernel)",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.12.4"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 5
}
