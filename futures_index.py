import requests
import pandas as pd
import time

# Config
class config:
    IBKR_BASE_URL = "http://localhost:5000"  # Update if needed

SYMBOLS = ["ZT", "Z3N", "ZF", "ZN", "TN"]
symbols_csv = ",".join(SYMBOLS)

def fetch_futures_chains(symbols_csv):
    url = config.IBKR_BASE_URL + f"/v1/api/trsrv/futures?symbols={symbols_csv}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Error fetching futures chain: {e}")
        return {}

def parse_futures_response(response_json):
    records = []
    for symbol, contracts in response_json.items():
        for contract in contracts:
            records.append({
                "requested_symbol": symbol,
                "conid": contract.get("conid"),
                "symbol": contract.get("symbol"),
                "description": contract.get("description"),
                "expiry": contract.get("expiry"),
                "tradingClass": contract.get("tradingClass"),
                "currency": contract.get("currency"),
                "exchange": contract.get("exchange"),
                "underlyingConid": contract.get("underlyingConid")
            })
    return pd.DataFrame(records)

def main():
    print(f"📡 Requesting futures chains for: {symbols_csv}")
    raw_data = fetch_futures_chains(symbols_csv)
    FUTURES = parse_futures_response(raw_data)

    print(f"\n📊 Retrieved {len(FUTURES)} contract(s).")
    print(FUTURES.head())

    return FUTURES

if __name__ == "__main__":
    FUTURES = main()
