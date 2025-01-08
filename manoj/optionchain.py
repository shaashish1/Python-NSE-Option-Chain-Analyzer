import os
import requests
import pandas as pd
import time


class NSEDataFetcher:
    def __init__(self):
        self.data_sources = {
            "BANKNIFTY_OPTIONS": "https://www.nseindia.com/api/option-chain-indices?symbol=BANKNIFTY",
            "NIFTY_OPTIONS": "https://www.nseindia.com/api/option-chain-indices?symbol=NIFTY",
        }

        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
        }

        self.columns = [
            "strikePrice", "expiryDate", "optionType", "underlying", "Product", "Ticker", "Expiry", "OptionType", 
            "StrikePrice", "openInterest", "changeinOpenInterest", "pchangeinOpenInterest", "totalTradedVolume", 
            "impliedVolatility", "lastPrice", "change", "pChange", "totalBuyQuantity", "totalSellQuantity", 
            "bidQty", "bidprice", "askQty", "askPrice", "underlyingValue", "timestamp", "totOI", "totVol"
        ]

        # Include CE and PE suffix for CE/PE-specific data
        self.columns_ce = [col + "_CE" for col in self.columns]
        self.columns_pe = [col + "_PE" for col in self.columns]

        self.session = requests.Session()

    def initialize_session(self):
        try:
            base_url = "https://www.nseindia.com/option-chain"
            response = self.session.get(base_url, headers=self.headers, timeout=10)
            if response.status_code == 200:
                print("Session initialized successfully.")
            else:
                print(f"Failed to initialize session. HTTP {response.status_code}")
        except Exception as e:
            print(f"Error initializing session: {e}")

    def fetch_data(self, url):
        try:
            response = self.session.get(url, headers=self.headers, timeout=10)
            if response.status_code == 200:
                return response.json()
            else:
                print(f"Failed to fetch data from {url}. HTTP {response.status_code}")
                return None
        except Exception as e:
            print(f"Error fetching data from {url}: {e}")
            return None

    def process_data(self, data):
        """
        Process fetched data to extract relevant columns for CE and PE data.
        """
        rows = []
        records = data.get("records", {}).get("data", [])
        tot_oi = data.get("records", {}).get("totOI", None)
        tot_vol = data.get("records", {}).get("totVol", None)
        timestamp = data.get("records", {}).get("timestamp", None)

        for record in records:
            strike_price = record.get("strikePrice")
            expiry_date = record.get("expiryDate")

            for option_type, option_data in {"CE": record.get("CE", {}), "PE": record.get("PE", {})}.items():
                # Extract identifier fields
                identifier = option_data.get("identifier", "")
                identifier_parts = identifier.split(" ")
                product, ticker, expiry, option_type_id, strike = (
                    identifier_parts[0] if len(identifier_parts) > 0 else None,
                    identifier_parts[1] if len(identifier_parts) > 1 else None,
                    identifier_parts[2] if len(identifier_parts) > 2 else None,
                    identifier_parts[3] if len(identifier_parts) > 3 else None,
                    identifier_parts[4] if len(identifier_parts) > 4 else None,
                )

                row = [
                    strike_price,
                    expiry_date,
                    option_type,
                    option_data.get("underlying", None),
                    product,
                    ticker,
                    expiry,
                    option_type_id,
                    strike,
                    option_data.get("openInterest", None),
                    option_data.get("changeinOpenInterest", None),
                    option_data.get("pchangeinOpenInterest", None),
                    option_data.get("totalTradedVolume", None),
                    option_data.get("impliedVolatility", None),
                    option_data.get("lastPrice", None),
                    option_data.get("change", None),
                    option_data.get("pChange", None),
                    option_data.get("totalBuyQuantity", None),
                    option_data.get("totalSellQuantity", None),
                    option_data.get("bidQty", None),
                    option_data.get("bidprice", None),
                    option_data.get("askQty", None),
                    option_data.get("askPrice", None),
                    option_data.get("underlyingValue", None),
                    timestamp,
                    tot_oi,
                    tot_vol,
                ]
                rows.append(row)

        return pd.DataFrame(rows, columns=self.columns_ce if option_type == "CE" else self.columns_pe)

    def save_to_excel(self, data, filename, folder="output"):
        if not os.path.exists(folder):
            os.makedirs(folder)
        file_path = os.path.join(folder, f"{filename}.xlsx")
        try:
            data.to_excel(file_path, index=False)
            print(f"Data successfully saved to {file_path}")
        except Exception as e:
            print(f"Error saving data to Excel for {filename}: {e}")

    def run(self):
        while True:
            self.initialize_session()

            for key, url in self.data_sources.items():
                print(f"Fetching data for {key}...")
                raw_data = self.fetch_data(url)
                if raw_data:
                    processed_data = self.process_data(raw_data)
                    if not processed_data.empty:
                        self.save_to_excel(processed_data, filename=key)
                    else:
                        print(f"No tabular data available for {key}.")
                else:
                    print(f"Failed to fetch data for {key}.")
            print("Waiting 60 seconds before the next fetch...")
            time.sleep(60)


if __name__ == "__main__":
    print("Starting NSE Data Fetcher...")
    fetcher = NSEDataFetcher()
    fetcher.run()

# All imports and class definitions above this line

def main():
    # Create an instance of the class and call the run method
    fetcher = NSEDataFetcher()
    fetcher.run()

# Ensure this is at the end
if __name__ == "__main__":
    main()

