import os
import requests
import pandas as pd
import time


class NSEDataFetcher:
    def __init__(self):
        self.data_sources = {
            "NIFTY_50_CASH": "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%2050",
        }

        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
        }

        # Define the columns for NIFTY_50_CASH
        self.columns = [
            "symbol", "identifier", "open", "dayHigh", "dayLow", "lastPrice", "previousClose",
            "change", "pChange", "ffmc", "yearHigh", "yearLow", "totalTradedVolume",
            "stockIndClosePrice", "totalTradedValue", "lastUpdateTime", "nearWKH", "nearWKL",
            "perChange365d", "date365dAgo", "date30dAgo", "perChange30d", "series"
        ]

        self.session = requests.Session()

    def initialize_session(self):
        """
        Initializes the session by visiting the base NSE website URL to retrieve cookies.
        """
        try:
            base_url = "https://www.nseindia.com"
            response = self.session.get(base_url, headers=self.headers, timeout=10)
            if response.status_code == 200:
                print("Session initialized successfully.")
            else:
                print(f"Failed to initialize session. HTTP {response.status_code}")
        except Exception as e:
            print(f"Error initializing session: {e}")

    def fetch_data(self, url):
        """
        Fetch data from the specified NSE API URL using the initialized session.
        """
        try:
            response = self.session.get(url, headers=self.headers, timeout=10)
            if response.status_code == 200:
                return response.json()  # Return parsed JSON data
            else:
                print(f"Failed to fetch data from {url}. HTTP {response.status_code}")
                return None
        except Exception as e:
            print(f"Error fetching data from {url}: {e}")
            return None

    def process_data(self, data):
        """
        Process the JSON response and map the data to the specified columns.
        """
        rows = []
        records = data.get("data", [])

        for record in records:
            row = [
                record.get("symbol", None),
                record.get("identifier", None),
                record.get("open", None),
                record.get("dayHigh", None),
                record.get("dayLow", None),
                record.get("lastPrice", None),
                record.get("previousClose", None),
                record.get("change", None),
                record.get("pChange", None),
                record.get("ffmc", None),
                record.get("yearHigh", None),
                record.get("yearLow", None),
                record.get("totalTradedVolume", None),
                record.get("stockIndClosePrice", None),
                record.get("totalTradedValue", None),
                record.get("lastUpdateTime", None),
                record.get("nearWKH", None),
                record.get("nearWKL", None),
                record.get("perChange365d", None),
                record.get("date365dAgo", None),
                record.get("date30dAgo", None),
                record.get("perChange30d", None),
                record.get("series", None),
            ]
            rows.append(row)

        return pd.DataFrame(rows, columns=self.columns)

    def save_to_excel(self, data, filename, folder="output"):
        """
        Save processed data to an Excel file.
        """
        if not os.path.exists(folder):
            os.makedirs(folder)
        file_path = os.path.join(folder, f"{filename}.xlsx")
        try:
            data.to_excel(file_path, index=False)
            print(f"Data successfully saved to {file_path}")
        except Exception as e:
            print(f"Error saving data to Excel for {filename}: {e}")

    def run(self):
        """
        Main loop to fetch and process data for NIFTY_50_CASH.
        """
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
    print("Starting NSE Data Fetcher for NIFTY_50_CASH...")
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
