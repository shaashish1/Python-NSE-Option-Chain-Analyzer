import os
import requests
import pandas as pd
import time


class NSEDataFetcher:
    def __init__(self):
        self.data_sources = {
            "BANKNIFTY_FUTURES": "https://www.nseindia.com/api/quote-derivative?symbol=BANKNIFTY",
            "NIFTY_FUTURES": "https://www.nseindia.com/api/quote-derivative?symbol=NIFTY",
        }

        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
        }

        self.columns = [
            "metadata.instrumentType", "metadata.expiryDate", "metadata.optionType", "metadata.strikePrice",
            "metadata.identifier", "metadata.openPrice", "metadata.highPrice", "metadata.lowPrice",
            "metadata.closePrice", "metadata.prevClose", "metadata.lastPrice", "metadata.change", "metadata.pChange",
            "metadata.numberOfContractsTraded", "metadata.totalTurnover", "underlyingValue", "volumeFreezeQuantity",
            "marketDeptOrderBook.totalBuyQuantity", "marketDeptOrderBook.totalSellQuantity",
            "marketDeptOrderBook.bid.price", "marketDeptOrderBook.bid.quantity", "marketDeptOrderBook.ask.price",
            "marketDeptOrderBook.ask.quantity", "carryOfCost.price.bestBuy", "carryOfCost.price.bestSell",
            "carryOfCost.price.lastPrice", "carryOfCost.carry.bestBuy", "carryOfCost.carry.bestSell",
            "carryOfCost.carry.lastPrice", "marketDeptOrderBook.tradeInfo.tradedVolume",
            "marketDeptOrderBook.tradeInfo.value", "marketDeptOrderBook.tradeInfo.vmap",
            "marketDeptOrderBook.tradeInfo.premiumTurnover", "marketDeptOrderBook.tradeInfo.openInterest",
            "marketDeptOrderBook.tradeInfo.changeinOpenInterest", "marketDeptOrderBook.tradeInfo.pchangeinOpenInterest",
            "marketDeptOrderBook.tradeInfo.marketLot", "marketDeptOrderBook.otherInfo.settlementPrice",
            "marketDeptOrderBook.otherInfo.dailyvolatility", "marketDeptOrderBook.otherInfo.annualisedVolatility",
            "marketDeptOrderBook.otherInfo.impliedVolatility", "marketDeptOrderBook.otherInfo.clientWisePositionLimits",
            "marketDeptOrderBook.otherInfo.marketWidePositionLimits"
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
        Process fetched data into a structured list of rows.
        """
        rows = []
        if "stocks" in data:
            for item in data["stocks"]:
                market_dept = item.get("marketDeptOrderBook", {})
                if isinstance(market_dept, list):
                    market_dept = market_dept[0] if market_dept else {}

                # Extract bid and ask details safely
                bid = market_dept.get("bid", {})
                if isinstance(bid, list):  # If bid is a list, take the first element
                    bid = bid[0] if bid else {}
                ask = market_dept.get("ask", {})
                if isinstance(ask, list):  # If ask is a list, take the first element
                    ask = ask[0] if ask else {}

                carry_of_cost = market_dept.get("carryOfCost", {})
                carry_price = carry_of_cost.get("price", {})
                carry_data = carry_of_cost.get("carry", {})

                rows.append([
                    item["metadata"].get("instrumentType", None),
                    item["metadata"].get("expiryDate", None),
                    item["metadata"].get("optionType", None),
                    item["metadata"].get("strikePrice", None),
                    item["metadata"].get("identifier", None),
                    item["metadata"].get("openPrice", None),
                    item["metadata"].get("highPrice", None),
                    item["metadata"].get("lowPrice", None),
                    item["metadata"].get("closePrice", None),
                    item["metadata"].get("prevClose", None),
                    item["metadata"].get("lastPrice", None),
                    item["metadata"].get("change", None),
                    item["metadata"].get("pChange", None),
                    item["metadata"].get("numberOfContractsTraded", None),
                    item["metadata"].get("totalTurnover", None),
                    data.get("underlyingValue", None),
                    item.get("volumeFreezeQuantity", None),
                    market_dept.get("totalBuyQuantity", None),
                    market_dept.get("totalSellQuantity", None),
                    bid.get("price", None),
                    bid.get("quantity", None),
                    ask.get("price", None),
                    ask.get("quantity", None),
                    carry_price.get("bestBuy", None),
                    carry_price.get("bestSell", None),
                    carry_price.get("lastPrice", None),
                    carry_data.get("bestBuy", None),
                    carry_data.get("bestSell", None),
                    carry_data.get("lastPrice", None),
                    market_dept.get("tradeInfo", {}).get("tradedVolume", None),
                    market_dept.get("tradeInfo", {}).get("value", None),
                    market_dept.get("tradeInfo", {}).get("vmap", None),
                    market_dept.get("tradeInfo", {}).get("premiumTurnover", None),
                    market_dept.get("tradeInfo", {}).get("openInterest", None),
                    market_dept.get("tradeInfo", {}).get("changeinOpenInterest", None),
                    market_dept.get("tradeInfo", {}).get("pchangeinOpenInterest", None),
                    market_dept.get("tradeInfo", {}).get("marketLot", None),
                    market_dept.get("otherInfo", {}).get("settlementPrice", None),
                    market_dept.get("otherInfo", {}).get("dailyvolatility", None),
                    market_dept.get("otherInfo", {}).get("annualisedVolatility", None),
                    market_dept.get("otherInfo", {}).get("impliedVolatility", None),
                    market_dept.get("otherInfo", {}).get("clientWisePositionLimits", None),
                    market_dept.get("otherInfo", {}).get("marketWidePositionLimits", None),
                ])
        return rows

    def save_to_excel(self, rows, filename):
        """
        Save processed rows to an Excel file.
        """
        df = pd.DataFrame(rows, columns=self.columns)
        output_dir = "output"
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        file_path = os.path.join(output_dir, f"{filename}.xlsx")
        if not df.empty:
            df.to_excel(file_path, index=False)
            print(f"Data saved to {file_path}")
        else:
            print(f"No data to save for {filename}")

    def run(self):
        """
        Main loop to fetch and save data for each source continuously.
        """
        while True:
            self.initialize_session()

            for key, url in self.data_sources.items():
                print(f"Fetching data for {key}...")
                raw_data = self.fetch_data(url)
                if raw_data:
                    rows = self.process_data(raw_data)
                    self.save_to_excel(rows, key)
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
