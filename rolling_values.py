from pymongo import MongoClient
import pandas as pd
from datetime import datetime
import time

# MongoDB connection setup
mongo_uri = 'mongodb://mongodb-9iyq:27017'
client = MongoClient(mongo_uri)
db = client['StockData']
ohlcv_collection = db['ohlcv_data']

# Function to calculate rolling returns and daily percentage change
def calculate_rs_and_pct_change(data, window_size):
    rolling_returns = (data['close'].shift(0) - data['close'].shift(window_size)) / data['close'].shift(window_size) * 100
    return rolling_returns

# Fetch distinct tickers in alphabetical order
tickers = sorted(ohlcv_collection.distinct('ticker'))

# Process tickers in batches of 10
batch_size = 10

for i in range(0, len(tickers), batch_size):
    batch_tickers = tickers[i:i + batch_size]

    for ticker in batch_tickers:
        print(f"Processing ticker: {ticker}")
        try:
            # Fetch the OHLCV data for the specific ticker
            ohlcv_data = pd.DataFrame(list(ohlcv_collection.find({"ticker": ticker}).sort("date", 1)))

            if ohlcv_data.empty:
                print(f"No data found for ticker: {ticker}")
                continue

            # Ensure we have data sorted by date
            ohlcv_data['date'] = pd.to_datetime(ohlcv_data['date'])
            ohlcv_data.sort_values(by='date', inplace=True)

            # Calculate the daily percentage change
            ohlcv_data['daily_pct_change'] = ohlcv_data['close'].pct_change() * 100

            # Calculate rolling returns for RS1, RS2, RS3, and RS4
            ohlcv_data['RS1'] = calculate_rs_and_pct_change(ohlcv_data, 63)  # 63-day rolling return
            ohlcv_data['RS2'] = calculate_rs_and_pct_change(ohlcv_data, 126) # 126-day rolling return
            ohlcv_data['RS3'] = calculate_rs_and_pct_change(ohlcv_data, 189) # 189-day rolling return
            ohlcv_data['RS4'] = calculate_rs_and_pct_change(ohlcv_data, 252) # 252-day rolling return

            # Update the database with the new fields: daily_pct_change, RS1, RS2, RS3, RS4
            for index, row in ohlcv_data.iterrows():
                _id = row['_id']  # MongoDB record identifier
                update_data = {
                    "daily_pct_change": row['daily_pct_change'],
                    "RS1": row['RS1'],
                    "RS2": row['RS2'],
                    "RS3": row['RS3'],
                    "RS4": row['RS4']
                }
                ohlcv_collection.update_one({"_id": _id}, {"$set": update_data})

            print(f"Successfully updated {ticker}")
        except Exception as e:
            print(f"Error processing {ticker}: {e}")

    # Add a delay to avoid overloading the server (optional)
    time.sleep(5)  # Adjust the sleep time as necessary

print("Processing complete for all tickers.")
