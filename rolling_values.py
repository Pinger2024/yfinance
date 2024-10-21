from pymongo import MongoClient, UpdateOne
import pandas as pd
from datetime import datetime
import time
import concurrent.futures

# MongoDB connection setup
mongo_uri = 'mongodb://mongodb-9iyq:27017'
client = MongoClient(mongo_uri)
db = client['StockData']
ohlcv_collection = db['ohlcv_data']

def calculate_rs_and_pct_change(data):
    data['daily_pct_change'] = data['close'].pct_change(fill_method=None) * 100
    for window in [63, 126, 189, 252]:
        data[f'RS{window//63}'] = (data['close'].shift(0) - data['close'].shift(window)) / data['close'].shift(window) * 100
    return data

def process_ticker(ticker):
    print(f"Processing ticker: {ticker}", flush=True)
    try:
        # Fetch the OHLCV data for the given ticker
        ohlcv_data = pd.DataFrame(list(ohlcv_collection.find({"ticker": ticker}).sort("date", 1)))

        if ohlcv_data.empty:
            print(f"No data found for ticker: {ticker}", flush=True)
            return

        ohlcv_data['date'] = pd.to_datetime(ohlcv_data['date'])
        ohlcv_data.sort_values(by='date', inplace=True)
        
        # Calculate RS and % change
        ohlcv_data = calculate_rs_and_pct_change(ohlcv_data)
        
        # Prepare bulk update operations
        bulk_operations = []
        for index, row in ohlcv_data.iterrows():
            update_data = {
                "daily_pct_change": row['daily_pct_change'],
                "RS1": row['RS1'],
                "RS2": row['RS2'],
                "RS3": row['RS3'],
                "RS4": row['RS4']
            }
            bulk_operations.append(
                UpdateOne({"_id": row['_id']}, {"$set": update_data})
            )
        
        # Execute bulk update
        if bulk_operations:
            ohlcv_collection.bulk_write(bulk_operations, ordered=False)  # Execute in parallel
        
        print(f"Successfully updated {ticker}", flush=True)
    except Exception as e:
        print(f"Error processing {ticker}: {e}", flush=True)

# Fetch distinct tickers in alphabetical order
tickers = sorted(ohlcv_collection.distinct('ticker'))

print("Script started...", flush=True)

# Use ThreadPoolExecutor for parallel processing
with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
    executor.map(process_ticker, tickers)

print("Processing complete for all tickers.", flush=True)
