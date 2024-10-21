from pymongo import MongoClient
import pandas as pd
import numpy as np

# MongoDB connection setup
mongo_uri = 'mongodb://mongodb-9iyq:27017'
client = MongoClient(mongo_uri, connectTimeoutMS=60000, socketTimeoutMS=60000)
db = client['StockData']
ohlcv_collection = db['ohlcv_data']

# Ticker to analyze
ticker = "TSLA"

# Fetch all OHLCV data for the given ticker and sort by date
ohlcv_data = pd.DataFrame(list(ohlcv_collection.find({"ticker": ticker}).sort("date", 1)))

# If there are no records, exit
if ohlcv_data.empty:
    print(f"No OHLCV data found for ticker {ticker}.")
    exit()

# Calculate rolling returns
for index, row in ohlcv_data.iterrows():
    # Define RS1, RS2, RS3, RS4 (in trading days)
    rolling_periods = {
        "RS1": 63,
        "RS2": 126,
        "RS3": 189,
        "RS4": 252
    }

    for rs_label, period in rolling_periods.items():
        # Ensure we have enough data for the rolling period
        if index >= period:
            # Calculate the rolling return as percentage change between current close and start close
            start_price = ohlcv_data.iloc[index - period]['close']
            end_price = row['close']
            rolling_return = ((end_price - start_price) / start_price) * 100

            # Update the rolling return in the dataframe
            ohlcv_data.at[index, rs_label] = rolling_return
        else:
            # Not enough data to calculate rolling return for the period
            ohlcv_data.at[index, rs_label] = None

# Update MongoDB with the calculated RS values
for index, row in ohlcv_data.iterrows():
    update_fields = {
        "RS1": row.get("RS1"),
        "RS2": row.get("RS2"),
        "RS3": row.get("RS3"),
        "RS4": row.get("RS4")
    }
    ohlcv_collection.update_one({"_id": row["_id"]}, {"$set": update_fields})

print(f"Updated RS values for ticker {ticker}.")
