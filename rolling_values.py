from pymongo import MongoClient
import pandas as pd
from datetime import datetime

# MongoDB connection setup (using your existing configuration)
mongo_uri = 'mongodb://mongodb-9iyq:27017'
client = MongoClient(mongo_uri, connectTimeoutMS=60000, socketTimeoutMS=60000)
db = client['StockData']
ohlcv_collection = db['ohlcv_data']

# Define the stock ticker (TSLA for now)
ticker = 'TSLA'

# Fetch OHLCV data from MongoDB
ohlcv_data = pd.DataFrame(list(ohlcv_collection.find({"ticker": ticker}, {"_id": 0, "date": 1, "close": 1})))

# Ensure the 'date' field is in datetime format
ohlcv_data['date'] = pd.to_datetime(ohlcv_data['date'])
ohlcv_data.set_index('date', inplace=True)

# Sort the data by date
ohlcv_data.sort_index(inplace=True)

# Calculate rolling returns (63-day, 126-day, 189-day, 252-day)
ohlcv_data['RS1'] = ohlcv_data['close'].pct_change(63).add(1).cumprod() - 1
ohlcv_data['RS2'] = ohlcv_data['close'].pct_change(126).add(1).cumprod() - 1
ohlcv_data['RS3'] = ohlcv_data['close'].pct_change(189).add(1).cumprod() - 1
ohlcv_data['RS4'] = ohlcv_data['close'].pct_change(252).add(1).cumprod() - 1

# Filter out rows where rolling calculations are not available
ohlcv_data.dropna(subset=['RS1', 'RS2', 'RS3', 'RS4'], inplace=True)

# Loop through data and store in MongoDB
for index, row in ohlcv_data.iterrows():
    # Create a document for each day
    document = {
        "ticker": ticker,
        "date": index,
        "RS1": row['RS1'],
        "RS2": row['RS2'],
        "RS3": row['RS3'],
        "RS4": row['RS4']
    }
    
    # Update the document if the date already exists, or insert it
    ohlcv_collection.update_one(
        {"ticker": ticker, "date": index},
        {"$set": document},
        upsert=True
    )

print("RS values have been stored in MongoDB.")
