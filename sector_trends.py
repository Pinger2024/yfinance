import yfinance as yf
from pymongo import MongoClient
import pandas as pd
import numpy as np
from datetime import datetime

# MongoDB connection
client = MongoClient("mongodb://mongodb-9iyq:27017")
db = client['StockData']
ohlcv_collection = db['ohlcv_data']
indicators_collection = db['indicators']
historical_rs_collection = db['historical_rs_scores']  # New collection for historical RS scores

# List of tickers
tickers = ohlcv_collection.distinct('ticker')

# Fetch existing RS scores from the indicators collection and store them as historical entries
def store_historical_rs_scores():
    today = pd.to_datetime('today')
    
    for ticker in tickers:
        # Fetch the most recent RS score from the indicators collection
        indicator_data = indicators_collection.find_one({"ticker": ticker})
        
        if indicator_data and 'rs_score' in indicator_data:
            rs_score = indicator_data['rs_score']
            sector = indicator_data.get('sector', 'Unknown')  # Fetch the sector if available
            
            # Prepare the document to store in the historical RS collection
            historical_entry = {
                "ticker": ticker,
                "date": today,
                "rs_score": rs_score,
                "sector": sector
            }
            
            # Insert into the historical_rs_scores collection
            historical_rs_collection.insert_one(historical_entry)
            print(f"Stored historical RS score for {ticker}: {rs_score} on {today}")
        else:
            print(f"No RS score found for {ticker}")

# Run the RS score storage process
if __name__ == "__main__":
    store_historical_rs_scores()
