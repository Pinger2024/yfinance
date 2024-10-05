import yfinance as yf
from pymongo import MongoClient
import pandas as pd
import numpy as np
from datetime import datetime

# MongoDB connection (hardcoded)
client = MongoClient("mongodb://mongodb-9iyq:27017")
db = client['StockData']
ohlcv_collection = db['ohlcv_data']
sector_trends_collection = db['sector_trends']

# Fetch distinct dates to process
dates = ohlcv_collection.distinct("date")

# Process each date to calculate average RS score for each sector
def calculate_sector_trends():
    print("Starting sector trend calculation script.")
    
    for date in dates:
        print(f"Processing for date: {date}")
        # Fetch data for that date, excluding sectors marked as "Unknown"
        date_data = list(ohlcv_collection.find({"date": date, "sector": {"$ne": "Unknown"}}))

        # Create a DataFrame
        df = pd.DataFrame(date_data)

        if not df.empty:
            # Group by sector and calculate average RS score
            sector_trends = df.groupby("sector").agg({"rs_score": "mean"}).reset_index()

            print(f"Found {len(sector_trends)} sectors for date: {date}")

            for _, row in sector_trends.iterrows():
                sector = row['sector']
                rs_score = row['rs_score']

                # Store the sector trend data for that date
                sector_data = {
                    "sector": sector,
                    "date": date,
                    "avg_rs_score": rs_score
                }

                sector_trends_collection.update_one(
                    {"sector": sector, "date": date},
                    {"$set": sector_data},
                    upsert=True
                )
                print(f"Storing data for sector: {sector}, RS score: {rs_score}")
        else:
            print(f"No valid data for date: {date} or all sectors are 'Unknown'.")

        print(f"Finished processing for {date}. Took {datetime.now()}.")

if __name__ == "__main__":
    print("Running 'sector_trend_average.py'")
    calculate_sector_trends()
