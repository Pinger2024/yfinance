import pandas as pd
from pymongo import MongoClient
from datetime import datetime
import time

# MongoDB connection (hardcoded)
client = MongoClient("mongodb://mongodb-9iyq:27017")
db = client['StockData']
ohlcv_collection = db['ohlcv_data']
sector_trends_collection = db['sector_trends']

# Fetch distinct dates from the ohlcv_data collection
dates = ohlcv_collection.distinct('date')

# Function to calculate and store average RS score per sector for each date
def calculate_average_rs_by_sector():
    print(f"Starting calculation for {len(dates)} dates...")
    for date in dates:
        start_time = time.time()  # Measure time for each date
        print(f"Processing for date: {date}")

        try:
            # Fetch all tickers' RS scores and sectors for the given date
            pipeline = [
                {"$match": {"date": date}}, 
                {"$group": {"_id": "$sector", "avg_rs_score": {"$avg": "$rs_score"}}}
            ]
            
            sector_rs_scores = list(ohlcv_collection.aggregate(pipeline))
            
            if len(sector_rs_scores) == 0:
                print(f"No data found for {date}, skipping.")
                continue

            print(f"Found {len(sector_rs_scores)} sectors for date: {date}")
            
            # Store the results in the sector_trends collection
            for sector_data in sector_rs_scores:
                print(f"Storing data for sector: {sector_data['_id']}, RS score: {sector_data['avg_rs_score']}")
                sector_trends_collection.update_one(
                    {"sector": sector_data["_id"], "date": date},
                    {"$set": {
                        "sector": sector_data["_id"],
                        "date": date,
                        "avg_rs_score": sector_data["avg_rs_score"]
                    }},
                    upsert=True
                )
            
            # Log the time taken to process this date
            end_time = time.time()
            print(f"Finished processing for {date}. Took {end_time - start_time:.2f} seconds.")

        except Exception as e:
            print(f"Error processing date {date}: {e}")
            continue  # In case of errors, log them and move on

# Run the function to calculate and store the sector trends
if __name__ == "__main__":
    print("Starting the sector trend calculation script.")
    calculate_average_rs_by_sector()
    print("Finished calculating and storing sector trends.")
