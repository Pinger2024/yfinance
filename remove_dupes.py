import sys
from pymongo import MongoClient
from datetime import datetime

# MongoDB connection setup
client = MongoClient("mongodb://mongodb-9iyq:27017")
db = client['StockData']
ohlcv_collection = db['ohlcv_data']

def clean_up_duplicates():
    # Step 1: Aggregate and find duplicate records (grouped by ticker and date without time)
    pipeline = [
        {
            "$group": {
                "_id": { "ticker": "$ticker", "date": { "$dateToString": { "format": "%Y-%m-%d", "date": "$date" }} },
                "count": { "$sum": 1 },
                "docs": { "$push": { "id": "$_id", "date": "$date" } }  # Keep track of document IDs and actual dates
            }
        },
        {
            "$match": { "count": { "$gt": 1 } }  # Only return groups with more than one document
        }
    ]

    duplicates = list(ohlcv_collection.aggregate(pipeline))

    if not duplicates:
        print("No duplicates found.")
        return  # Exit early if no duplicates are found

    # Step 2: Process each group of duplicates
    for duplicate in duplicates:
        ticker = duplicate["_id"]["ticker"]
        date_str = duplicate["_id"]["date"]
        doc_list = duplicate["docs"]

        # Convert the date string back to an actual datetime object
        date_objects = [(doc["id"], doc["date"]) for doc in doc_list]
        date_objects.sort(key=lambda x: x[1], reverse=True)  # Sort by date, descending

        # Step 3: Keep the most recent record and delete others
        most_recent_id = date_objects[0][0]  # Keep this one
        ids_to_remove = [doc_id for doc_id, _ in date_objects[1:]]  # Remove the rest

        if ids_to_remove:
            ohlcv_collection.delete_many({"_id": { "$in": ids_to_remove }})
            print(f"Removed {len(ids_to_remove)} duplicates for {ticker} on {date_str}")

    print("Duplicate cleanup completed.")
    sys.exit(0)  # Exit after script completion

if __name__ == "__main__":
    clean_up_duplicates()
