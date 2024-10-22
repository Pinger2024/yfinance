import pandas as pd
from pymongo import MongoClient
import logging
from datetime import datetime
import warnings

# Suppress specific warnings
warnings.filterwarnings("ignore", category=FutureWarning)

# Setup detailed logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# MongoDB connection setup
client = MongoClient("mongodb://mongodb-9iyq:27017")
db = client['StockData']
ohlcv_collection = db['ohlcv_data']

def debug_tsla_calculations():
    logging.info("\n=== TSLA Calculation Debug ===")
    
    # Get TSLA historical data
    history = list(ohlcv_collection.find(
        {"ticker": "TSLA"},
        {"date": 1, "close": 1}
    ).sort("date", -1).limit(252))
    
    # Convert to DataFrame for easier manipulation
    df = pd.DataFrame(history)
    df = df.sort_values('date')  # Sort chronologically
    
    # Print all dates and closes for verification
    logging.info("\nAll dates and closes for TSLA (last 10 days):")
    for _, row in df.tail(10).iterrows():
        logging.info(f"Date: {row['date'].strftime('%Y-%m-%d')}, Close: {row['close']}")
    
    # Daily Percentage Change Calculation
    latest_close = df['close'].iloc[-1]
    previous_close = df['close'].iloc[-2]
    daily_pct_change = (latest_close - previous_close) / previous_close * 100
    
    logging.info("\nDaily Percentage Change Calculation:")
    logging.info(f"Latest date: {df['date'].iloc[-1].strftime('%Y-%m-%d')}")
    logging.info(f"Latest close: {latest_close}")
    logging.info(f"Previous date: {df['date'].iloc[-2].strftime('%Y-%m-%d')}")
    logging.info(f"Previous close: {previous_close}")
    logging.info(f"Calculated daily_pct_change: {daily_pct_change:.4f}%")
    
    # RS4 Calculation
    current_close = df['close'].iloc[-1]
    historical_close = df['close'].iloc[-252]  # 252 trading days ago
    rs4 = (current_close - historical_close) / historical_close * 100
    
    logging.info("\nRS4 Calculation:")
    logging.info(f"Current date: {df['date'].iloc[-1].strftime('%Y-%m-%d')}")
    logging.info(f"Current close: {current_close}")
    logging.info(f"Historical date (252 days ago): {df['date'].iloc[-252].strftime('%Y-%m-%d')}")
    logging.info(f"Historical close: {historical_close}")
    logging.info(f"Calculated RS4: {rs4:.4f}%")
    
    # Other RS Calculations
    periods = {
        "RS1": 63,
        "RS2": 126,
        "RS3": 189,
    }
    
    for rs_key, period in periods.items():
        current_close = df['close'].iloc[-1]
        historical_close = df['close'].iloc[-period]
        rs_value = (current_close - historical_close) / historical_close * 100
        
        logging.info(f"\n{rs_key} Calculation:")
        logging.info(f"Current date: {df['date'].iloc[-1].strftime('%Y-%m-%d')}")
        logging.info(f"Current close: {current_close}")
        logging.info(f"Historical date ({period} days ago): {df['date'].iloc[-period].strftime('%Y-%m-%d')}")
        logging.info(f"Historical close: {historical_close}")
        logging.info(f"Calculated {rs_key}: {rs_value:.4f}%")

    # Update database with corrected values
    latest_date = df['date'].iloc[-1]
    update_result = ohlcv_collection.update_one(
        {"ticker": "TSLA", "date": latest_date},
        {"$set": {
            "daily_pct_change": daily_pct_change,
            "RS1": (df['close'].iloc[-1] - df['close'].iloc[-63]) / df['close'].iloc[-63] * 100,
            "RS2": (df['close'].iloc[-1] - df['close'].iloc[-126]) / df['close'].iloc[-126] * 100,
            "RS3": (df['close'].iloc[-1] - df['close'].iloc[-189]) / df['close'].iloc[-189] * 100,
            "RS4": rs4
        }},
        upsert=True
    )
    
    logging.info("\nDatabase Update Result:")
    logging.info(f"Matched: {update_result.matched_count}")
    logging.info(f"Modified: {update_result.modified_count}")
    
    # Verify the update
    updated_doc = ohlcv_collection.find_one(
        {"ticker": "TSLA", "date": latest_date},
        {"date": 1, "RS1": 1, "RS2": 1, "RS3": 1, "RS4": 1, "daily_pct_change": 1}
    )
    
    logging.info("\nUpdated Values in Database:")
    logging.info(f"Date: {updated_doc['date'].strftime('%Y-%m-%d')}")
    logging.info(f"Daily %Change: {updated_doc.get('daily_pct_change', 'Not found')}")
    logging.info(f"RS1: {updated_doc.get('RS1', 'Not found')}")
    logging.info(f"RS2: {updated_doc.get('RS2', 'Not found')}")
    logging.info(f"RS3: {updated_doc.get('RS3', 'Not found')}")
    logging.info(f"RS4: {updated_doc.get('RS4', 'Not found')}")

def main():
    logging.info("Starting TSLA debug script")
    debug_tsla_calculations()
    logging.info("Debug complete")

if __name__ == "__main__":
    main()