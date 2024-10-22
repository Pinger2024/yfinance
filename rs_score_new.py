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

def get_trading_days(df, current_date, days_back):
    """
    Get the correct historical date by counting actual trading days backwards.
    """
    # Ensure df is sorted by date in ascending order and handle duplicates
    df = df.sort_values('date').drop_duplicates(subset=['date'], keep='last')
    
    # Find the index of the current date
    current_idx = df[df['date'] <= current_date].index[-1]
    
    # Count back the specified number of trading days
    historical_idx = current_idx - days_back
    
    if historical_idx < 0:
        raise ValueError(f"Not enough trading days in dataset to go back {days_back} days")
        
    return df.iloc[historical_idx]['date'], df.iloc[historical_idx]['close']

def debug_tsla_calculations():
    logging.info("\n=== TSLA Calculation Debug ===")
    
    # Get TSLA historical data - no limit on the find operation
    history = list(ohlcv_collection.find(
        {"ticker": "TSLA"},
        {"date": 1, "close": 1, "_id": 0}
    ))
    
    logging.info(f"Total number of records retrieved: {len(history)}")
    
    # Convert to DataFrame and handle duplicates
    df = pd.DataFrame(history)
    df = df.sort_values('date').drop_duplicates(subset=['date'], keep='last')
    
    logging.info(f"Number of unique trading days: {len(df)}")
    logging.info(f"Date range: {df['date'].min().strftime('%Y-%m-%d')} to {df['date'].max().strftime('%Y-%m-%d')}")
    
    # Get the most recent trading day
    latest_date = df['date'].max()
    
    # Print all dates and closes for verification
    logging.info("\nAll dates and closes for TSLA (last 10 days):")
    for _, row in df.tail(10).iterrows():
        logging.info(f"Date: {row['date'].strftime('%Y-%m-%d')}, Close: {row['close']}")
    
    # Daily Percentage Change Calculation
    latest_close = df[df['date'] == latest_date]['close'].iloc[0]
    previous_date = df[df['date'] < latest_date]['date'].max()
    previous_close = df[df['date'] == previous_date]['close'].iloc[0]
    daily_pct_change = (latest_close - previous_close) / previous_close * 100
    
    logging.info("\nDaily Percentage Change Calculation:")
    logging.info(f"Latest date: {latest_date.strftime('%Y-%m-%d')}")
    logging.info(f"Latest close: {latest_close}")
    logging.info(f"Previous date: {previous_date.strftime('%Y-%m-%d')}")
    logging.info(f"Previous close: {previous_close}")
    logging.info(f"Calculated daily_pct_change: {daily_pct_change:.4f}%")
    
    # RS Calculations
    periods = {
        "RS1": 63,   # ~3 months
        "RS2": 126,  # ~6 months
        "RS3": 189,  # ~9 months
        "RS4": 252   # ~12 months
    }
    
    rs_values = {}
    
    for rs_key, period in periods.items():
        try:
            historical_date, historical_close = get_trading_days(df, latest_date, period)
            rs_value = (latest_close - historical_close) / historical_close * 100
            rs_values[rs_key] = rs_value
            
            logging.info(f"\n{rs_key} Calculation:")
            logging.info(f"Current date: {latest_date.strftime('%Y-%m-%d')}")
            logging.info(f"Current close: {latest_close}")
            logging.info(f"Historical date ({period} trading days ago): {historical_date.strftime('%Y-%m-%d')}")
            logging.info(f"Historical close: {historical_close}")
            logging.info(f"Calculated {rs_key}: {rs_value:.4f}%")
        except ValueError as e:
            logging.error(f"Error calculating {rs_key}: {str(e)}")
            rs_values[rs_key] = None

    # Update database with corrected values
    update_doc = {
        "daily_pct_change": daily_pct_change
    }
    
    # Only include RS values that were successfully calculated
    for key, value in rs_values.items():
        if value is not None:
            update_doc[key] = value

    update_result = ohlcv_collection.update_one(
        {"ticker": "TSLA", "date": latest_date},
        {"$set": update_doc},
        upsert=True
    )
    
    logging.info("\nDatabase Update Result:")
    logging.info(f"Matched: {update_result.matched_count}")
    logging.info(f"Modified: {update_result.modified_count}")
    
    # Verify the update
    updated_doc = ohlcv_collection.find_one(
        {"ticker": "TSLA", "date": latest_date},
        {"date": 1, "RS1": 1, "RS2": 1, "RS3": 1, "RS4": 1, "daily_pct_change": 1, "_id": 0}
    )
    
    logging.info("\nUpdated Values in Database:")
    logging.info(f"Date: {updated_doc['date'].strftime('%Y-%m-%d')}")
    logging.info(f"Daily %Change: {updated_doc.get('daily_pct_change', 'Not found')}")
    for rs_key in ["RS1", "RS2", "RS3", "RS4"]:
        logging.info(f"{rs_key}: {updated_doc.get(rs_key, 'Not found')}")

def main():
    logging.info("Starting TSLA debug script")
    try:
        debug_tsla_calculations()
        logging.info("Debug complete")
    except Exception as e:
        logging.error(f"Error in main execution: {str(e)}")
        raise

if __name__ == "__main__":
    main()