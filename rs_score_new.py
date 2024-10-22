import pandas as pd
from pymongo import MongoClient
import logging
from datetime import datetime, timedelta
import warnings

# Suppress specific warnings
warnings.filterwarnings("ignore", category=FutureWarning)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# MongoDB connection setup
client = MongoClient("mongodb://mongodb-9iyq:27017")
db = client['StockData']
ohlcv_collection = db['ohlcv_data']

def get_latest_trading_date():
    """Get the most recent date with data in the database"""
    latest_record = ohlcv_collection.find_one(
        {},
        sort=[("date", -1)]
    )
    return latest_record['date'] if latest_record else None

def update_rs_values():
    latest_date = get_latest_trading_date()
    if not latest_date:
        logging.error("No data found in database")
        return

    logging.info(f"Processing RS values for date: {latest_date}")
    
    # Get distinct tickers for the latest date
    tickers = ohlcv_collection.distinct(
        "ticker",
        {"date": latest_date}
    )
    
    logging.info(f"Found {len(tickers)} tickers to process")

    # RS periods definition
    periods = {
        "RS1": 63,
        "RS2": 126,
        "RS3": 189,
        "RS4": 252
    }

    for ticker in tickers:
        logging.info(f"\nProcessing ticker: {ticker}")
        try:
            # Fetch historical data for the ticker
            history = list(ohlcv_collection.find(
                {"ticker": ticker},
                {"date": 1, "close": 1}
            ).sort("date", -1).limit(252))
            
            if len(history) < 2:  # Need at least 2 days for daily_pct_change
                logging.warning(f"Insufficient data for {ticker}")
                continue

            history_df = pd.DataFrame(history)
            
            # Calculate daily percentage change
            try:
                daily_pct_change = ((history_df['close'].iloc[0] - history_df['close'].iloc[1]) 
                                  / history_df['close'].iloc[1] * 100)
                
                ohlcv_collection.update_one(
                    {"ticker": ticker, "date": latest_date},
                    {"$set": {"daily_pct_change": daily_pct_change}},
                    upsert=True
                )
                logging.info(f"Updated daily_pct_change: {daily_pct_change:.2f}%")
            except Exception as e:
                logging.error(f"Error calculating daily_pct_change for {ticker}: {e}")

            # Calculate RS values
            for rs_key, period in periods.items():
                try:
                    if len(history_df) >= period:
                        current_price = history_df['close'].iloc[0]
                        historical_price = history_df['close'].iloc[period-1]
                        
                        rolling_return = ((current_price - historical_price) 
                                        / historical_price * 100)
                        
                        ohlcv_collection.update_one(
                            {"ticker": ticker, "date": latest_date},
                            {"$set": {rs_key: rolling_return}},
                            upsert=True
                        )
                        logging.info(f"Updated {rs_key}: {rolling_return:.2f}%")
                    else:
                        logging.warning(f"Insufficient data for {rs_key} calculation (need {period} days)")
                except Exception as e:
                    logging.error(f"Error calculating {rs_key} for {ticker}: {e}")

        except Exception as e:
            logging.error(f"Error processing ticker {ticker}: {e}")

def verify_updates():
    """Verify that updates were successful for the latest date"""
    latest_date = get_latest_trading_date()
    if not latest_date:
        return
    
    # Check a random sample of records
    sample_records = ohlcv_collection.find(
        {"date": latest_date},
        {"ticker": 1, "RS1": 1, "RS2": 1, "RS3": 1, "RS4": 1, "daily_pct_change": 1}
    ).limit(5)
    
    logging.info("\nVerification Sample:")
    for record in sample_records:
        logging.info(f"\nTicker: {record['ticker']}")
        logging.info(f"RS1: {record.get('RS1')}")
        logging.info(f"RS2: {record.get('RS2')}")
        logging.info(f"RS3: {record.get('RS3')}")
        logging.info(f"RS4: {record.get('RS4')}")
        logging.info(f"Daily %Change: {record.get('daily_pct_change')}")

def main():
    logging.info("Starting RS values update script")
    update_rs_values()
    verify_updates()
    logging.info("RS values update completed")

if __name__ == "__main__":
    main()