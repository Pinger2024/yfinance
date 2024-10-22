import pandas as pd
from pymongo import MongoClient, UpdateOne
import logging
from datetime import datetime
import warnings
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

# Suppress warnings
warnings.filterwarnings("ignore", category=FutureWarning)

# Setup basic logging
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
    df = df.sort_values('date').drop_duplicates(subset=['date'], keep='last')
    current_idx = df[df['date'] <= current_date].index[-1]
    historical_idx = current_idx - days_back
    
    if historical_idx < 0:
        raise ValueError(f"Not enough trading days")
        
    return df.iloc[historical_idx]['date'], df.iloc[historical_idx]['close']

def calculate_rs_scores(ticker):
    """
    Calculate RS scores for all dates for a given ticker.
    """
    try:
        # Get all historical data for the ticker
        history = list(ohlcv_collection.find(
            {"ticker": ticker},
            {"date": 1, "close": 1, "_id": 0}
        ))
        
        if not history:
            return f"No data found for {ticker}"

        # Convert to DataFrame and prepare data
        df = pd.DataFrame(history)
        df = df.sort_values('date').drop_duplicates(subset=['date'], keep='last')
        
        # Prepare bulk updates
        bulk_operations = []
        
        # Calculate values for each date
        for i in range(1, len(df)):
            current_row = df.iloc[i]
            previous_row = df.iloc[i-1]
            current_date = current_row['date']
            
            # Calculate daily percentage change
            daily_pct_change = (current_row['close'] - previous_row['close']) / previous_row['close'] * 100
            
            # Initialize update document
            update_doc = {
                "daily_pct_change": daily_pct_change
            }
            
            # Calculate RS values for different periods
            periods = {
                "RS1": 63,   # ~3 months
                "RS2": 126,  # ~6 months
                "RS3": 189,  # ~9 months
                "RS4": 252   # ~12 months
            }
            
            for rs_key, period in periods.items():
                try:
                    historical_date, historical_close = get_trading_days(df, current_date, period)
                    rs_value = (current_row['close'] - historical_close) / historical_close * 100
                    update_doc[rs_key] = rs_value
                except ValueError:
                    # Skip RS calculation if not enough historical data
                    continue
            
            # Add update operation to bulk operations list
            bulk_operations.append(UpdateOne(
                {"ticker": ticker, "date": current_date},
                {"$set": update_doc}
            ))
        
        # Execute bulk update if there are operations
        if bulk_operations:
            ohlcv_collection.bulk_write(bulk_operations, ordered=False)
            
        return f"Successfully updated {ticker} - {len(bulk_operations)} records"
        
    except Exception as e:
        return f"Error processing {ticker}: {str(e)}"

def main():
    # Get unique tickers
    tickers = ohlcv_collection.distinct("ticker")
    total_tickers = len(tickers)
    
    logging.info(f"Starting bulk update for {total_tickers} tickers")
    
    # Process each ticker with progress bar using ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=5) as executor:  # Adjust max_workers as needed
        futures = {executor.submit(calculate_rs_scores, ticker): ticker for ticker in tickers}
        
        for future in tqdm(as_completed(futures), total=total_tickers, desc="Processing tickers"):
            result = future.result()
            ticker = futures[future]
            if "Error" in result:
                logging.error(result)
            elif "No data" in result:
                logging.warning(result)
            else:
                logging.info(result)
    
    logging.info("Bulk update complete")

if __name__ == "__main__":
    main()
