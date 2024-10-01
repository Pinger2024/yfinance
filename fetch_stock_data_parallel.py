import yfinance as yf
from pymongo import MongoClient
import pandas as pd
import time
import logging
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# MongoDB connection (using Render's internal hostname)
uri = "mongodb://mongodb-9iyq:27017"  # Internal connection string for MongoDB on Render
client = MongoClient(uri)  # Removed ServerApi
db = client['StockData']  # The database will be created if it doesn't exist
collection = db['comprehensive_data']

# Create a unique index to prevent duplicates
try:
    collection.create_index([('ticker', 1), ('date', 1)], unique=True)
    logger.info("Index created successfully")
except Exception as e:
    logger.error(f"Failed to create index: {e}")

# Function to check MongoDB connection
def check_mongo_connection():
    try:
        client.admin.command('ping')
        logger.info("MongoDB connection successful.")
        return True
    except Exception as e:
        logger.error(f"Failed to connect to MongoDB: {e}")
        return False

# Function to convert any non-string keys to strings recursively
def convert_keys_to_string(data):
    if isinstance(data, dict):
        return {str(key): convert_keys_to_string(value) for key, value in data.items()}
    elif isinstance(data, list):
        return [convert_keys_to_string(element) for element in data]
    else:
        return data

# Function to fetch and store data
def fetch_and_store_ticker_data(ticker):
    logger.info(f"Fetching data for {ticker}")
    stock = yf.Ticker(ticker)
    
    try:
        hist = stock.history(period="2y")
        if hist.empty:
            logger.warning(f"No data found for {ticker}, possibly delisted or unavailable.")
            return False

        dividends = stock.dividends
        splits = stock.splits
        financials = convert_keys_to_string(stock.financials.T.to_dict() if not stock.financials.empty else {})
        balance_sheet = convert_keys_to_string(stock.balance_sheet.T.to_dict() if not stock.balance_sheet.empty else {})
        cashflow = convert_keys_to_string(stock.cashflow.T.to_dict() if not stock.cashflow.empty else {})
        
        try:
            recommendations = stock.recommendations_summary
            analyst_recommendations = convert_keys_to_string(recommendations.to_dict()) if recommendations is not None else {}
        except Exception as e:
            logger.warning(f"Could not fetch recommendations for {ticker}: {e}")
            analyst_recommendations = {}

        for date, row in hist.iterrows():
            data = {
                'ticker': ticker,
                'date': date.to_pydatetime(),
                'open': row['Open'],
                'high': row['High'],
                'low': row['Low'],
                'close': row['Close'],
                'volume': row['Volume'],
                'dividends': dividends.get(date, 0),
                'splits': splits.get(date, 0),
                'financials': financials,
                'balance_sheet': balance_sheet,
                'cashflow': cashflow,
                'analyst_recommendations': analyst_recommendations
            }
            collection.update_one(
                {'ticker': ticker, 'date': data['date']},
                {'$set': data},
                upsert=True
            )
        logger.info(f"Successfully fetched and stored data for {ticker}")
    except Exception as e:
        logger.error(f"Error fetching data for {ticker}: {e}")
        return False
    return True

# Function to fetch data in parallel batches using threading
def fetch_data_in_parallel(tickers, max_workers=10):
    total_tickers = len(tickers)
    logger.info(f"Fetching data for {total_tickers} tickers using {max_workers} threads.")
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_ticker = {executor.submit(fetch_and_store_ticker_data, ticker): ticker for ticker in tickers}
        for future in as_completed(future_to_ticker):
            ticker = future_to_ticker[future]
            try:
                result = future.result()
                if result:
                    logger.info(f"Completed fetching data for {ticker}")
                else:
                    logger.warning(f"Failed to fetch data for {ticker}")
            except Exception as exc:
                logger.error(f"Error fetching data for {ticker}: {exc}")

# Main process
if __name__ == "__main__":
    # Check MongoDB connection before proceeding
    if check_mongo_connection():
        # Load tickers from the CSV files stored in the GitHub repo (in the same directory)
        uk_stocks = pd.read_csv('Stock Screener_UK.csv')['Symbol']
        us_stocks = pd.read_csv('Stock Screener_2024-09-30 (3).csv')['Symbol']

        # Combine the tickers
        all_tickers = pd.concat([us_stocks, uk_stocks], ignore_index=True)

        # Save combined tickers to a CSV file (optional)
        all_tickers.to_csv('combined_tickers.csv', index=False)

        # Convert tickers to a list and drop any NaN values
        tickers_list = all_tickers.dropna().tolist()

        # Fetch data using threading
        fetch_data_in_parallel(tickers_list, max_workers=10)
    else:
        logger.error("Script aborted due to MongoDB connection failure.")
