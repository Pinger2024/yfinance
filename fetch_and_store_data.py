import yfinance as yf
from pymongo import MongoClient
import pandas as pd
import logging
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# MongoDB connection
uri = "mongodb://mongodb-9iyq:27017"  # Internal connection string for MongoDB on Render
client = MongoClient(uri)
db = client['StockData']

# Separate collections for OHLCV and Meta Data
ohlcv_collection = db['ohlcv_data']
meta_data_collection = db['meta_data']

# Create unique indexes to avoid duplicates
try:
    ohlcv_collection.create_index([('ticker', 1), ('date', 1)], unique=True)
    meta_data_collection.create_index([('ticker', 1)], unique=True)
    logger.info("Indexes created successfully")
except Exception as e:
    logger.error(f"Failed to create indexes: {e}")

# Function to fetch and store OHLCV data
def fetch_and_store_ohlcv_data(ticker):
    logger.info(f"Fetching OHLCV data for {ticker}")
    stock = yf.Ticker(ticker)
    
    try:
        hist = stock.history(period="2y")  # Fetch last 2 years of OHLCV data
        if hist.empty:
            logger.warning(f"No OHLCV data found for {ticker}, possibly delisted or unavailable.")
            return False

        for date, row in hist.iterrows():
            data = {
                'ticker': ticker,
                'date': date.to_pydatetime(),
                'open': row['Open'],
                'high': row['High'],
                'low': row['Low'],
                'close': row['Close'],
                'volume': row['Volume']
            }
            # Insert OHLCV data (upsert to avoid duplicates)
            ohlcv_collection.update_one(
                {'ticker': ticker, 'date': data['date']},
                {'$set': data},
                upsert=True
            )
        logger.info(f"Successfully fetched and stored OHLCV data for {ticker}")
    except Exception as e:
        logger.error(f"Error fetching OHLCV data for {ticker}: {e}")
        return False

# Function to fetch and store meta data (financials, dividends, splits, recommendations)
def fetch_and_store_meta_data(ticker):
    logger.info(f"Fetching Meta Data for {ticker}")
    stock = yf.Ticker(ticker)
    
    try:
        # Fetch non-daily meta data
        financials = stock.financials.T.to_dict() if not stock.financials.empty else {}
        balance_sheet = stock.balance_sheet.T.to_dict() if not stock.balance_sheet.empty else {}
        cashflow = stock.cashflow.T.to_dict() if not stock.cashflow.empty else {}
        dividends = stock.dividends.to_dict() if not stock.dividends.empty else {}
        splits = stock.splits.to_dict() if not stock.splits.empty else {}
        recommendations = stock.recommendations_summary.to_dict() if stock.recommendations_summary is not None else {}

        # Store meta data
        meta_data = {
            'ticker': ticker,
            'financials': financials,
            'balance_sheet': balance_sheet,
            'cashflow': cashflow,
            'dividends': dividends,
            'splits': splits,
            'analyst_recommendations': recommendations
        }
        meta_data_collection.update_one(
            {'ticker': ticker},
            {'$set': meta_data},
            upsert=True
        )
        logger.info(f"Successfully fetched and stored meta data for {ticker}")
    except Exception as e:
        logger.error(f"Error fetching meta data for {ticker}: {e}")
        return False

# Function to fetch OHLCV and meta data for a ticker
def fetch_and_store_all_data(ticker):
    success_ohlcv = fetch_and_store_ohlcv_data(ticker)
    success_meta = fetch_and_store_meta_data(ticker)
    return success_ohlcv and success_meta

# Function to fetch data in parallel batches using threading
def fetch_data_in_parallel(tickers, max_workers=10):
    total_tickers = len(tickers)
    logger.info(f"Fetching data for {total_tickers} tickers using {max_workers} threads.")
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_ticker = {executor.submit(fetch_and_store_all_data, ticker): ticker for ticker in tickers}
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
    # Load tickers from your CSV files
    uk_stocks = pd.read_csv('Stock Screener_UK.csv')['Symbol']
    us_stocks = pd.read_csv('Stock Screener_2024-09-30 (3).csv')['Symbol']

    # Combine the tickers
    all_tickers = pd.concat([us_stocks, uk_stocks], ignore_index=True)

    # Convert tickers to a list and drop any NaN values
    tickers_list = all_tickers.dropna().tolist()

    # Fetch data in parallel
    fetch_data_in_parallel(tickers_list)
