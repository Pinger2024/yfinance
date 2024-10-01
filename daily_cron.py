import yfinance as yf
from pymongo import MongoClient
from datetime import datetime
import pytz
import logging

# MongoDB connection
client = MongoClient("mongodb://mongodb-9iyq:27017")
db = client['StockData']
collection = db['ohlcv_data']

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Get today's date in UK time
uk_tz = pytz.timezone('Europe/London')
today = datetime.now(uk_tz).date()

# Get unique tickers from the 'ohlcv_data' collection
def get_unique_tickers():
    try:
        tickers = collection.distinct("ticker")
        return tickers
    except Exception as e:
        logger.error(f"Error fetching tickers from the database: {e}")
        return []

# Fetch and store today's OHLCV data for each ticker
def fetch_todays_data(ticker):
    logger.info(f"Fetching today's data for {ticker}")
    stock = yf.Ticker(ticker)

    try:
        # Fetch today's OHLCV data
        hist = stock.history(period="1d")

        # Check if we have today's data
        if hist.empty or hist.index[0].date() != today:
            logger.warning(f"No data found for {ticker} today, possibly delisted or unavailable.")
            return False

        # Extract data for today
        row = hist.iloc[0]
        data = {
            'ticker': ticker,
            'date': row.name.to_pydatetime(),
            'open': row['Open'],
            'high': row['High'],
            'low': row['Low'],
            'close': row['Close'],
            'volume': row['Volume'],
        }

        # Store today's OHLCV data in the database
        collection.update_one(
            {'ticker': ticker, 'date': data['date']},
            {'$set': data},
            upsert=True
        )
        logger.info(f"Successfully stored today's data for {ticker}")
    except Exception as e:
        logger.error(f"Error fetching data for {ticker}: {e}")
        return False
    return True

# Fetch today's data for all tickers in the database
def fetch_all_tickers_data():
    tickers = get_unique_tickers()
    logger.info(f"Fetching data for {len(tickers)} tickers.")

    success = True
    for ticker in tickers:
        if not fetch_todays_data(ticker):
            success = False

    return success

# Example of how to run the cron job
if __name__ == "__main__":
    success = fetch_all_tickers_data()

    # Send email notification (success or failure)
    if success:
        notify_status(True)
    else:
        notify_status(False)
