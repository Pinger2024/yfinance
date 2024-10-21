from pymongo import MongoClient
import pandas as pd
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)

# MongoDB connection setup
mongo_uri = 'mongodb://mongodb-9iyq:27017'
client = MongoClient(mongo_uri, connectTimeoutMS=60000, socketTimeoutMS=60000)
db = client['StockData']
ohlcv_collection = db['ohlcv_data']

def calculate_rolling_return(df, num_days):
    if len(df) >= num_days:
        end_price = df.iloc[-1]['close']
        start_price = df.iloc[-num_days]['close']
        return ((end_price / start_price) - 1) * 100
    return None

def update_rolling_returns(ticker):
    try:
        # Fetch OHLCV data for the given ticker
        ohlcv_data = pd.DataFrame(list(ohlcv_collection.find({"ticker": ticker}).sort("date", 1)))
        
        if ohlcv_data.empty:
            logging.warning(f"No data found for ticker {ticker}")
            return

        # Iterate over each row to calculate RS1, RS2, RS3, RS4
        for i in range(len(ohlcv_data)):
            current_data = ohlcv_data.iloc[:i+1]  # Get data from start to the current row
            
            rs1 = calculate_rolling_return(current_data, 63)
            rs2 = calculate_rolling_return(current_data, 126)
            rs3 = calculate_rolling_return(current_data, 189)
            rs4 = calculate_rolling_return(current_data, 252)
            
            # Update the document in MongoDB
            update_query = {"_id": ohlcv_data.iloc[i]["_id"]}
            update_values = {"$set": {"RS1": rs1, "RS2": rs2, "RS3": rs3, "RS4": rs4}}
            ohlcv_collection.update_one(update_query, update_values)

        logging.info(f"Successfully updated rolling RS values for {ticker}")
    except Exception as e:
        logging.error(f"Error updating rolling RS values for {ticker}: {e}")

if __name__ == "__main__":
    ticker = "TSLA"
    update_rolling_returns(ticker)
