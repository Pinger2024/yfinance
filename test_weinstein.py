import pandas as pd
from pymongo import MongoClient
import numpy as np
from datetime import datetime

# MongoDB connection (adjust the connection string as necessary)
client = MongoClient("mongodb://mongodb-9iyq:27017")
db = client['StockData']
ohlcv_collection = db['ohlcv_data']

# Market index symbol (using ^GSPC instead of SPY)
market_ticker = '^GSPC'

# Function to fetch data from MongoDB
def fetch_daily_data(ticker):
    data = list(ohlcv_collection.find({"ticker": ticker}).sort("date", 1))
    if data:
        df = pd.DataFrame(data)
        df['date'] = pd.to_datetime(df['date'])
        df.set_index('date', inplace=True)
        df = df[['open', 'high', 'low', 'close', 'volume']].sort_index()
        df.columns = ['Open', 'High', 'Low', 'Close', 'Volume']
        return df
    else:
        print(f"No data found for ticker: {ticker}")
        return None

# Function to resample daily data to weekly data
def resample_to_weekly(df_daily):
    df_weekly = df_daily.resample('W-FRI').agg({
        'Open': 'first',
        'High': 'max',
        'Low': 'min',
        'Close': 'last',
        'Volume': 'sum'
    }).dropna()
    return df_weekly

# Implement Weinstein Stage Analysis
def weinstein_stage_analysis(ticker_df, market_df):
    # Parameters (as per your Pine Script)
    ma_slow_period = 30  # Slow MA period (30 weeks)
    ma_fast_period = 10  # Fast MA period (10 weeks)
    mansfield_ma_period = 52  # Mansfield RS MA period
    vol_ma_period = 20  # Volume MA period

    # Calculate moving averages
    ticker_df['sma_slow'] = ticker_df['Close'].rolling(window=ma_slow_period).mean()
    ticker_df['sma_fast'] = ticker_df['Close'].rolling(window=ma_fast_period).mean()

    # Calculate Mansfield Relative Strength
    df_combined = ticker_df[['Close']].join(market_df['Close_market'], how='inner')
    df_combined['stock_divided_by_market'] = df_combined['Close'] / df_combined['Close_market'] * 100
    df_combined['zero_line_ma'] = df_combined['stock_divided_by_market'].rolling(window=mansfield_ma_period).mean()
    df_combined['mansfield_rs'] = ((df_combined['stock_divided_by_market'] / df_combined['zero_line_ma']) - 1) * 100

    # Merge back the mansfield_rs into ticker_df
    ticker_df = ticker_df.join(df_combined['mansfield_rs'], how='left')

    # Volume confirmation
    ticker_df['vol_ma'] = ticker_df['Volume'].rolling(window=vol_ma_period).mean()
    ticker_df['vol_confirmation'] = ticker_df['Volume'].rolling(window=5).mean() > ticker_df['vol_ma']

    # Determine stage
    def determine_stage(row):
        if row['Close'] > row['sma_slow'] and row['mansfield_rs'] > 0:
            return 1  # Stage 2: Advancing
        elif row['Close'] < row['sma_slow'] and row['mansfield_rs'] < 0:
            return 3  # Stage 4: Declining
        elif row['Close'] > row['sma_fast'] and row['sma_fast'] > row['sma_slow'] and row['mansfield_rs'] > -5:
            return 0  # Stage 1: Basing (late)
        elif row['Close'] < row['sma_fast'] and row['sma_fast'] < row['sma_slow'] and row['mansfield_rs'] < 5:
            return 2  # Stage 3: Topping
        else:
            return -1  # Transitional

    ticker_df['current_stage'] = ticker_df.apply(determine_stage, axis=1)
    ticker_df['stage_change'] = ticker_df['current_stage'].diff()

    # Detect potential buy setup
    ticker_df['vol_confirmation'] = ticker_df['vol_confirmation'].fillna(False)
    ticker_df['potential_buy_setup'] = (
        (ticker_df['Close'] > ticker_df['sma_slow']) &
        (ticker_df['Close'] > ticker_df['Close'].shift(1)) &
        (ticker_df['sma_fast'] > ticker_df['sma_fast'].shift(1)) &
        (ticker_df['mansfield_rs'] > 0) &
        (ticker_df['vol_confirmation'])
    )

    # Generate buy signals
    ticker_df['buy_signal'] = (
        ((ticker_df['stage_change'] != 0) & (ticker_df['current_stage'] == 1) & (ticker_df['current_stage'].shift(1) == 0)) |
        ((ticker_df['current_stage'] == 1) & ticker_df['potential_buy_setup'])
    )

    return ticker_df

# Main function to process all tickers
def main():
    # Fetch all tickers from the database
    all_tickers = ohlcv_collection.distinct('ticker')
    print(f"Total tickers to process: {len(all_tickers)}")

    # Fetch market data
    market_daily_df = fetch_daily_data(market_ticker)
    if market_daily_df is None:
        print("Market data not available.")
        return

    market_weekly_df = resample_to_weekly(market_daily_df)
    market_weekly_df.rename(columns={'Close': 'Close_market'}, inplace=True)

    # Process each ticker
    for ticker in all_tickers:
        print(f"Processing {ticker}")
        ticker_daily_df = fetch_daily_data(ticker)
        if ticker_daily_df is None:
            continue

        ticker_weekly_df = resample_to_weekly(ticker_daily_df)
        if ticker_weekly_df.empty:
            print(f"No weekly data available for {ticker}")
            continue

        # Ensure both dataframes have the same date index
        ticker_weekly_df = ticker_weekly_df.loc[ticker_weekly_df.index.isin(market_weekly_df.index)]
        market_weekly_df_ticker = market_weekly_df.loc[market_weekly_df.index.isin(ticker_weekly_df.index)]

        if ticker_weekly_df.empty or market_weekly_df_ticker.empty:
            print(f"No overlapping weekly data for {ticker}")
            continue

        # Run Weinstein Stage Analysis
        combined_df = weinstein_stage_analysis(ticker_weekly_df, market_weekly_df_ticker)
        if combined_df.empty:
            print(f"No combined data for {ticker}")
            continue

        # Get the latest week's data
        latest_data = combined_df.iloc[-1]
        if latest_data['buy_signal']:
            print(f"Buy signal detected for {ticker} on {latest_data.name.date()}")
            # Optionally store the signal in the database
            # Store signal in indicators collection
            indicator_data = {
                "ticker": ticker,
                "buy_signal": True,
                "signal_date": latest_data.name.to_pydatetime(),
                "stage": int(latest_data['current_stage']),
                "mansfield_rs": float(latest_data['mansfield_rs']),
                "date": pd.to_datetime('today'),
            }
            # Convert numpy types to native types
            indicator_data = convert_numpy_types(indicator_data)
            db.indicators.update_one(
                {"ticker": ticker},
                {"$set": indicator_data},
                upsert=True
            )
        else:
            print(f"No buy signal for {ticker}")
            # Optionally update the indicator data without buy signal
            indicator_data = {
                "ticker": ticker,
                "buy_signal": False,
                "signal_date": latest_data.name.to_pydatetime(),
                "stage": int(latest_data['current_stage']),
                "mansfield_rs": float(latest_data['mansfield_rs']),
                "date": pd.to_datetime('today'),
            }
            # Convert numpy types to native types
            indicator_data = convert_numpy_types(indicator_data)
            db.indicators.update_one(
                {"ticker": ticker},
                {"$set": indicator_data},
                upsert=True
            )

# Function to convert numpy data types to native Python types
def convert_numpy_types(data):
    if isinstance(data, dict):
        return {k: convert_numpy_types(v) for k, v in data.items()}
    elif isinstance(data, np.bool_):
        return bool(data)
    elif isinstance(data, np.integer):
        return int(data)
    elif isinstance(data, np.floating):
        return float(data)
    elif isinstance(data, np.ndarray):
        return data.tolist()
    else:
        return data

if __name__ == "__main__":
    main()
