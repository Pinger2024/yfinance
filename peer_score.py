# Function to calculate RS score based on multiple periods (same as the original RS score calculation)
def calculate_rs_score(merged_df):
    periods = [63, 126, 189, 252]  # Look-back periods in days
    weights = [2, 1, 1, 1]  # Weights for each period
    rs_values = []

    for i, period in enumerate(periods):
        n = min(len(merged_df) - 1, period)
        if n > 0:
            current_ticker_close = merged_df['close_ticker'].iloc[-1]
            previous_ticker_close = merged_df['close_ticker'].iloc[-(n+1)]
            current_peer_avg_close = merged_df['close_peer_avg'].iloc[-1]
            previous_peer_avg_close = merged_df['close_peer_avg'].iloc[-(n+1)]

            rs_value = (current_ticker_close / previous_ticker_close) - \
                       (current_peer_avg_close / previous_peer_avg_close)
            rs_values.append(rs_value)
        else:
            rs_values.append(0.0)

    # Calculate the raw RS score using weighted sum
    rs_raw = sum([rs_values[i] * weights[i] for i in range(len(rs_values))])
    max_score = sum(weights)
    min_score = -max_score

    # Normalize the RS score to fit between 1 and 99
    rs_score = normalize_rs_score(rs_raw, max_score, min_score)
    return max(1, min(99, rs_score))  # Ensure the score stays within 1 and 99

# Function to process peer RS scores (using the same logic as original RS)
def process_peer_rs(ticker, ticker_df, category, category_value, lookback_days):
    logging.info(f"Calculating {category} peer RS for {ticker} in {category_value}")

    # Get tickers in the same sector or industry
    peer_tickers = indicators_collection.distinct("ticker", {category: category_value, "ticker": {"$ne": ticker}})

    if not peer_tickers:
        return

    # Fetch peer data (limit to 'lookback_days')
    peer_data = list(ohlcv_collection.find(
        {"ticker": {"$in": peer_tickers}}
    ).sort("date", -1).limit(lookback_days))

    if not peer_data:
        return

    # Create a DataFrame for peer data and calculate the peer average
    peer_df = pd.DataFrame(peer_data)
    peer_df['date'] = pd.to_datetime(peer_df['date'])
    peer_avg = peer_df.groupby("date")["close"].mean().reset_index()
    peer_avg.rename(columns={"close": "close_peer_avg"}, inplace=True)

    # Merge ticker data with peer average
    merged_df = pd.merge(ticker_df[['date', 'close']], peer_avg, on="date", how="inner")
    merged_df.rename(columns={"close": "close_ticker"}, inplace=True)

    # Check if merged_df has rows before proceeding
    if merged_df.empty:
        logging.warning(f"No matching data found for {ticker} in {category}: {category_value}")
        return

    # Calculate peer RS score using the same method as original RS score
    peer_rs_score = calculate_rs_score(merged_df)

    # Update OHLCV collection with the peer RS score
    ohlcv_collection.update_many(
        {"ticker": ticker, "date": {"$gte": merged_df['date'].min(), "$lte": merged_df['date'].max()}},
        {"$set": {f"peer_rs_score_{category}": peer_rs_score}}
    )
    logging.info(f"Stored peer RS score for {ticker} in {category}: {peer_rs_score}")
