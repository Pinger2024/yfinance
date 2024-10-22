import pandas as pd
from pymongo import MongoClient, UpdateOne
import logging
from datetime import datetime
import numpy as np

# Setup basic logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class RSScoreCalculator:
    def __init__(self, mongodb_uri="mongodb://mongodb-9iyq:27017", db_name='StockData'):
        """Initialize the RS Score Calculator with MongoDB connection."""
        self.client = MongoClient(mongodb_uri)
        self.db = self.client[db_name]
        self.ohlcv_collection = self.db['ohlcv_data']
        self.indicators_collection = self.db['indicators']
        
    def get_stock_metadata(self, ticker):
        """Get sector and industry information for a stock."""
        metadata = self.indicators_collection.find_one(
            {"ticker": ticker},
            {"sector": 1, "industry": 1}
        )
        return metadata if metadata else {"sector": None, "industry": None}

    def calculate_weighted_rs_score(self, ticker):
        """Calculate weighted RS score for a stock using multiple timeframes."""
        try:
            # Get the latest date with RS values
            latest_date = self.ohlcv_collection.find_one(
                {"ticker": ticker, "$or": [
                    {"RS4": {"$exists": True, "$ne": None}},
                    {"RS3": {"$exists": True, "$ne": None}},
                    {"RS2": {"$exists": True, "$ne": None}},
                    {"RS1": {"$exists": True, "$ne": None}},
                ]},
                sort=[("date", -1)],
                projection={"date": 1}
            )
            
            if not latest_date:
                logging.warning(f"No RS values found for {ticker}")
                return None
                
            latest_date = latest_date["date"]
            
            # Fetch RS values
            rs_data = self.ohlcv_collection.find_one(
                {"ticker": ticker, "date": latest_date},
                {"RS1": 1, "RS2": 1, "RS3": 1, "RS4": 1}
            )
            
            if not rs_data:
                logging.warning(f"No RS data for {ticker} on {latest_date}")
                return None
            
            # Get stock metadata
            metadata = self.get_stock_metadata(ticker)
            
            # Define weights for different timeframes
            weights = {
                "RS1": 0.40,  # 40% weight for 3-month RS
                "RS2": 0.30,  # 30% weight for 6-month RS
                "RS3": 0.20,  # 20% weight for 9-month RS
                "RS4": 0.10   # 10% weight for 12-month RS
            }
            
            # Calculate weighted score
            weighted_score = sum(
                weights[f"RS{i}"] * rs_data.get(f"RS{i}", 0)
                for i in range(1, 5)
            )
            
            return {
                "ticker": ticker,
                "date": latest_date,
                "weighted_score": weighted_score,
                "sector": metadata.get("sector"),
                "industry": metadata.get("industry")
            }
            
        except Exception as e:
            logging.error(f"Error calculating RS score for {ticker}: {str(e)}")
            return None

    def normalize_scores(self, scores, groupby=None):
        """
        Normalize RS scores to a 1-99 range using percentile ranking.
        If groupby is provided, normalize within each group.
        """
        if not scores:
            return []
            
        # Convert to DataFrame for easier grouping
        df = pd.DataFrame(scores)
        
        if groupby:
            # Group by the specified field and calculate percentile ranks within each group
            df['normalized_score'] = df.groupby(groupby)['weighted_score'].transform(
                lambda x: x.rank(pct=True) * 98 + 1
            ).round().astype(int)
        else:
            # Calculate overall percentile ranks
            df['normalized_score'] = (
                df['weighted_score'].rank(pct=True) * 98 + 1
            ).round().astype(int)
            
        # Convert back to list of dictionaries
        return df.to_dict('records')

    def update_database(self, scores):
        """Update the database with only the essential RS scores and ranks."""
        if not scores:
            return
            
        # Convert to DataFrame for easier manipulation
        df = pd.DataFrame(scores)
        
        # Calculate ranks for different groupings
        df['market_rank'] = df['market_score'].rank(ascending=False, method='min').astype(int)
        df['sector_rank'] = df.groupby('sector')['sector_score'].rank(ascending=False, method='min').astype(int)
        df['industry_rank'] = df.groupby('industry')['industry_score'].rank(ascending=False, method='min').astype(int)
        
        # Prepare bulk operations with only essential fields
        bulk_operations = []
        for _, row in df.iterrows():
            bulk_operations.append(
                UpdateOne(
                    {
                        "ticker": row['ticker'],
                        "date": row['date']
                    },
                    {
                        "$set": {
                            # Market score and rank
                            "rs_score_market": int(row['market_score']),
                            "rs_rank_market": int(row['market_rank']),
                            
                            # Sector score and rank
                            "rs_score_sector": int(row['sector_score']),
                            "rs_rank_sector": int(row['sector_rank']),
                            
                            # Industry score and rank
                            "rs_score_industry": int(row['industry_score']),
                            "rs_rank_industry": int(row['industry_rank'])
                        }
                    },
                    upsert=True
                )
            )
        
        # Execute bulk update
        if bulk_operations:
            result = self.indicators_collection.bulk_write(bulk_operations, ordered=False)
            logging.info(f"Updated {result.modified_count} documents")

    def calculate_all_scores(self):
        """Calculate and update RS scores for all stocks."""
        try:
            # Get all tickers
            tickers = self.ohlcv_collection.distinct("ticker")
            logging.info(f"Processing {len(tickers)} tickers")
            
            # Calculate weighted scores
            scores = []
            for ticker in tickers:
                result = self.calculate_weighted_rs_score(ticker)
                if result:
                    scores.append(result)
            
            # Calculate different types of scores
            market_scores = self.normalize_scores(scores)
            sector_scores = self.normalize_scores(scores, groupby='sector')
            industry_scores = self.normalize_scores(scores, groupby='industry')
            
            # Combine all scores
            final_scores = []
            for market, sector, industry in zip(market_scores, sector_scores, industry_scores):
                score_data = market.copy()
                score_data['market_score'] = market['normalized_score']
                score_data['sector_score'] = sector['normalized_score']
                score_data['industry_score'] = industry['normalized_score']
                final_scores.append(score_data)
            
            # Update database
            self.update_database(final_scores)
            
            logging.info("RS score calculation completed successfully")
            
        except Exception as e:
            logging.error(f"Error in calculate_all_scores: {str(e)}")

if __name__ == "__main__":
    calculator = RSScoreCalculator()
    calculator.calculate_all_scores()