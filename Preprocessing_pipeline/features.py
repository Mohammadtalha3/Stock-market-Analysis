import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from database.db_config import Connection
import pandas as pd

db= Connection()

db.connect()


def features():
    
        feature_query = """
        WITH prev_day AS (
            SELECT 
                stock_symbol,
                timestamp,
                close AS prev_day_close,
                daily_return AS  prev_day_return
            
            FROM  stock_prices_daily
        
        )
            SELECT
                d.stock_symbol,
                d.timestamp,
                d.close,
                d.volume,
                d.ma_5,
                d.ma_10,
                d.ma_50,
                d.daily_return,
                d.intraday_volatility,
                prev_day.prev_day_close,
                prev_day.prev_day_return,
                (d.close - prev_day.prev_day_close) AS  price_diff,
                CASE 
                    WHEN d.daily_return > 0 THEN  1
                    ELSE 0
                END AS target
            
            FROM  stock_prices_daily d
        
            LEFT JOIN prev_day
        
            ON d.stock_symbol = prev_day.stock_symbol AND d.timestamp = prev_day.timestamp
            ORDER BY d.stock_symbol, d.timestamp;        
        """

        return feature_query


ff= features()

data= db.fetch(ff)

df= pd.DataFrame(data)

print(df)
