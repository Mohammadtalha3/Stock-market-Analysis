# from database.db_config import Connection

import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from database.db_config import Connection


db= Connection()

db.connect()


query= """SELECT 
    stock_symbol,
    DATE(timestamp) AS date,
    AVG(close) AS avg_intraday_price,
    MAX(high) AS max_intraday_high,
    MIN(low) AS min_intraday_low,
    SUM(volume) AS total_intraday_volume,
    STDDEV(close) AS intraday_volatility
FROM stock_prices_intraday
GROUP BY stock_symbol, DATE(timestamp);"""


data=db.fetch(query=query)


print(data)
