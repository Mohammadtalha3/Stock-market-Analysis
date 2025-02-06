# from database.db_config import Connection
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from database.db_config import Connection


db= Connection()

db.connect()


query= """

SELECT d.close, d.open,d.low, d.high,d.volume, d.stock_symbol, d.timestamp,
    i.intraday_closing_avg, i.max_intraday_high, i.min_intrday_low, 
    i.intraday_volatility, i.sum_intrday_volume,

    (close - LAG(d.close) OVER (PARTITION BY d.stock_symbol ORDER BY d.timestamp ))/
    LAG(close) OVER (PARTITION BY d.stock_symbol ORDER BY d.timestamp) As daily_return,

    AVG(d.close) OVER (PARTITION BY d.stock_symbol ORDER BY d.timestamp ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) As ma_5,
    AVG (d.close) OVER (PARTITION BY d.stock_symbol ORDER BY d.timestamp ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) AS ma_10,
    AVG(d.close) OVER (PARTITION BY d.stock_symbol ORDER BY d.timestamp ROWS BETWEEN 49 PRECEDING AND CURRENT ROW) As ma_50

FROM stock_prices_daily d
LEFT JOIN(
SELECT
    stock_symbol,
    DATE(timestamp) AS date,
    AVG(close) AS intraday_closing_avg,
    MAX(high) AS max_intraday_high,
    MIN(low) AS min_intrday_low,
    SUM(volume) AS sum_intrday_volume,
    STDDEV(close)  AS intraday_volatility

FROM stock_prices_intraday 

GROUP BY stock_symbol ,DATE(timestamp)


) i

ON  d.stock_symbol =i.stock_symbol AND d.timestamp ::DAtE = i.date;


"""


data=db.fetch(query=query)


import pandas as pd

df=pd.DataFrame(data=data, columns=['close', 'open','low', 'high','volume', 'stock_symbol', 'timestamp',
    'intraday_closing_avg', 'max_intraday_high', 'min_intrday_low', 
    'intraday_volatility', 'sum_intrday_volume','Daily_return', 'ma_5', 'ma_10', 'ma_50'])


