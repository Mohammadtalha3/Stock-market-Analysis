import pandas as pd 
from ingestion_pipeline.db_config import Connection

db= Connection()

db.connect()


data= pd.read_csv("daily_stock_data.csv")

query= """ INSERT INTO stock_prices_daily (stock_symbol,timestamp, open, high, low, close, volume) VALUES (%s, %s, %s, %s, %s, %s, %s)"""

for index, row in data.iterrows():
    data= (row['stock_symbol'], row['timestamp'], row['open'], row['high'], row['low'], row['close'], row['volume'])

    db.execute(query, data)

