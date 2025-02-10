import pandas as pd
import numpy as np
import os
import dotenv
import requests
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database.db_config import Connection
from database.db_queries import Daily_stock_fetch, Intra_day_stock_fetch, daily_hypertable, intraday_hypertable
import json

db= Connection()


# Load environment variables
dotenv.load_dotenv()



def load_daily_Data():
    API_KEY = os.getenv("Daily_Data")
    url= f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=IBM&outputsize=full&apikey={API_KEY}'
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f"API request failed with status code {response.status_code}")
    data = response.json()

    stock_symbol = 'IBM'


    # for date, values in data['Meta Data'].items():
    #     # print(f"Stock Symbol: {values['2. Symbol']}")
    #     print(date)
    #     stock_symbol = values['2. Symbol']

    records= []

    for date,values in data['Time Series (Daily)'].items():
        try:
            open_price = float(values["1. open"])
            high_price = float(values["2. high"])
            low_price = float(values["3. low"])
            close_price = float(values["4. close"])
            volume = int(values["5. volume"])
            records.append([stock_symbol,date, open_price, high_price, low_price, close_price, volume])
        except (KeyError, ValueError, TypeError) as e:
            print(f"Skipping malformed entry for {date}: {values} - Error: {e}")
    

    df = pd.DataFrame(records, columns=["stock_symbol", "timestamp", "open", "high", "low", "close", "volume"])
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df.to_csv("daily_stock_data_automated.csv", index=False)
    db.connect()
    query= """ INSERT INTO stock_prices_daily (stock_symbol,timestamp, open, high, low, close, volume) VALUES (%s, %s, %s, %s, %s, %s, %s)"""
    for index, row in df.iterrows():
        try:
            timestamp=row["timestamp"]
            open = row['open']
            high = row['high']
            low = row['low']
            close = row['close']
            volume = row['volume']
            db.execute(query, (stock_symbol,timestamp, open, high, low, close, volume))
        except Exception as e:
            print(f"❌ Error inserting row {row['timestamp']}: {e}")
            db.rollback()
        print(timestamp, open, high, low, close, volume)
    
    return df


def load_intraday_Data():
    API_KEY= os.getenv("Intraday_Data")
    url= f'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=IBM&interval=5min&apikey={API_KEY}'
    response= requests.get(url)

    data=response.json()
    
    stock_symbol= 'IBM'

    records= []
    for date,values in data['Time Series (5min)'].items():
        try:
    
            open_price = float(values["1. open"])
            high_price = float(values["2. high"])
            low_price = float(values["3. low"])
            close_price = float(values["4. close"])
            volume = int(values["5. volume"])
            records.append([stock_symbol,date, open_price, high_price, low_price, close_price, volume])
        except (KeyError, ValueError, TypeError) as e:
            print(f"Skipping malformed entry for {date}: {values} - Error: {e}")
    
    df = pd.DataFrame(records, columns=["stock_symbol", "timestamp", "open", "high", "low", "close", "volume"])
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df.to_csv("intraday_stock_data-automated.csv", index=False)
    db.connect()
    query= """ INSERT INTO stock_prices_intraday (stock_symbol,timestamp, open, high, low, close, volume) VALUES (%s, %s, %s, %s, %s, %s, %s)"""
    for index, row in df.iterrows():
        try:
            timestamp=row["timestamp"]
            open_price  = row['open']
            high_price  = row['high']
            low_price = row['low']
            close_price = row['close']
            volume = row['volume']
            db.execute(query, (stock_symbol,timestamp, open_price , high_price, low_price, close_price, volume))
        except Exception as e:
            print(f"❌ Error inserting row {row['timestamp']}: {e}")
            # db.rollback()
        print(timestamp, open_price, high_price, low_price, close_price, volume)



















