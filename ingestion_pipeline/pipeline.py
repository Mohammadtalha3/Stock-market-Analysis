import pandas as pd
import numpy as np
import os
import dotenv
import requests
from Database import Connection

db= Connection()

# Load environment variables
dotenv.load_dotenv()

def load_data():
    API_KEY = os.getenv("INTRA_DAY")  # Retrieve API key from .env file
    if not API_KEY:
        raise ValueError("API_KEY not found. Make sure it is set in the .env file.")
    # print('This is Api key ', API_KEY)

    url =f'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=IBM&interval=5min&apikey={API_KEY}'
    
    # Fetch data from API
    response = requests.get(url)
    
    # Check if the request was successful
    if response.status_code != 200:
        raise Exception(f"API request failed with status code {response.status_code}")
    
    data = response.json()
    print(data)

    dataframe=pd.DataFrame(data)
    import csv
    dt=dataframe.to_csv('stock_data.csv')
    print(dt)
    print(dataframe['Time Series (5min)'])
    db.connect()

    query= """ INSERT INTO stock_data (timestamp, open, high, low, close, volume) VALUES (%s, %s, %s, %s, %s, %s)"""

    for index, row in dataframe.iterrows():
        timestamp = index
        open = row['1. open']
        high = row['2. high']
        low = row['3. low']
        close = row['4. close']
        volume = row['5. volume']
        db.execute(query, (timestamp, open, high, low, close, volume))
        print(timestamp, open, high, low, close, volume)
    
    # db.execute(query)
    
    return dataframe  # Return data for further processing

# Example usage
load_data()
