import pandas as pd
import numpy as numpy 
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import  accuracy_score, classification_report
import os 
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from Preprocessing_pipeline.features import features
from database.db_config import Connection

db= Connection()

connection= db.connect()

ff= features()


data= db.fetch(ff)

df= pd.DataFrame(data, columns=[
    'stock_symbol',          # Stock ticker symbol
    'timestamp',             # Date/time of the stock data
    'close',                 # Closing price of the stock
    'volume',                # Trading volume of the stock
    'ma_5',                  # 5-day moving average
    'ma_10',                 # 10-day moving average
    'ma_50',                 # 50-day moving average
    'daily_return',          # Daily return percentage
    'intraday_volatility',   # Intraday volatility measure
    'prev_day_close',        # Previous day's closing price
    'prev_day_return',       # Previous day's return percentage
    'price_diff',            # Difference between current close and previous close
    'target'                 # Binary target (1 if daily_return > 0, else 0)
]
)

df.to_csv('final_model.csv')
df.describe()
print(df.isna().sum())

print((df['target']==0).sum())

