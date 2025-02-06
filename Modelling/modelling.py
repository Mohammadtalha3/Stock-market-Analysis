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

df['intraday_volatility'].fillna(df['intraday_volatility'].mean(), inplace=True)
df['daily_return'].fillna(0, inplace=True)
df['prev_day_return'].fillna(0, inplace=True)
df.dropna(inplace=True)

df["price_momentum"] = df["price_diff"] / df["prev_day_close"]  # Price momentum ratio
df["volatility_ratio"] = df["intraday_volatility"] / df["ma_10"]  # Normalized volatility
df["volume_change"] = df["volume"].pct_change()  # Percentage change in volume




# Select features for training
features = ["prev_day_close", "ma_5", "ma_10", "ma_50", "daily_return", "intraday_volatility", "price_momentum", "volatility_ratio", "volume_change"]
X = df[features]
y = df["target"]  # Target: 1 (up), 0 (down)

# Train-Test Split
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Train a RandomForest Model
model = RandomForestClassifier(n_estimators=100, random_state=42)
model.fit(X_train, y_train)

# Evaluate model
y_pred = model.predict(X_test)
print(f"Accuracy: {accuracy_score(y_test, y_pred) * 100:.2f}%")


import seaborn as sns
import matplotlib.pyplot as plt


plt.figure(figsize=(10,6))
sns.heatmap(df.corr(), annot=True, cmap="coolwarm", fmt=".2f")
plt.show()





# %%
