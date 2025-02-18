import sys
import os
import numpy as np
import pandas as pd
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from database.db_config import Connection

# Database Connection
db = Connection()
db.connect()

def fetch_features():
    """Fetch raw stock features from the database"""
    feature_query = """
        WITH prev_day AS (
            SELECT 
                stock_symbol,
                timestamp,
                close AS prev_day_close,
                daily_return AS prev_day_return
            FROM stock_prices_daily
        )
        SELECT
            d.stock_symbol,
            d.timestamp,
            d.close,
            d.high,
            d.low,
            d.volume,
            d.ma_5,
            d.ma_10,
            d.ma_50,
            d.daily_return, 
            d.intraday_volatility,
            prev_day.prev_day_close,
            prev_day.prev_day_return,
            (d.close - prev_day.prev_day_close) AS price_diff,
            CASE 
                WHEN d.daily_return > 0 THEN 1
                ELSE 0
            END AS target
        FROM stock_prices_daily d
        LEFT JOIN prev_day
        ON d.stock_symbol = prev_day.stock_symbol AND d.timestamp = prev_day.timestamp
        ORDER BY d.stock_symbol, d.timestamp DESC
        
    """
    return db.fetch(feature_query)


def calculate_technical_indicators(df):
    """Generate technical indicators for stock price analysis"""
    print('Feature technical indicator called')
    
    # Handle missing values
    df['intraday_volatility'].fillna(df['intraday_volatility'].mean(), inplace=True)
    df['daily_return'].fillna(0, inplace=True)
    df['prev_day_return'].fillna(0, inplace=True)
    df.dropna(inplace=True)

    # Derived Features
    df["price_momentum"] = df["price_diff"] / df["prev_day_close"]
    df["volatility_ratio"] = df["intraday_volatility"] / df["ma_10"]
    df["volume_change"] = df["volume"].pct_change()

    # Moving Averages & EMA
    df['ema_10'] = df['close'].ewm(span=10, adjust=False).mean()
    df['ema_20'] = df['close'].ewm(span=20, adjust=False).mean()

    # Relative Strength Index (RSI)
    def calculate_rsi(data, window=14):
        delta = data.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=window).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=window).mean()
        rs = gain / loss
        return 100 - (100 / (1 + rs))

    df['rsi'] = calculate_rsi(df['close'])

    # Rate of Change (ROC)
    df['roc'] = df['close'].pct_change(periods=10) * 100

    # Average True Range (ATR)
    df['high_low'] = df['high'] - df['low']
    df['high_close'] = (df['high'] - df['close'].shift()).abs()
    df['low_close'] = (df['low'] - df['close'].shift()).abs()
    df['tr'] = df[['high_low', 'high_close', 'low_close']].max(axis=1)
    df['atr'] = df['tr'].rolling(window=14).mean()

    # Bollinger Bands
    df['bollinger_upper'] = df['ma_10'] + (2 * df['atr'])
    df['bollinger_lower'] = df['ma_10'] - (2 * df['atr'])

    # Price-Volume Trend (PVT) & On-Balance Volume (OBV)
    df['pvt'] = (df['volume'] * df['daily_return']).cumsum()
    df['obv'] = (np.sign(df['daily_return']).shift(1) * df['volume']).cumsum()

    # Chaikin Money Flow (CMF)
    df['cmf'] = ((df['close'] - df['low']) - (df['high'] - df['close'])) / (df['high'] - df['low']) * df['volume']
    df['cmf'] = df['cmf'].rolling(window=20).mean()

    # MACD (Moving Average Convergence Divergence)
    df['ema_12'] = df['close'].ewm(span=12, adjust=False).mean()
    df['ema_26'] = df['close'].ewm(span=26, adjust=False).mean()
    df['macd'] = df['ema_12'] - df['ema_26']
    df['macd_signal'] = df['macd'].ewm(span=9, adjust=False).mean()

    # Parabolic SAR (Simplified)
    df['sar'] = df['close'].shift(1) + 0.02 * (df['close'] - df['close'].shift(1))

    # Lag Features
    df['prev_day_close'] = df['close'].shift(1)
    df['prev_day_volume'] = df['volume'].shift(1)
    df['prev_day_return'] = df['daily_return'].shift(1)

    # Time Features
    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
    df['day_of_week'] = df['timestamp'].dt.dayofweek
    df['month_of_year'] = df['timestamp'].dt.month

    # Statistical Features
    df['skewness'] = df['daily_return'].rolling(window=14).skew()
    df['kurtosis'] = df['daily_return'].rolling(window=14).kurt()

    # Autocorrelation Features
    df['autocorrelation_1'] = df['daily_return'].shift(1).corr(df['daily_return'])
    df['autocorrelation_5'] = df['daily_return'].shift(5).corr(df['daily_return'])

    # Drop NaNs after feature engineering
    df.dropna(inplace=True)

    return df


def main():
    """Main execution pipeline"""
    print("Fetching data from database...")
    raw_data = fetch_features()

    print("Converting data to DataFrame...")
    df = pd.DataFrame(raw_data, columns=[
    'stock_symbol', 'timestamp', 'close', 'high', 'low', 'volume', 'ma_5', 
    'ma_10', 'ma_50', 'daily_return', 'intraday_volatility', 'prev_day_close', 
    'prev_day_return', 'price_diff', 'target'
])

    print("Calculating technical indicators...")
    df = calculate_technical_indicators(df)

    print("Feature engineering complete. Sample Data:")
    
    print(df.columns)

    return df


if __name__ == "__main__":
    main()
