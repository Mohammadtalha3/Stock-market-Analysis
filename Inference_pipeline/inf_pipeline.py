import pickle
import os, sys
import joblib
import pandas as pd
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database.db_config import Connection
from sklearn.preprocessing import StandardScaler
from Preprocessing_pipeline.features import fetch_features,calculate_technical_indicators

db= Connection()

class Inference:
    def Inference_fetching_daily(self):
        
        query ="""
        SELECT * from stock_prices_daily
        ORDER BY timestamp DESC
        LIMIT 1;
        """
        db.connect()
        data=db.fetch(query)
        return data


    def Inference_fetching_intraday(self):
        query="""
        SELECT * FROM  stock_prices_intraday
        ORDER BY timestamp DESC
        LIMIT 100;
        """
        db.connect()
        data= db.fetch(query)
        
        return data 
    
    def preprocessing(self):
        raw_data=fetch_features()
        
        df = pd.DataFrame(raw_data, columns=[
            'stock_symbol', 'timestamp', 'close', 'high', 'low', 'volume', 'ma_5', 
            'ma_10', 'ma_50', 'daily_return', 'intraday_volatility', 'prev_day_close', 
            'prev_day_return', 'price_diff', 'target'
            ])
        print('This is inf data ', df)
        # df.to_csv('inference_2.csv')
        df = calculate_technical_indicators(df)
        print('this is rsi indicator ', df.columns)
        print('this is rsi', df)
        
        numerical_features = ['close', 'volume', 'ma_5', 'ma_10', 'ma_50', 'daily_return', 'intraday_volatility', 
                      'price_diff', 'price_momentum', 'volatility_ratio', 'volume_change', 'ema_10', 'ema_20', 
                      'rsi', 'roc', 'atr', 'bollinger_upper', 'bollinger_lower', 'pvt', 'obv', 'cmf', 'macd', 
                      'macd_signal', 'sar', 'prev_day_volume', 'skewness', 'kurtosis', 'autocorrelation_1', 
                      'autocorrelation_5']
        # print('this  is num features',df[numerical_features])

        scaler = StandardScaler()
        df[numerical_features] = scaler.fit_transform(df[numerical_features])

        print('this os scaled data', df[numerical_features])

        return df[numerical_features]


    # def make_prediction(self):
    # # Preprocess the DataFrame
    #     model_path="D:\Stock-market-Analysis\Experiment\logistic_regression_model.pkl"
    #     df_preprocessed = self.preprocessing()

    #     # Get the latest row (most recent data)
    #     latest_row = df_preprocessed.iloc[-1:]

    #     # Load the trained model
    #     model = joblib.load(model_path)

    #     # Make a prediction on the latest row
    #     prediction = model.predict(latest_row)

    #     # Return or print the prediction
    #     print(f"Prediction for the latest data: {prediction}")

    #     return prediction
    

    
    # # Output result








        






