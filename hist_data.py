import pandas as pd 
import numpy as np  
import requests
import os
import csv

APi_key = os.getenv("Vantage_API")

def load_data(Api_key):
    url = f'https://www.alphavantage.co/query?function=HISTORICAL_OPTIONS&symbol=IBM&date=2020-11-15&apikey={Api_key}'
    r  = requests.get(url)
    data = r.json()
    data = pd.DataFrame(data)
    data.to_csv('vantage_data.csv')

    print(data)


load_data(APi_key)

