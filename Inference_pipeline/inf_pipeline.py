import pandas as pd
import numpy as np
import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database.db_config import Connection

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
    
    def preprocessing(self,data):
        pass 


dv= Inference()
gg=dv.Inference_fetching_daily()
print(gg)



        






