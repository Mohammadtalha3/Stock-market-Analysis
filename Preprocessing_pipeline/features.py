import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from database.db_config import Connection
import pandas as pd

db= Connection()

db.connect()



query= "SELECT * FROM stock_features;"

data=db.fetch(query=query)

pd.DataFrame(data).to_csv('stock_features.csv', index=False)
