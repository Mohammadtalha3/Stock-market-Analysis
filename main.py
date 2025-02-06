from fastapi import FastAPI
from pydantic import BaseModel
import pickle
import numpy as np
import pandas as pd

app = FastAPI()

# Load the trained Logistic Regression model (ensure the model is saved as a .pkl file)
model = pickle.load(open('D:\Stock-market-Analysis\Experiment\logistic_regression_model.pkl', 'rb'))

# Define the request body using Pydantic
class StockData(BaseModel):
    data: list  # List of feature values (from preprocessing steps)

@app.post("/predict")
def predict(stock_data: StockData):
    # Convert the list to a numpy array and reshape for prediction
    feature_vector = np.array(stock_data.data).reshape(1, -1)  # Reshape as needed
    
    # Make prediction
    prediction = model.predict(feature_vector)
    
    return {"prediction": int(prediction[0])}
