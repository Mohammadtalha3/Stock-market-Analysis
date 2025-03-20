import streamlit as st
import pandas as pd
import joblib
# from Inference import Inference  # Import the inference pipeline
from Inference_pipeline.inf_pipeline import Inference

# Load the model
model_path = "D:\Stock-market-Analysis\Experiment\logistic_regression_model.pkl"
model = joblib.load(model_path)

# Streamlit App
st.title("📈 Stock Market Prediction")

# Fetch live data
inference = Inference()
df = inference.preprocessing()

# Get latest stock datac
latest_row = df.iloc[-1:]

# Make prediction
prediction = model.predict(latest_row)
predicted_movement = "📈 Up" if prediction[0] == 1 else "📉 Down"

# Display results
st.subheader("📊 Latest Prediction")
st.write(f"**Predicted Movement:** {predicted_movement}")
st.write("Latest Stock Data:")
st.dataframe(latest_row)

# Plot historical data (last 30 days)
st.subheader("📉 Stock Price Trend (Last 30 Days)")
df['timestamp'] = pd.to_datetime(df['timestamp'])
df = df.sort_values(by='timestamp')

st.line_chart(df.set_index("timestamp")["close"])
