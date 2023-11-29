# QUANT SCIENCE UNIVERSITY ----
# Advanced Quantitative Trading Application
# Crack Spread x Refiner Trade
# -----------------------------
# This program is a Streamlit app that allows the user to enter three futures contracts. 
# Disclaimer: Provided for educational purposes. Use at your own risk. 

# HOW TO USE:
# 1. Install Streamlit: pip install streamlit
# 2. Run the app: streamlit run crack.py

# Import libraries
import pandas as pd
import numpy as np
import streamlit as st
import plotly.graph_objects as go
from openbb_terminal.sdk import openbb
from datetime import datetime, timedelta


# Function to fetch data from OpenBB
def fetch_data_from_openbb(ticker, start_date, end_date, expiry=None):
    print(ticker, start_date, end_date)
    data = openbb.futures.historical(ticker, start_date, end_date)
    data.rename(columns={"Adj Close": ticker}, inplace=True)
    return data[ticker]


# Streamlit App
st.title("Modeling the Crack Spread x Refiner Trade")

# Calculate one year ago from today
one_year_ago = (datetime.now() - timedelta(days=365)).date()

# Date fields for start and end dates
start_date = st.date_input(
    "Start date:", value=one_year_ago, min_value=pd.to_datetime("2022-01-01").date()
)
end_date = st.date_input("End date:", min_value=start_date)

# Coerce to yyyy-mm-dd format
start_date_str = start_date.strftime("%Y-%m-%d")
end_date_str = end_date.strftime("%Y-%m-%d")

# Create columns for text fields and inline fields
col1_1, col1_2 = st.columns(2)
col2_1, col2_2 = st.columns(2)
col3_1, col3_2 = st.columns(2)

# Text fields for ticker symbols and inline fields for number values
ticker1 = col1_1.text_input("Enter first contract:", "HO")
number1 = col1_2.number_input("Contracts:", 1)

ticker2 = col2_1.text_input("Enter second contract:", "RB")
number2 = col2_2.number_input("Contracts:", 2)

ticker3 = col3_1.text_input("Enter third contract:", "CL")
number3 = col3_2.number_input("Contracts:", 3)

ticker4 = st.text_input("Enter refiner symbol:", "PSX")

# Submit button
if st.button("Submit"):
    # Fetch data based on tickers
    data1 = fetch_data_from_openbb(
        ticker1, start_date=start_date_str, end_date=end_date_str
    )
    data2 = fetch_data_from_openbb(
        ticker2, start_date=start_date_str, end_date=end_date_str
    )
    data3 = fetch_data_from_openbb(
        ticker3, start_date=start_date_str, end_date=end_date_str
    )
    crack = number1 * data1 + number2 * data2 - number3 * data3
    crack_ret = np.log(crack / crack.shift(1))

    refiner = openbb.stocks.load(
        ticker4, start_date=start_date_str, end_date=end_date_str
    )["Adj Close"]
    refiner_ret = np.log(refiner / refiner.shift(1))

    # Calculate the spread
    spread = crack - refiner

    # Calculate the rolling z score of spread
    spread_zscore = (spread - spread.mean()) / spread.std()

    # Plotting
    fig1 = go.Figure()
    fig2 = go.Figure()

    fig1.add_trace(
        go.Scatter(x=crack_ret.index, y=crack_ret.cumsum(), name="Crack spread")
    )
    fig1.add_trace(
        go.Scatter(x=refiner_ret.index, y=refiner_ret.cumsum(), name="Refiners")
    )

    fig1.update_layout(
        title="Crack spread versus refiners", yaxis_title="Compound change"
    )

    # Create subplot for z score
    fig2.add_trace(
        go.Scatter(x=spread_zscore.index, y=spread_zscore, name="Spread Z-Score")
    )

    fig2.update_layout(title="Spread Z-Score", yaxis_title="Z-Score")

    # Display both plots
    st.plotly_chart(fig1, use_container_width=True)
    st.plotly_chart(fig2, use_container_width=True)
