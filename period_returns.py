import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import streamlit as st

df_stock_history = pd.read_csv('data/SPY.csv')

def plot_returns(df, period, tails):
    data = df[f'Return-{period}'].dropna()
    lower_tail = np.percentile(data, tails)
    upper_tail = np.percentile(data, 100-tails)
    plt.hist(data, bins=50)
    # show the upper and lower tail amount labels at the top of the chart
    plt.text(lower_tail, 50, f'{lower_tail:.4f}', rotation=90, va='top')
    plt.text(upper_tail, 50, f'{upper_tail:.4f}', rotation=90, va='top')
    plt.axvline(lower_tail, color='r', linestyle='dashed', linewidth=2)
    plt.axvline(upper_tail, color='r', linestyle='dashed', linewidth=2)
    return plt

# Streamlit code
st.title('SPY Period Return Distribution')

period = st.slider('Select a period', min_value=1, max_value=45, value=1)
tails = st.slider('Select tails', min_value=0, max_value=100, value=5)
plt = plot_returns(df_stock_history, period, tails)
st.pyplot(plt)

