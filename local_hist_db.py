import pandas as pd
import numpy as np
from openbb import obb
from arcticdb import Arctic, QueryBuilder
import pytz
from datetime import datetime

def today_ny():
    # Define time zones
    nz_timezone = pytz.timezone('Pacific/Auckland')  # New Zealand Time Zone
    ny_timezone = pytz.timezone('America/New_York')   # New York Time Zone

    # Get current time in NZ
    current_time_nz = datetime.now(nz_timezone)
    print("Current time in NZ:", current_time_nz.strftime('%Y-%m-%d %H:%M:%S %Z'))

    # Convert to NY time
    current_time_ny = current_time_nz.astimezone(ny_timezone)
    print("Current time in NY:", current_time_ny.strftime('%Y-%m-%d %H:%M:%S %Z'))
    
    return np.datetime64(current_time_ny.strftime('%Y-%m-%d')) 
    
today = today_ny()

def get_data(symbol, interval='DAILY'):
    ac = Arctic("lmdb:///data/arcticdb")
    lib = ac[interval]
    try:
        # Try loading from the local DB
        data_local = lib.read(symbol)
        first_date = data_local.data.head(1).index
        last_date = data_local.data.tail(1).index
        str_date_range = f"{np.datetime_as_string(first_date, unit='D')[0]} to {np.datetime_as_string(last_date, unit='D')[0]}"
        # Get the latest data
        if last_date < today:
            print(f"{symbol} Retreiving latest data up to {today}. Current data range {str_date_range}")
            data_remote = obb.equity.price.historical(symbol=symbol, start_date = last_date, interval="1d", provider="yfinance")    
            # Write the new data to the DB
            lib.write(symbol, data_remote.to_df())
        else:
            print(f"{symbol} Data up to date {str_date_range}")
    except:
        # get all of the data if the symbol doesn't yet exist
        data_remote = obb.equity.price.historical(symbol=symbol, interval="1d", provider="yfinance")
        # Write the new data to the DB
        lib.write(symbol, data_remote.to_df())
    
    # Load all the data from the DB for analysis
    return lib.read(symbol)

data = get_data("GOOG")

undervalued_growth = obb.equity.discovery.undervalued_growth(sort="desc")

undervalued_growth.to_df()

for sym in undervalued_growth.to_df().symbol:
    get_data(sym)


lib = ac['DAILY']
lib.list_symbols()
    
data = openbb.stocks.load('AAPL', start_date='2020-01-01')



sp500 = obb.index.constituents(index="sp500")
