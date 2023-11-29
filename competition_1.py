# Competition Rules:
# Sharpe Ratio is the Evaluation Criteria
# Reset your Paper Trading Account to $10,000
# Start: November 27th
# End: December 15th
# Assets: Stocks Only
# Guidance:
# Stock screener to find your universe of investment assets
# Volatility targeting
# Rebalance daily
# Code to rebalance is not given (Hint - Zipline Order Target Percent)

import pandas as pd
import numpy as np
import time
import threading

from openbb_terminal.sdk import openbb
import riskfolio as rp

from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.order import *

from zipline.api import order_target_percent
# https://zipline.ml4trading.io/api-reference.html#zipline.api.order_target_percent

break_out_stocks = openbb.stocks.screener.screener_data("break_out_stocks")
# value_growth = openbb.stocks.screener.screener_data("value_growth")
# bull_runs_over_10pct = openbb.stocks.screener.screener_data("bull_runs_over_10pct")
# short_squeeze_scan = openbb.stocks.screener.screener_data("short_squeeze_scan")
# growth_stocks = openbb.stocks.screener.screener_data("growth_stocks")
# buffett_like = openbb.stocks.screener.screener_data("buffett_like", data_type='financial')

break_out_stocks    
# value_growth
# bull_runs_over_10pct
# short_squeeze_scan



# new_highs = openbb.stocks.screener.screener_data("new_high")
# new_highs

# port_data = new_highs[
#     (new_highs.Price > 15) &
#     (new_highs.Country == "USA")
# ]

port_data = break_out_stocks

tickers = port_data.Ticker.tolist()

data = openbb.economy.index(tickers, start_date="2016-01-01")

returns = data.pct_change()[1:]

returns.dropna(how="any", axis=1, inplace=True)

port = rp.Portfolio(returns=returns)

method_mu = "hist"

method_cov = "hist"

port.assets_stats(method_mu=method_mu, method_cov=method_cov, d=0.94)

port.lowerret = 0.00008
w_rp_c = port.rp_optimization(
    model="Classic",
    rm="MV",
    hist=True,
    rf=0.05,
    b=None
)

ax = rp.plot_risk_con(
    w_rp_c,
    cov=port.cov,
    returns=port.returns,
    rm="MV",
    rf=0.05
)

port_val = 60_000

w_rp_c["invest_amt"] = w_rp_c * port_val

w_rp_c["last_price"] = data.iloc[-1]

w_rp_c["last_price"]

w_rp_c["shares"] = (w_rp_c.invest_amt / w_rp_c.last_price).astype(int)

(w_rp_c["shares"] * w_rp_c["last_price"]).sum()


class IBapi(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)
        self.pos_df = pd.DataFrame(columns=['Account', 'Symbol', 'SecType',
                                            'Currency', 'Position', 'Avg cost'])    
        
    def nextValidId(self, orderId):
        super().nextValidId(orderId)
        self.nextOrderId = orderId
        
    def position(self, account, contract, position, avgCost):
        super().position(account, contract, position, avgCost)
        # self.contract = contract
        dictionary = {"Account":account, "Symbol": contract.symbol, "SecType": contract.secType,
                        "Currency": contract.currency, "Position": position, "Avg cost": avgCost}
        if self.pos_df["Symbol"].str.contains(contract.symbol).any():
            self.pos_df.loc[self.pos_df["Symbol"]==contract.symbol,"Position"] = position
            self.pos_df.loc[self.pos_df["Symbol"]==contract.symbol,"Avg cost"] = avgCost
        else:
            self.pos_df = pd.concat([self.pos_df,pd.DataFrame(dictionary, index=[0])], ignore_index=True)
            
def stock_contract(symbol, secType="STK", exchange="SMART", currency="USD"):
    contract = Contract()
    contract.symbol = symbol
    contract.secType = secType
    contract.exchange = exchange
    contract.currency = currency

    return contract

def submit_order(contract, direction, qty=100, orderType="MKT", transmit=True):
    order = Order()
    order.action = direction
    order.totalQuantity = qty
    order.orderType = orderType
    order.transmit = transmit
    order.eTradeOnly = ""
    order.firmQuoteOnly = ""
    # submit order
    app.placeOrder(app.nextOrderId, contract, order)
    app.nextOrderId += 1

    
def run_loop():
    app.run()

app = IBapi()
app.connect('127.0.0.1', 7497, 123)
app.nextOrderId = None

api_thread = threading.Thread(target=run_loop, daemon=True)
api_thread.start()

while True:
    if isinstance(app.nextOrderId, int):
        print("Connected")
        break
    else:
        print("Waiting")
        time.sleep(1)
        


app.reqPositions()
time.sleep(1)
pos_df = app.pos_df


df_change = (pd.merge(w_rp_c, pos_df.set_index('Symbol'),left_index=True,right_index=True, how="outer")
    .fillna(0)
    .assign(change = lambda x: x.shares - x.Position)
    .assign(buy = lambda x: np.where(x.change > 0, x.change, 0))
    .assign(sell = lambda x: np.where(x.change < 0, abs(x.change), 0))

)

df_buy = df_change[df_change.buy > 0]
df_sell = df_change[df_change.sell > 0]

for row in df_sell.itertuples():
    contract = stock_contract(row.Index)
    submit_order(contract, direction="SELL", qty=row.sell)

for row in df_buy.itertuples():
    contract = stock_contract(row.Index)
    submit_order(contract, direction="BUY", qty=row.buy)

# order_target_percent(self, asset, target, limit_price=None, stop_price=None, style=None)

# for row in w_rp_c.itertuples():
#     contract = stock_contract(row.Index)
#     submit_order(contract, direction="BUY", qty=row.shares)


app.disconnect()
    
# Calculate sharpe ratio
# https://www.investopedia.com/terms/s/sharperatio.asp

# standard deviation of returns










        
