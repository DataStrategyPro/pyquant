# https://www.investopedia.com/terms/forex/n/nzd-usd-new-zealand-dollar-us-dollar-currency-pair.asp

from openbb_terminal.sdk import openbb
import pandas as pd

SPY = openbb.stocks.load('SPY',start_date='2019-01-01')
NZD = openbb.forex.load('NZD','USD',start_date='2019-01-01')

df = pd.merge(SPY,NZD,on='date',how='inner',suffixes=('_SPY','_NZD'))
df['fx_adj_close'] = df['Close_SPY'] * df['Close_NZD']
df['spy_return'] = df['Close_SPY'].pct_change()
df['nzd_return'] = df['Close_NZD'].pct_change()
df['fx_adj_close_return'] = df['fx_adj_close'].pct_change()

df['spy_cumulative_return'] = (1 + df['spy_return']).cumprod() - 1
df['nzd_cumulative_return'] = (1 + df['nzd_return']).cumprod() - 1
df['fx_adj_close_cumulative_return'] = (1 + df['fx_adj_close_return']).cumprod() - 1

# Plot cummulative returns
df[['spy_cumulative_return','nzd_cumulative_return','fx_adj_close_cumulative_return']].plot()

# Review return distributions
df[['spy_return','nzd_return','fx_adj_close_return']].dropna().hist(bins=100, sharex=True, figsize=(12,8))

# calculate sharpe ratio
rf = 0.05 / 252
nzd_sharpe = (df['nzd_return'].mean() - rf) / df['nzd_return'].std()
nzd_sharpe
spy_sharpe = (df['spy_return'].mean() - rf) / df['spy_return'].std()
spy_sharpe
fx_adj_sharpe = (df['fx_adj_close_return'].mean() - rf) / df['fx_adj_close_return'].std()
fx_adj_sharpe

# correlation between spy and nzd returns
df[['spy_return','nzd_return']].corr()

df[['spy_return','nzd_return','fx_adj_close_return']]