from openbb_terminal.sdk import openbb

openbb.stocks.options.vsurf('SPY')

openbb.stocks.options.vsurf_chart('SPY')

openbb.stocks.options.pcr_chart('SPY')

openbb.stocks.options.unu_chart()

openbb.stocks.options.info_chart('SPY')

openbb.stocks.options.oi(symbol='SPY',expiry="10")

openbb.stocks.options.hist(symbol='SPY',exp="2021-01-15",strike="450")

df