import numpy as np
import pandas as pd
import features as ft
import connectdb
import datetime
import stockplot as sp
import plotly.express as px
import matplotlib.pyplot as plt
#import imp
pd.options.mode.chained_assignment = None

dbconn = connectdb.connectdb()
dbconn.conn = dbconn.pgconnect()

ticker1 = 'ANZ'
ticker2 = 'WBC'
spreadroc = 10
rochigh =0.00
roclow =-0.00
target =0.1
stop =0.016
upper =0.07
lower =-0.07

query = """
SELECT * FROM asxminutedata where datetime > '31/10/2019' AND datetime < '29/02/2020' AND
ticker in ('""" + ticker1 + "','" + ticker2 + "')"

query2 = """
SELECT * FROM asxminutedata where datetime > '30/04/2020' and datetime < '31/08/2020' AND
ticker in ('""" + ticker1 + "','" + ticker2 + "')"

query3 = """
SELECT * FROM asxminutedata where datetime > '1/05/2020' and datetime < '26/10/2020' AND
ticker in ('""" + ticker1 + "','" + ticker2 + "')"

colnames = ['open', 'high', 'low', 'close', 'volume', 'value', 'count', 'datetime', 'ticker']
x = dbconn.pgquery(dbconn.conn, query3, None)
df = pd.DataFrame(x, columns=colnames).set_index('datetime')
df = df.sort_values(by=['ticker', 'datetime'], ascending=[True, True])
df['date'] = df.index.date
df['time'] = df.index.time


def addfeat(df):
    df = ft.addmatches(df)
    df = ft.vwap(df)
    df = ft.wap(df)
    df = ft.addsma(df, [30, 5], 'volume')
    df = ft.addsma(df, [5, 30], 'close')
    df = ft.avat(df)
    df = ft.zeds(df)
    df = ft.prevclose(df)
    df = ft.avat(df)
    return df
df = addfeat(df)
df['cumvol'] = df.groupby(['ticker','date'])['volume'].cumsum()

spreaddf = ft.spread(df, [ticker1, ticker2], ['close', 'volume', 'daychg','cavat5','cumvol'])
spreaddf['date'] = spreaddf.index.date
spreaddf['time'] = spreaddf.index.time
spreaddf = ft.spreadroc(spreaddf, [ticker1, ticker2], [spreadroc])


def spreadtraderoc(df, roc, col1, col2, ticker1,ticker2,rochigh, roclow, upper=0.005, lower=-0.005):
    data = df.copy()
    data['spread'] = data[col1] - data[col2]
    # data['signal'] = np.where((data[roc] > rochigh) & (data['spread'] > upper)& (data[ticker1 + 'cavat5']< data[ticker1 + 'cumvol'])
    #                 & (data[ticker2 + 'cavat5']< data[ticker2 + 'cumvol']), 'sell', np.where((data[roc] < roclow)
    #                 & (data['spread'] < lower) & (data[ticker1 + 'cavat5']< data[ticker1 + 'cumvol'])
    #                 & (data[ticker2 + 'cavat5']< data[ticker2 + 'cumvol']),'buy', ''))
    data['signal'] = np.where((data[roc] > rochigh) & (data['spread'] > upper), 'sell', np.where((data[roc] < roclow)
                    & (data['spread'] < lower),'buy', ''))

    return data
spreaddf = spreadtraderoc(spreaddf, 'spreadroc' + str(spreadroc), ticker1 + 'daychg', ticker2 + 'daychg',ticker1,ticker2,
                          rochigh=rochigh, roclow=-roclow, upper=upper, lower=lower)


# genetrate trades df
def generatetrades(df, target=0.003, stop=0.009, spread=[]):
    # takes a data frame with sell and buy signals, iterates through and opens and closes trades based on stop, target
    # and time rules. Only opens a trade when not already open. Return dataframe of trades. Needs a spreaddf with col
    # as spread for spread trades

    openlong = False
    openshort = False
    skip = True
    counter = 1
    data = df.copy()
    name1 = spread[0] + 'close'
    name2 = spread[1] + 'close'
    trades = pd.DataFrame(columns=['datetime', 'ticker', 'side', 'price', 'quantity', 'tradeid', 'date', 'state'])

    if spread:
        # need to rename spreadcol to 'close' so it works with iteration
        data.rename(columns={spread[0] + spread[1] + 'close': 'close'}, inplace=True)

    def createtrades(trades, side, openpos):

        if spread:
            price1 = getattr(row, name1)
            price2 = getattr(row, name2)
        else:
            price1 = row.close

        if side == 'sell' and openlong == False:
            quantity1 = -round(10000 / price1)
            state = 'short'
        elif side == 'buy' and openshort == False:
            quantity1 = round(10000 / price1)
            state = 'long'
        elif side == 'sell' and openlong == True:
            quantity1 = openpos[spread[0]]
            state = 'long'
        elif side == 'buy' and openshort == True:
            quantity1 = openpos[spread[0]]
            state = 'short'

        if spread and openshort == False and openlong == False:
            quantity2 = -round((quantity1 * price1) / price2)
        else:
            quantity2 = openpos[spread[1]]

        if not spread:
            tradedict = {'tradeid': counter, 'datetime': row.Index, 'ticker': row.ticker, 'side': side,
                         'price': row.close, 'quantity': quantity, 'state': state}
            trades = trades.append(tradedict, ignore_index=True)

        else:
            tradedict = {'tradeid': counter, 'datetime': row.Index, 'ticker': spread[0], 'side': side,
                         'price': price1, 'quantity': quantity1, 'state': state}
            trades = trades.append(tradedict, ignore_index=True)

            if side == 'buy':
                side = 'sell'
            else:
                side = 'buy'
            tradedict = {'tradeid': counter, 'datetime': row.Index, 'ticker': spread[1], 'side': side,
                         'price': price2, 'quantity': quantity2, 'state': state}

            trades = trades.append(tradedict, ignore_index=True)

        return trades

    for row in data.itertuples():
        if skip == False:
            if openlong == False and openshort == False:
                openpos = {}
                if row.signal == 'buy' and row.Index.time() < datetime.time(15, 50) and getattr(row, spread[
                                                                                                         0] + 'volume') > 0 \
                        and getattr(row, spread[1] + 'volume') > 0:
                    stoppx = round(row.close * (1 - stop), 4)
                    targetpx = round(row.close * (1 + target), 4)
                    trades = createtrades(trades, row.signal, openpos)
                    openlong = True
                    lasttrades = trades[-2:]
                    openpos = {lasttrades['ticker'].iloc[0]: lasttrades['quantity'].iloc[0],
                               lasttrades['ticker'].iloc[1]: lasttrades['quantity'].iloc[1]}
                    skip = True

                elif row.signal == 'sell' and row.Index.time() < datetime.time(15, 50) and getattr(row, spread[
                                                                                                            0] + 'volume') > 0 \
                        and getattr(row, spread[1] + 'volume') > 0:

                    stoppx = round(row.close * (1 + stop), 4)
                    targetpx = round(row.close * (1 - target), 4)
                    trades = createtrades(trades, row.signal, openpos)
                    openshort = True
                    lasttrades = trades[-2:]
                    openpos = {lasttrades['ticker'].iloc[0]: lasttrades['quantity'].iloc[0],
                               lasttrades['ticker'].iloc[1]: lasttrades['quantity'].iloc[1]}
                    skip = True

            elif openlong == True:
                if row.close < stoppx or row.close > targetpx or row.Index.time() > datetime.time(15, 50) and getattr(
                        row, spread[0] + 'volume') > 0 \
                        and getattr(row, spread[1] + 'volume') > 0:
                    trades = createtrades(trades, 'sell', openpos)
                    openlong = False
                    openpos = {}
                    stoppx = 0
                    targetpx = 0
                    counter += 1
                    skip = True

            elif openshort == True:
                if row.close > stoppx or row.close < targetpx or row.Index.time() > datetime.time(15, 50) and getattr(
                        row, spread[0] + 'volume') > 0 \
                        and getattr(row, spread[1] + 'volume') > 0:
                    trades = createtrades(trades, 'buy', openpos)
                    openshort = False
                    openpos = {}
                    stoppx = 0
                    targetpx = 0
                    counter += 1
                    skip = True
        else:
            skip = False

    trades['date'] = trades.datetime.dt.date
    return trades
trades = generatetrades(spreaddf, target=target, stop=stop, spread=[ticker1, ticker2])


# join trades with pricedf
def jointrades(tradesdf, fulldf):
    # join the trades df back to the full data df for ease of plotting backtest results
    def openpositionfilter(merged):
        # filters larger dataframe by when there is an open trade and fills down the trade id so we can groupby tradeid
        grouped = merged.groupby('tradeid')
        for name, group in grouped:

            mask = ((merged['datetime'] >= grouped.get_group(name).datetime.iloc[0]) & (
                    merged['datetime'] <= grouped.get_group(name).datetime.iloc[-1])
                    & (merged['ticker'].isin(grouped.get_group(name).ticker.unique())))
            if name == 1:
                filter = mask
            else:
                filter = mask | filter

        opentrade = merged.loc[filter]
        cols = ['tradeid', 'quantity', 'state']
        opentrade.loc[:, cols] = opentrade.loc[:, cols].ffill()
        tradeids = opentrade[['tradeid', 'datetime', 'ticker', 'quantity', 'state']]
        merged.drop(columns=['tradeid', 'quantity', 'state'], inplace=True)
        merged = pd.merge(left=merged, right=tradeids, left_on=['datetime', 'ticker'], right_on=['datetime', 'ticker'],
                          how='left')
        return merged

    filterdays = tradesdf[['ticker', 'date']].drop_duplicates()
    filtereddf = pd.merge(left=fulldf.reset_index(), right=filterdays, how='inner', left_on=['date', 'ticker'],
                          right_on=['date', 'ticker'])
    filtereddf.drop(columns='date', inplace=True)
    merged = pd.merge(left=filtereddf, right=tradesdf, how='left', left_on=['datetime', 'ticker'],
                      right_on=['datetime', 'ticker']).drop_duplicates()  # .sort_values(by = 'datetime')
    merged = openpositionfilter(merged)
    merged['date'] = merged.datetime.dt.date
    return merged
fulldf = jointrades(trades, df)

fulldf = fulldf.sort_values(by=['tradeid', 'ticker', 'datetime'])


def backteststats(df):
    df['tradevalue'] = df['quantity'] * df['close']
    df['pl'] = df.groupby(['tradeid', 'ticker'])['tradevalue'].diff(1)
    df['brokerage'] = np.where((df['side'] == 'buy') | (df['side'] == 'sell'), df['tradevalue'].abs() * 0.0006, 0)
    df['pl'].fillna(0, inplace=True)
    df['netpl'] = df['pl'] - df['brokerage']
    return df
fulldf = backteststats(fulldf)

signals = {'Ticker1':[],'Ticker2':[],'exp':[],'trades':[],'winrate':[],'band':[],'target':[],'stop':[],'spreadroc10':[]}

netpl = fulldf.groupby('tradeid').sum()
winrate = len(netpl[netpl['netpl'] > 0]) / len(netpl)

fulldf['netplpertrade'] = fulldf.groupby('tradeid')['netpl'].transform('sum')
fulldf['exp'] = fulldf.groupby('tradeid')['netpl'].sum().mean()



signals['Ticker1'].append(ticker1)
signals['Ticker2'].append(ticker2)
signals['exp'].append(fulldf.groupby('tradeid')['netpl'].sum().mean())
signals['trades'].append(fulldf['tradeid'].max())
signals['winrate'].append(winrate)
signals['band'].append(upper)
signals['target'].append(target)
signals['stop'].append(stop)
signals['spreadroc10'].append(rochigh)

print(signals)
#imp.reload(sp)
i = df.merge(fulldf)
t = sp.stockplots(i)
t.backtestplot(spread = [ticker1,ticker2])
