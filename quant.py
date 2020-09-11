import connectdb
import plotly.express as px
from plotly.offline import plot
import scipy
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import stockplot as sp
import imp
pd.options.mode.chained_assignment = None
import filters
import features as ft
import datetime
import pyfinance as pf
imp.reload(ft)
imp.reload(sp)
dbconn = connectdb.connectdb()
dbconn.conn = dbconn.pgconnect()

query = """
SELECT * FROM asxminutedata where datetime > '06/05/2019' AND datetime < '31/08/2020' AND
ticker in ('ANZ','WBC') 
"""
query2 = """
SELECT * FROM asxminutedata where datetime > '30/05/2020' and datetime < '31/08/2020' AND
ticker in ('ANZ','WBC') 
"""
colnames = ['open','high','low','close','volume','value','count','datetime','ticker']
x = dbconn.pgquery(dbconn.conn,query,None)
df = pd.DataFrame(x, columns= colnames).set_index('datetime')
df = df.sort_values(by = ['ticker', 'datetime'], ascending = [True, True])
df['date']  = df.index.date
df['time'] = df.index.time

#add features
def addfeat(df):
    df = ft.addmatches(df)
    df = ft.vwap(df)
    df = ft.wap(df)
    df = ft.addsma(df,[30,5],'volume')
    df = ft.addsma(df,[5,30],'close')
    df = ft.avat(df)
    df = ft.zeds(df)
    df = ft.prevclose(df)
    df = ft.avat(df)
    return df
df = addfeat(df)
spreaddf = ft.spread(df,['ANZ','WBC'],['close','volume','daychg'])


#spreaddf = ft.addsma(spreaddf,[5,30],('ANZWBCclose'),spread=True)

#add signals
# def movingavgcross(df,short,long,col):
#     data = df.copy()
#     above = data[short + col]>data[long + col]
#     crossedabove = data[short + 'prev' + col]<=data[long + 'prev' + col]
#     below = data[short + col]<data[long + col]
#     crossedbelow = data[short + 'prev' + col]>=data[long + 'prev' + col]
#     data['signal'] = np.where((above) & (crossedabove),'buy',np.where((below) & (crossedbelow),'sell',''))
#     return data
# spreaddf = movingavgcross(spreaddf,'sma5','sma30','ANZWBCclose')

def spreadtrade(df,col1,col2,upper =0.0068,lower = -0.0083 ):
    data = df.copy()
    data['spread'] = data[col1] - data[col2]
    data['signal'] = np.where((data['spread'] > upper),'sell',np.where(data['spread']<lower,'buy',''))
    return data

spreaddf = spreadtrade(spreaddf, 'ANZdaychg','WBCdaychg',upper =0.004,lower = -0.004)


#genetrate trades df

def generatetrades(df,target=0.003,stop=0.009, spread = []):
    #takes a data frame with sell and buy signals, iterates through and opens and closes trades based on stop, target
    # and time rules. Only opens a trade when not already open. Return dataframe of trades. Needs a spreaddf with col
    # as spread for spread trades

    openlong = False
    openshort = False
    skip =True
    counter =1
    data = df.copy()
    name1 = spread[0] + 'close'
    name2 = spread[1] + 'close'
    trades = pd.DataFrame(columns=['datetime','ticker','side','price','quantity','tradeid','date','state'])

    if spread:
        #need to rename spreadcol to 'close' so it works with iteration
        data.rename(columns = {spread[0]+spread[1]+'close':'close'},inplace = True)

    def createtrades(trades,side):

        if side == 'sell' and openlong == False:
            quantity = -1000
            state = 'short'
        elif side == 'buy' and openshort == False:
            quantity = 1000
            state = 'long'
        elif side== 'sell' and openlong == True:
            quantity = 1000
            state = 'long'
        elif side =='buy' and openshort == True:
            quantity = -1000
            state = 'short'
        if not spread:
            tradedict =  {'tradeid':counter,'datetime':row.Index,'ticker':row.ticker,'side': side,
                          'price':row.close,'quantity':quantity,'state':state}
            trades = trades.append(tradedict,ignore_index=True)

        else:
            tradedict =  {'tradeid':counter,'datetime':row.Index,'ticker':spread[0],'side': side,
                          'price':getattr(row,name1),'quantity':quantity,'state':state}
            trades = trades.append(tradedict,ignore_index=True)

            if side == 'buy':
                side = 'sell'
            else:
                side = 'buy'
            tradedict = {'tradeid': counter, 'datetime': row.Index, 'ticker': spread[1], 'side': side,
                         'price': getattr(row,name2), 'quantity': quantity * -1, 'state': state}

            trades = trades.append(tradedict, ignore_index=True)


        return trades

    for row in data.itertuples():
        if skip ==False:
            if openlong == False and openshort == False:
                if row.signal == 'buy' and row.Index.time() < datetime.time(15, 50) and getattr(row,spread[0]+'volume') > 0\
                        and getattr(row,spread[1]+'volume') > 0:
                    openlong = True
                    stoppx = round(row.close*(1-stop),4)
                    targetpx = round(row.close*(1+target),4)
                    trades = createtrades(trades,row.signal)
                    skip = True

                elif row.signal == 'sell' and row.Index.time() < datetime.time(15, 50) and getattr(row,spread[0]+'volume') > 0\
                        and getattr(row,spread[1]+'volume') > 0:
                    openshort = True
                    stoppx = round(row.close*(1+stop),4)
                    targetpx = round(row.close*(1-target),4)
                    trades = createtrades(trades,row.signal)
                    skip = True

            elif openlong == True:
                if row.close < stoppx or row.close > targetpx or row.Index.time() > datetime.time(15, 57) and getattr(row,spread[0]+'volume') > 0\
                        and getattr(row,spread[1]+'volume') > 0:
                    trades = createtrades(trades,'sell')
                    openlong = False
                    stoppx = 0
                    targetpx =0
                    counter+=1
                    skip = True

            elif openshort == True:
                if row.close > stoppx or row.close < targetpx or row.Index.time() > datetime.time(15, 57) and getattr(row,spread[0]+'volume') > 0\
                        and getattr(row,spread[1]+'volume') > 0:
                    trades = createtrades(trades,'buy')
                    openshort = False
                    stoppx = 0
                    targetpx =0
                    counter+=1
                    skip = True
        else:
            skip = False

    trades['date'] = trades.datetime.dt.date
    return trades


trades = generatetrades(spreaddf,target=0.004,stop = 0.009 ,spread = ['ANZ','WBC'])


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
    df['equity'] = df['pl'].cumsum()
    df['cumpl'] = df.groupby('tradeid')['pl'].transform('cumsum')
    df['brokerage'] = np.where((df['side'] == 'buy') | (df['side'] == 'sell'), df['tradevalue'].abs() * 0.0015, 0)
    df['pl'].fillna(0, inplace=True)
    df['netpl'] = df['pl'] - df['brokerage']
    df['equitynet'] = df['netpl'].cumsum()
    return df
fulldf = backteststats(fulldf)

i= fulldf.groupby('tradeid')['netpl'].sum().sort_values()

fulldf['session'] = np.where(fulldf['time'] <datetime.time(11,30),'morning',np.where(fulldf['time'] < datetime.time(14,30),'mid','arvo'))
plt.plot(fulldf.groupby('tradeid')['netpl'].sum().cumsum())


#optimise loop
resultlist = {'target':[],'stop':[],'bottom':[],'top':[],'netpl':[],'winrate':[],'trades':[]}

for top in np.arange(0.003,0.008,0.001):
    for bottom in np.arange(-0.003,-0.008,-0.001):
        for target in np.arange(0.003,0.02,0.001):
            for stop in np.arange(0.002,0.01,0.001):

                tempspreaddf = spreadtrade(spreaddf, 'ANZdaychg', 'WBCdaychg', top, bottom)
                trades = generatetrades(tempspreaddf,target=target,stop = stop ,spread = ['ANZ','WBC'])

            #join trades with pricedf
                def jointrades(tradesdf,fulldf):
                    #join the trades df back to the full data df for ease of plotting backtest results
                    def openpositionfilter(merged):
                        #filters larger dataframe by when there is an open trade and fills down the trade id so we can groupby tradeid
                        grouped = merged.groupby('tradeid')
                        for name, group in grouped:

                            mask = ((merged['datetime'] >= grouped.get_group(name).datetime.iloc[0]) & (merged['datetime']<=grouped.get_group(name).datetime.iloc[-1])
                                    & (merged['ticker'].isin(grouped.get_group(name).ticker.unique())))
                            if name==1:
                                filter = mask
                            else:
                                filter = mask | filter

                        opentrade = merged.loc[filter]
                        cols = ['tradeid','quantity','state']
                        opentrade.loc[:,cols] = opentrade.loc[:,cols].ffill()
                        tradeids = opentrade[['tradeid','datetime','ticker','quantity','state']]
                        merged.drop(columns = ['tradeid','quantity','state'],inplace = True)
                        merged = pd.merge(left = merged, right = tradeids,left_on=['datetime','ticker'],right_on=['datetime','ticker'],how='left')
                        return merged


                    filterdays = tradesdf[['ticker','date']].drop_duplicates()
                    filtereddf = pd.merge(left = fulldf.reset_index(),right=filterdays,how='inner',left_on=['date','ticker'],right_on=['date','ticker'])
                    filtereddf.drop(columns='date',inplace=True)
                    merged = pd.merge(left = filtereddf,right=tradesdf,how='left',left_on=['datetime','ticker'],
                                      right_on=['datetime','ticker']).drop_duplicates()#.sort_values(by = 'datetime')
                    merged = openpositionfilter(merged)
                    merged['date'] = merged.datetime.dt.date
                    return merged

                fulldf = jointrades(trades,df)
                fulldf = fulldf.sort_values(by = ['tradeid','ticker','datetime'])

                def backteststats(df):
                    df['tradevalue'] = df['quantity'] * df['close']
                    df['pl'] = df.groupby(['tradeid','ticker'])['tradevalue'].diff(1)
                    df['equity'] = df['pl'].cumsum()
                    df['cumpl'] = df.groupby('tradeid')['pl'].transform('cumsum')
                    df['brokerage'] = np.where((df['side'] == 'buy') | (df['side'] == 'sell'), df['tradevalue'].abs() * 0.0008,0)
                    df['pl'].fillna(0, inplace=True)
                    df['netpl'] = df['pl'] - df['brokerage']
                    df['equitynet'] = df['netpl'].cumsum()
                    return df
                fulldf = backteststats(fulldf)

                resultlist['target'].append(target)
                resultlist['stop'].append(stop)
                resultlist['bottom'].append(bottom)
                resultlist['top'].append(top)
                resultlist['netpl'].append(fulldf.netpl.sum())
                resultlist['winrate'].append((fulldf.groupby('tradeid')['netpl'].sum() > 0).sum()/len(fulldf))
                resultlist['trades'].append(len(fulldf.groupby('tradeid')['netpl'].sum()))


results = pd.DataFrame(resultlist)
results.to_csv('optimize2.csv')

fulldffiltered = fulldf[fulldf]
t =  sp.stockplots(fulldf)
t.backtestplot(spread = ['ANZ','WBC'])
t.backteststats()
btest = t.backtestdf.T

def backtestresult(df):
    # takes a dataframe of trades and returns a dataframe of performance statistics
    data = df.copy()
    data['value'] = data.apply(lambda x: x['price']* abs(x['quantity'])*-1 if x['side']=='buy' else x['price'] * (abs(x['quantity'])),axis=1 )
    performancedf = data.groupby(['tradeid','ticker'],as_index=False).sum()[['tradeid','ticker','value']].rename(columns = {'value':'pl'})
    performancedf['equity'] = performancedf.groupby('ticker').cumsum()['pl']

    return performancedf



imp.reload(sp)


