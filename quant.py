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
signals = {'Ticker1':[],'Ticker2':[],'exp':[],'trades':[],'winrate':[],'band':[],'target':[],'stop':[],'spreadroc10':[]}

spreads = [['AGL','ORG'],['ASX','TCL'],['XRO','ALU'],['XRO','APT'],
                        ['RIO','SGM'],['COH','SEK'],['WOW','WES'],['ALU','CPU'],['DMP','HVN'],['NCM','NST'],
                        ['HVN','JBH'],['STO','WPL'],['IEL','SOL'],['FLT','WEB'],['FPH','COH'],['CSL','FPH'],
                        ['IAG','SUN'],['PDL','MFG'],['JHG','PDL'],['LNK','CPU'],['NXT','MP1'],['NXT','GMG'],
                        ['ORG','WPL'],['OSH','STO'],['OSH','WPL'],['RMD','FPH'],['RMD','COH'],['CSL','RMD'],
                        ['REA','DHG'],['SOL','COH'],['TNE','ALU'],['RWC','TWE'],['XRO','WTC'],['SYD','TCL'],
                        ['AST','SKI'],['XRO','APX'],['ANN','COH'],['BIN','CWY'], ['SGM','BHP']]

closeparams = {'NEAAPX':{'stop':0.018,'target':0.1},'NEAAPT':{'stop':0.018,'target':0.1},
                            'BHPFMG':{'stop':0.016,'target':0.024},'RIOFMG':{'stop':0.016,'target':0.026},
                            'ALUAPX':{'stop':0.016,'target':0.1},'APXAPT':{'stop':0.018,'target':0.1},
                            'APTALU':{'stop':0.018,'target':0.1},'WTCAPT':{'stop':0.018,'target':0.1},
                            'WTCALU':{'stop':0.016,'target':0.1},'WTCAPX':{'stop':0.018,'target':0.1},
                            'SEKCAR':{'stop':0.016,'target':0.1},'COHPME':{'stop':0.012,'target':0.1},
                            'ANNWTC':{'stop':0.018,'target':0.1},'SHLRHC':{'stop':0.018,'target':0.1},
                            'AGLORG':{'stop':0.018,'target':0.1},'ASXTCL':{'stop':0.012,'target':0.1},
                            'XROALU':{'stop':0.014,'target':0.1},'XROAPT':{'stop':0.018,'target':0.1},
                            'NCMNST':{'stop':0.012,'target':0.1},'RIOSGM':{'stop':0.018,'target':0.1},
                            'COHSEK':{'stop':0.016,'target':0.1},'WOWWES':{'stop':0.012,'target':0.026},
                            'ALUCPU':{'stop':0.018,'target':0.1},'DMPHVN':{'stop':0.012,'target':0.1},
                            'HVNJBH':{'stop':0.012,'target':0.1},'STOWPL':{'stop':0.012,'target':0.1},
                            'IELSOL':{'stop':0.018,'target':0.1},'FLTWEB':{'stop':0.014,'target':0.1},
                            'FPHCOH':{'stop':0.012,'target':0.1},'CSLFPH':{'stop':0.016,'target':0.1},
                            'IAGSUN':{'stop':0.012,'target':0.1},'PDLMFG':{'stop':0.012,'target':0.1},
                            'JHGPDL':{'stop':0.014,'target':0.1},'LNKCPU':{'stop':0.016,'target':0.1},
                            'NXTMP1':{'stop':0.018,'target':0.1},'NXTGMG':{'stop':0.016,'target':0.1},
                            'ORGWPL':{'stop':0.012,'target':0.1},'OSHSTO':{'stop':0.018,'target':0.1},
                            'OSHWPL':{'stop':0.016,'target':0.1},'RMDFPH':{'stop':0.016,'target':0.1},
                            'RMDCOH':{'stop':0.014,'target':0.1},'CSLRMD':{'stop':0.012,'target':0.1},
                            'READHG':{'stop':0.018,'target':0.1},'SOLCOH':{'stop':0.012,'target':0.1},
                            'TNEALU':{'stop':0.012,'target':0.1},'RWCTWE':{'stop':0.018,'target':0.1},
                            'XROWTC':{'stop':0.012,'target':0.1},'SYDTCL':{'stop':0.018,'target':0.1},
                            'ASTSKI':{'stop':0.018,'target':0.1},'XROAPX':{'stop':0.018,'target':0.1},
                            'ANNCOH':{'stop':0.018,'target':0.1},'BINCWY':{'stop':0.018,'target':0.1},
                            'SGMBHP':{'stop':0.018,'target':0.1}}

openparams = {'NEAAPX' : {'roclow':-0.002, 'rochigh':0.002,'upper':0.028, 'lower':-0.028},
                           'NEAAPT' : {'roclow':-0.006, 'rochigh':0.006,'upper':0.028, 'lower':-0.028},
                           'BHPFMG' : {'roclow':-0.00, 'rochigh':0.00,'upper':0.018, 'lower':-0.018},
                           'RIOFMG' : {'roclow':-0.00, 'rochigh':0.00,'upper':0.018, 'lower':-0.018},
                           'ALUAPX' : {'roclow':-0.002, 'rochigh':0.002,'upper':0.022, 'lower':-0.022},
                           'APXAPT' : {'roclow':-0.00, 'rochigh':0.00,'upper':0.028, 'lower':-0.028},
                           'APTALU' : {'roclow':-0.008, 'rochigh':0.008,'upper':0.026, 'lower':-0.026},
                           'WTCAPT' : {'roclow':-0.002, 'rochigh':0.002,'upper':0.026, 'lower':-0.026},
                           'WTCALU' : {'roclow':-0.002, 'rochigh':0.002,'upper':0.032, 'lower':-0.032},
                           'WTCAPX' : {'roclow':-0.008, 'rochigh':0.008,'upper':0.03, 'lower':-0.03},
                           'SEKCAR' : {'roclow':-0.002, 'rochigh':0.002,'upper':0.02, 'lower':-0.02},
                           'COHPME' : {'roclow':-0.002, 'rochigh':0.002,'upper':0.024, 'lower':-0.024},
                           'ANNWTC' : {'roclow':-0.006, 'rochigh':0.006,'upper':0.03, 'lower':-0.03},
                           'SHLRHC': {'roclow': -0.006, 'rochigh': 0.006,'upper':0.016,'lower':-0.016},
                           'AGLORG' : {'roclow':-0.002, 'rochigh':0.002,'upper':0.024, 'lower':-0.024},
                           'ASXTCL' : {'roclow':-0.006, 'rochigh':0.006,'upper':0.018, 'lower':-0.018},
                           'XROALU' : {'roclow':-0.00, 'rochigh':0.00,'upper':0.024, 'lower':-0.024},
                           'XROAPT' : {'roclow':-0.00, 'rochigh':0.00,'upper':0.03, 'lower':-0.03},
                           'NCMNST' : {'roclow':-0.00, 'rochigh':0.00,'upper':0.02, 'lower':-0.02},
                           'RIOSGM' : {'roclow':-0.00, 'rochigh':0.00,'upper':0.026, 'lower':-0.026},
                           'COHSEK' : {'roclow':-0.00, 'rochigh':0.00,'upper':0.024, 'lower':-0.024},
                           'WOWWES' : {'roclow':-0.00, 'rochigh':0.00,'upper':0.02, 'lower':-0.02},
                           'ALUCPU' : {'roclow':-0.00, 'rochigh':0.00,'upper':0.026, 'lower':-0.026},
                           'DMPHVN' : {'roclow':-0.00, 'rochigh':0.00,'upper':0.026, 'lower':-0.026},
                           'HVNJBH' : {'roclow':-0.004, 'rochigh':0.004,'upper':0.024, 'lower':-0.024},
                           'STOWPL' : {'roclow':-0.004, 'rochigh':0.004,'upper':0.02, 'lower':-0.02},
                           'IELSOL' : {'roclow':-0.00, 'rochigh':0.00,'upper':0.024, 'lower':-0.024},
                           'FLTWEB' : {'roclow':-0.008, 'rochigh':0.008,'upper':0.028, 'lower':-0.028},
                           'FPHCOH' : {'roclow':-0.004, 'rochigh':0.004,'upper':0.024, 'lower':-0.024},
                           'CSLFPH' : {'roclow':-0.00, 'rochigh':0.00,'upper':0.024, 'lower':-0.024},
                           'IAGSUN' : {'roclow':-0.0, 'rochigh':0.00,'upper':0.028, 'lower':-0.028},
                           'PDLMFG' : {'roclow':-0.006, 'rochigh':0.006,'upper':0.026, 'lower':-0.026},
                           'JHGPDL' : {'roclow':-0.00, 'rochigh':0.00,'upper':0.026, 'lower':-0.026},
                           'LNKCPU' : {'roclow':-0.002, 'rochigh':0.002,'upper':0.028, 'lower':-0.028},
                           'NXTMP1': {'roclow': -0.008, 'rochigh': 0.008, 'upper': 0.024, 'lower': -0.024},
                           'NXTGMG': {'roclow': -0.002, 'rochigh': 0.002, 'upper': 0.024, 'lower': -0.024},
                           'ORGWPL': {'roclow': -0.002, 'rochigh': 0.002, 'upper': 0.022, 'lower': -0.022},
                           'OSHSTO': {'roclow': -0.006, 'rochigh': 0.006, 'upper': 0.024, 'lower': -0.024},
                           'OSHWPL': {'roclow': -0.004, 'rochigh': 0.004, 'upper': 0.024, 'lower': -0.024},
                           'RMDFPH': {'roclow': -0.004, 'rochigh': 0.004, 'upper': 0.022, 'lower': -0.022},
                           'RMDCOH': {'roclow': -0.00, 'rochigh': 0.00, 'upper': 0.022, 'lower': -0.022},
                           'CSLRMD': {'roclow': -0.006, 'rochigh': 0.006, 'upper': 0.022, 'lower': -0.022},
                           'READHG': {'roclow': -0.002, 'rochigh': 0.002, 'upper': 0.028, 'lower': -0.028},
                           'SOLCOH': {'roclow': -0.002, 'rochigh': 0.002, 'upper': 0.026, 'lower': -0.026},
                           'TNEALU': {'roclow': -0.002, 'rochigh': 0.002, 'upper': 0.022, 'lower': -0.022},
                           'RWCTWE': {'roclow': -0.002, 'rochigh': 0.002, 'upper': 0.022, 'lower': -0.022},
                           'XROWTC': {'roclow': -0.002, 'rochigh': 0.002, 'upper': 0.022, 'lower': -0.022},
                           'SYDTCL': {'roclow': -0.002, 'rochigh': 0.002, 'upper': 0.022, 'lower': -0.022},
                           'ASTSKI': {'roclow': -0.002, 'rochigh': 0.002, 'upper': 0.022, 'lower': -0.022},
                           'XROAPX': {'roclow': -0.002, 'rochigh': 0.002, 'upper': 0.022, 'lower': -0.022},
                           'ANNCOH': {'roclow': -0.002, 'rochigh': 0.002, 'upper': 0.022, 'lower': -0.022},
                           'BINCWY': {'roclow': -0.002, 'rochigh': 0.002, 'upper': 0.022, 'lower': -0.022},
                           'SGMBHP': {'roclow': -0.002, 'rochigh': 0.002, 'upper': 0.022, 'lower': -0.022}
                           }

for spread in spreads:
    try:

        ticker1 = spread[0]
        ticker2 = spread[1]
        target = closeparams[ticker1+ticker2]['target']
        stop = closeparams[ticker1+ticker2]['stop']
        rochigh = openparams[ticker1+ticker2]['rochigh']
        roclow = openparams[ticker1+ticker2]['roclow']
        spreadroc = 10
        upper = openparams[ticker1+ticker2]['upper']
        lower = openparams[ticker1+ticker2]['lower']
        filename = ticker1 + ticker2 +'out'

        query = """
        SELECT * FROM asxminutedata where datetime > '31/10/2019' AND datetime < '29/02/2020' AND
        ticker in ('""" + ticker1 +"','" + ticker2+"')"

        query2 = """
        SELECT * FROM asxminutedata where datetime > '30/04/2020' and datetime < '31/08/2020' AND
        ticker in ('""" + ticker1 +"','" + ticker2+"')"

        query3 = """
        SELECT * FROM asxminutedata where datetime > '1/05/2019' and datetime < '31/10/2019' AND
        ticker in ('""" + ticker1 +"','" + ticker2+"')"

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

        spreaddf = ft.spread(df,[ticker1,ticker2],['close','volume','daychg'])
        spreaddf['date'] = spreaddf.index.date
        spreaddf['time'] = spreaddf.index.time



        spreaddf = ft.spreadroc(spreaddf,[ticker1,ticker2],[spreadroc])

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



        # spreaddf = spreadtrade(spreaddf, 'ANZzed','WBCzed',zedupper = 3,zlower = -3)

        # def spreadtraderoc(df,roc,col1,col2,rochigh,roclow,upper =0.005,lower = -0.005 ):
        #     data = df.copy()
        #     data['spread'] = data[col1] - data[col2]
        #     data['signal'] = np.where((data[roc] > rochigh) & (data['spread'] > upper),'sell',np.where((data[roc]<roclow)
        #                                         & (data['spread']<lower),'buy',''))
        #     return data
        #
        # spreaddf = spreadtraderoc(spreaddf,'spreadroc10','EVNdaychg', 'SARdaychg', rochigh=0.004,roclow=-0.004,upper =0.026,lower = -0.026 )

        def spreadtraderoc(df,roc,col1,col2,rochigh,roclow,upper =0.005,lower = -0.005 ):
            data = df.copy()
            data['spread'] = data[col1] - data[col2]
            data['signal'] = np.where((data[roc] > rochigh) & (data['spread'] > upper),'sell',np.where((data[roc]<roclow)
                                                & (data['spread']<lower),'buy',''))
            return data

        spreaddf = spreadtraderoc(spreaddf,'spreadroc' + str(spreadroc),ticker1 + 'daychg', ticker2 + 'daychg', rochigh=rochigh,roclow=-roclow,upper =upper,lower = lower )


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

            def createtrades(trades,side,openpos):

                if spread:
                    price1 = getattr(row,name1)
                    price2 = getattr(row, name2)
                else:
                    price1 = row.close

                if side == 'sell' and openlong == False:
                    quantity1 = -round(10000/price1)
                    state = 'short'
                elif side == 'buy' and openshort == False:
                    quantity1 = round(10000/price1)
                    state = 'long'
                elif side== 'sell' and openlong == True:
                    quantity1 = openpos[spread[0]]
                    state = 'long'
                elif side =='buy' and openshort == True:
                    quantity1 = openpos[spread[0]]
                    state = 'short'

                if spread and openshort==False and openlong==False:
                    quantity2 = -round((quantity1 * price1)/price2)
                else:
                    quantity2 = openpos[spread[1]]

                if not spread:
                    tradedict =  {'tradeid':counter,'datetime':row.Index,'ticker':row.ticker,'side': side,
                                  'price':row.close,'quantity':quantity,'state':state}
                    trades = trades.append(tradedict,ignore_index=True)

                else:
                    tradedict =  {'tradeid':counter,'datetime':row.Index,'ticker':spread[0],'side': side,
                                  'price':price1,'quantity':quantity1,'state':state}
                    trades = trades.append(tradedict,ignore_index=True)

                    if side == 'buy':
                        side = 'sell'
                    else:
                        side = 'buy'
                    tradedict = {'tradeid': counter, 'datetime': row.Index, 'ticker': spread[1], 'side': side,
                                 'price': price2, 'quantity': quantity2, 'state': state}

                    trades = trades.append(tradedict, ignore_index=True)


                return trades

            for row in data.itertuples():
                if skip ==False:
                    if openlong == False and openshort == False:
                        openpos = {}
                        if row.signal == 'buy' and row.Index.time() < datetime.time(15, 50) and getattr(row,spread[0]+'volume') > 0\
                                and getattr(row,spread[1]+'volume') > 0:
                            stoppx = round(row.close*(1-stop),4)
                            targetpx = round(row.close*(1+target),4)
                            trades = createtrades(trades,row.signal,openpos)
                            openlong = True
                            lasttrades = trades[-2:]
                            openpos = {lasttrades['ticker'].iloc[0] : lasttrades['quantity'].iloc[0],
                                       lasttrades['ticker'].iloc[1] : lasttrades['quantity'].iloc[1]}
                            skip = True

                        elif row.signal == 'sell' and row.Index.time() < datetime.time(15, 50) and getattr(row,spread[0]+'volume') > 0\
                                and getattr(row,spread[1]+'volume') > 0:

                            stoppx = round(row.close*(1+stop),4)
                            targetpx = round(row.close*(1-target),4)
                            trades = createtrades(trades,row.signal,openpos)
                            openshort = True
                            lasttrades = trades[-2:]
                            openpos = {lasttrades['ticker'].iloc[0] : lasttrades['quantity'].iloc[0],
                                       lasttrades['ticker'].iloc[1] : lasttrades['quantity'].iloc[1]}
                            skip = True

                    elif openlong == True:
                        if row.close < stoppx or row.close > targetpx or row.Index.time() > datetime.time(15, 50) and getattr(row,spread[0]+'volume') > 0\
                                and getattr(row,spread[1]+'volume') > 0:
                            trades = createtrades(trades,'sell',openpos)
                            openlong = False
                            openpos = {}
                            stoppx = 0
                            targetpx =0
                            counter+=1
                            skip = True

                    elif openshort == True:
                        if row.close > stoppx or row.close < targetpx or row.Index.time() > datetime.time(15, 50) and getattr(row,spread[0]+'volume') > 0\
                                and getattr(row,spread[1]+'volume') > 0:
                            trades = createtrades(trades,'buy',openpos)
                            openshort = False
                            openpos = {}
                            stoppx = 0
                            targetpx =0
                            counter+=1
                            skip = True
                else:
                    skip = False

            trades['date'] = trades.datetime.dt.date
            return trades
        trades = generatetrades(spreaddf,target=target,stop = stop,spread = [ticker1,ticker2])


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

        netpl = fulldf.groupby('tradeid').sum()
        winrate = len(netpl[netpl['netpl']>0])/len(netpl)

        fulldf['netplpertrade'] = fulldf.groupby('tradeid')['netpl'].transform('sum')
        fulldf['exp'] = fulldf.groupby('tradeid')['netpl'].sum().mean()

        fulldf.groupby('tradeid').mean().to_csv(filename + '.csv')

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
    except:
        pass


signals = pd.DataFrame(signals)
signals.to_csv('sigs.csv')
# fulldf['session'] = np.where(fulldf['time'] <datetime.time(11,30),'morning',np.where(fulldf['time'] < datetime.time(14,30),'mid','arvo'))
# fulldf.groupby('session')['netpl'].sum()
# fulldf.groupby('time')['netpl'].sum().to_csv('oostime.csv')
#
# plt.plot(fulldf.groupby('tradeid')['netpl'].sum().cumsum())
#
# fulldf.groupby('tradeid')['netpl'].sum().to_csv('oos.csv')
#
#
#
# fulldffiltered = fulldf[fulldf['tradeid'].isin(list(np.arange(1,25,1)))]
#

# t = sp.stockplots(fulldf)
# t.backtestplot(spread = [ticker1,ticker2])
# t.multiplot(1,1)
# # t.backteststats()
# btest = t.backtestdf.T
#
# def backtestresult(df):
#     # takes a dataframe of trades and returns a dataframe of performance statistics
#     data = df.copy()
#     data['value'] = data.apply(lambda x: x['price']* abs(x['quantity'])*-1 if x['side']=='buy' else x['price'] * (abs(x['quantity'])),axis=1 )
#     performancedf = data.groupby(['tradeid','ticker'],as_index=False).sum()[['tradeid','ticker','value']].rename(columns = {'value':'pl'})
#     performancedf['equity'] = performancedf.groupby('ticker').cumsum()['pl']
#
#     return performancedf
#
# fulldf.groupby('time')['netpl'].sum().to_csv('test.csv')
#
imp.reload(sp)


