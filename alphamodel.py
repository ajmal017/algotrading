import pandas as pd
import numpy as np
import features as ft
pd.options.mode.chained_assignment = None
class alphamodel():

    def __init__(self, df,prevclosedict):
        #resample 5sec bars to 1 min and add all features
        self.data = df.copy()
        self.data.set_index('datetime', inplace = True)
        self.data['value'] = self.data['vwap'] * self.data['volume']
        self.data = self.data.groupby('ticker').resample('1min').agg({'open':'first','high':'max','low':'min','close':'last','volume':'sum','value':'sum','count':'sum'}).reset_index(level = 0).dropna()
        self.data['prevdayclose'] = self.data['ticker'].map(prevclosedict)
        self.data['daychg'] = self.data['close'] / self.data['prevdayclose'] - 1
        self.data['date'] = self.data.index.date
        self.endtime = self.data.index.max()
        self.newspreadtrades = pd.DataFrame()
        self.spreaddf= pd.DataFrame()

    def generateNewTrades(self,availablelist):
        self.newtrades = self.data[(self.data.index == self.endtime) & (self.data.signal !='') & self.data.ticker.isin(availablelist)]
        self.newspreadtrades = self.spreaddf[(self.spreaddf.index ==self.endtime)& (self.spreaddf.signal !='') & self.spreaddf.ticker.isin(availablelist)]
        print('new trades df: ')
        print(self.newtrades)
        if not self.newtrades.empty:
            self.newtrades['quantity'] = self.newtrades.apply(lambda x: round(min(x.volume*0.2,10000/x.close),0),axis =1)
        return self.newtrades


    def generateNewSpreadTrades(self,availablelist,openparams):

        for spread in availablelist:
            roclow = openparams[spread[0]+spread[1]]['roclow']
            rochigh = openparams[spread[0] + spread[1]]['rochigh']
            upper = openparams[spread[0] + spread[1]]['upper']
            lower = openparams[spread[0] + spread[1]]['lower']

            newtrades = pd.DataFrame()
            self.spreaddf = ft.spread(self.data, ticker=spread, vals = ['close', 'volume', 'daychg'])
            if not self.spreaddf[(self.spreaddf.index == self.endtime)][spread[0] + 'volume'].iloc[0] == 0 and not self.spreaddf[(self.spreaddf.index == self.endtime)][spread[1] + 'volume'].iloc[0] == 0:

                self.spreaddf = ft.spreadroc(self.spreaddf,spread, [10])
                self.spreaddf = self.spreadtrade(self.spreaddf, roc='spreadroc10', roclow=roclow, rochigh=rochigh,
                                                 upper=upper, lower=lower, col1=spread[0] + 'daychg', col2=spread[1] +'daychg')

                self.spreaddf = self.spreaddf[[spread[0] + spread[1] +'close','signal']].rename(columns = {spread[0] + spread[1] +'close':spread[0] + spread[1]}).\
                    reset_index().melt(id_vars = ['datetime','signal'],var_name = 'ticker',value_name = 'close').set_index('datetime')
                self.spreaddf['ticker1'] = self.spreaddf['ticker'].apply(lambda x: x[:3])
                self.spreaddf['ticker2'] = self.spreaddf['ticker'].apply(lambda x: x[-3:])
                newtrades = self.spreaddf[(self.spreaddf.index == self.endtime) & (self.spreaddf.signal != '')]

                if not newtrades.empty:
                    if self.newspreadtrades.empty:
                        self.newspreadtrades = newtrades
                    else:
                        pd.concat([self.newspreadtrades,newtrades])

        if not self.newspreadtrades.empty:
            print(self.newspreadtrades)
        return self.newspreadtrades

    def addsma(self,df,period =[5]):
        data = df.copy()
        for i in period:
            data['sma'+str(i)] = data.groupby('ticker')['close'].transform(lambda x: x.rolling(i).mean())
            data['smaprev'+str(i)] = data.groupby('ticker')['sma'+str(i)].shift(1)
        return data

    def movingavgcross(self,df,signal = 'buy'):
        data = df.copy()
        above = data['sma1']==data['sma1']
        #crossed = data['smaprev1']<=data['smaprev2']
        data['signal'] = np.where((above) ,signal,'')
        return data


    def spreadtrade(self,df, roc, roclow,rochigh,upper,lower, col1,col2):
        data = df.copy()
        data['spread'] = data[col1] - data[col2]
        data['signal'] = np.where((data['spread'] > upper) & (data[roc] > rochigh), 'sell', np.where((data['spread'] < lower) &
                                (data[roc] < roclow), 'buy', ''))
        return data

