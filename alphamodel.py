import pandas as pd
import numpy as np
import features as ft
pd.options.mode.chained_assignment = None
class alphamodel():

    def __init__(self, df,anzprevclose,wbcprevlose,spreads = []):
        #resample 5sec bars to 1 min and add all features
        self.data = df.copy()
        self.data.set_index('datetime', inplace = True)
        self.data['value'] = self.data['vwap'] * self.data['volume']
        self.data = self.data.groupby('ticker').resample('1min').agg({'open':'first','high':'max','low':'min','close':'last','volume':'sum','value':'sum','count':'sum'}).reset_index(level = 0).dropna()
        self.data['prevdayclose'] = np.where(self.data['ticker']=='ANZ',float(anzprevclose),np.where(self.data['ticker']=='WBC',float(wbcprevlose),''))
        self.data['prevdayclose'] = pd.to_numeric(self.data['prevdayclose'])
        self.data['daychg'] = self.data['close'] / self.data['prevdayclose'] - 1
        self.spreaddf = ft.spread(self.data,['ANZ','WBC'],['close','volume','daychg'])
        self.spreaddf = self.spreadtrade(self.spreaddf,0.0008,-0.0003, 'ANZdaychg','WBCdaychg')
        # self.data['wap'] = self.data.groupby(['ticker',self.data.index.date])['value'].transform('cumsum')/self.data.groupby(['ticker',self.data.index.date])['volume'].transform('cumsum')
        # self.data['cvol'] = self.data.groupby('ticker')['volume'].cumsum()
        # self.data['prevbarclose'] = self.data.groupby('ticker')['close'].shift(1)
        # self.data = self.addsma(self.data,period = [1,2])
        # self.data = self.movingavgcross(self.data,'buy')
        self.endtime = self.data.index.max()

    def generateNewTrades(self,availablelist):
        self.newtrades = self.data[(self.data.index == self.endtime) & (self.data.signal !='') & self.data.ticker.isin(availablelist)]
        self.newspreadtrades = self.spreaddf[(self.spreaddf.index ==self.endtime)& (self.spreaddf.signal !='') & self.spreaddf.ticker.isin(availablelist)]
        print('new trades df: ')
        print(self.newtrades)
        if not self.newtrades.empty:
            self.newtrades['quantity'] = self.newtrades.apply(lambda x: round(min(x.volume*0.2,10000/x.close),0),axis =1)
        return self.newtrades

    def generateNewSpreadTrades(self,availablelist):
        self.spreaddf = self.spreaddf[['ANZWBCclose','signal']].rename(columns = {'ANZWBCclose':'ANZWBC'}).\
            reset_index().melt(id_vars = ['datetime','signal'],var_name = 'ticker',value_name = 'close').set_index('datetime')
        print(self.spreaddf)
        self.spreaddf['ticker1'] = self.spreaddf['ticker'].apply(lambda x: x[:3])
        self.spreaddf['ticker2'] = self.spreaddf['ticker'].apply(lambda x: x[-3:])
        self.newspreadtrades = self.spreaddf[(self.spreaddf.index == self.endtime) & (self.spreaddf.signal != '') & self.spreaddf.ticker1.isin(
                availablelist) & (self.spreaddf.ticker2.isin(availablelist))]
        print(self.newspreadtrades)
        if not self.newspreadtrades.empty:
            self.newspreadtrades['quantity'] = 1000
            self.availablelist = list(set(availablelist) - set([self.newspreadtrades.ticker[0][:3] ,self.newspreadtrades.ticker[0][-3:]]))

        else:
            self.availablelist = availablelist

        return self.newspreadtrades, self.availablelist

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

    def spreadtrade(self,df, upper,lower, col1,col2):
        data = df.copy()
        data['spread'] = data[col1] - data[col2]
        data['signal'] = np.where((data['spread'] > upper), 'sell', np.where(data['spread'] < lower, 'buy', ''))
        return data

