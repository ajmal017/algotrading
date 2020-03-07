import pandas as pd
import numpy as np
class alphamodel:
# init with all relevant features need to calc signals
    def __init__(self, df):

        self.data = df.copy()
        self.data.set_index('datetime', inplace = True)
        self.data['value'] = self.data['vwap'] * self.data['volume']
        self.data = self.data.groupby('ticker').resample('1min').agg({'open':'first','high':'max','low':'min','close':'last','volume':'sum','value':'sum','count':'sum'}).reset_index(level = 0).dropna()
        self.data['wap'] = self.data.groupby(['ticker',self.data.index.date])['value'].transform('cumsum')/self.data.groupby(['ticker',self.data.index.date])['volume'].transform('cumsum')
        self.data['cvol'] = self.data.groupby('ticker')['volume'].cumsum()
        self.data['prevbarclose'] = self.data.groupby('ticker')['close'].shift(1)
        self.endtime = datetime(2020,3,4,15,57,00)
        #self.endtime = self.data.index.max()
        self.datafiltered = self.data[self.data.index == self.endtime]

    def generateCloseSignals(self,openpos):

        self.currentpositiondf = self.datafiltered[self.datafiltered['ticker'].isin(openpos)]
        self.currentpositiondf['signal'] = 0
        self.closesignal = self.currentpositiondf['wap'] > self.currentpositiondf['close']
        self.currentpositiondf['signal'] = np.where(self.closesignal,1,0)
        self.closetrades = self.currentpositiondf[self.currentpositiondf['signal']!=0]

#pass dataframe into alphamodels and generate buy/sell signals
    def generateSignals(self,available):

        self.newpositionsdf = self.datafiltered.copy()
        self.newpositionsdf =  self.newpositionsdf[ self.newpositionsdf['ticker'].isin(available)]
        self.newpositionsdf['signal'] = 0
        self.pricecondition = self.datafiltered['close'] > self.datafiltered['prevbarclose']
        self.volcondition = self.datafiltered['volume'] > 5000
        self.newpositionsdf['signal'] = np.where(self.pricecondition & self.volcondition,1,0)
        self.opentrades = self.newpositionsdf[self.newpositionsdf['signal']!=0]

    def genTrades(self,df):
        df['side'] = np.where(df['signal'] ==1,'BUY','SELL')
        df['quantity'] = 1000
        df['limitpx'] = df['close']*1.003
        df['limitpx'] = df['limitpx'].round(4)
        tradesdf = df[['ticker','side','quantity','limitpx']]
        return tradesdf




test = alphamodel(df)
test.generateCloseSignals(['ALU'])
test.generateSignals(['ALU'])
test.genTrades(test.closetrades)
test.sendOpenTrades()


