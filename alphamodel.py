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
        self.endtime = self.data.index.max()
        self.datafiltered = self.data[self.data.index == self.endtime]

    def generateCloseSignals(self,openpos):




#pass dataframe into alphamodels and generate buy/sell signals
    def generateSignals(self):
        self.datafiltered['signal'] = 0
        self.pricecondition = self.datafiltered['close'] > self.datafiltered['prevbarclose']
        self.volcondition = self.datafiltered['volume'] > 5000
        self.datafiltered['signal'] = np.where(self.pricecondition & self.volcondition,1,0)
        self.opentrades = self.datafiltered[self.datafiltered['signal']!=0]

    def sendOpenTrades(self):

        self.opentrades['side'] = np.where(self.opentrades['signal'] ==1,'BUY','SELL')
        self.opentrades['quantity'] = 1000
        self.opentrades['limitpx'] = self.opentrades['close']*1.003
        self.opentrades['limitpx'] = self.opentrades['limitpx'].round(4)
        self.tradesdf = self.opentrades[['ticker','side','quantity','limitpx']]
        return self.tradesdf




# test = alphamodel(df)
# test.generateSignals()
# test.sendOpenTrades()


