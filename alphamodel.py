import pandas as pd
class alphamodel:
# init with all relevant features need to calc signals
    def __init__(self, df):
        self.data = df.copy()
        self.data.set_index('datetime', inplace = True)
        self.data['value'] = self.data['vwap'] * self.data['volume']
        self.data = self.data.groupby('ticker').resample('1min').agg({'open':'first','high':'max','low':'min','close':'last','volume':'sum','value':'sum','count':'sum'}).reset_index(level = 0).dropna()
        self.data['vwap'] = self.data.groupby(['ticker',self.data.index.date])['value'].transform('cumsum')/self.data.groupby(['ticker',self.data.index.date])['volume'].transform('cumsum')
        self.data['cvol'] = self.data.groupby('ticker')['volume'].cumsum()

#pass dataframe into alphamodels and generate buy/sell signals
    def generatesignals(self):
        self.data['signal'] = 0
        self.data['signal'].iloc[len(self.data)-1] = 1

    def gettrades(self):
        self.tradesdf = pd.DataFrame({'ticker':['ALL'],'side':['BUY'],'quantity':[100],'limitpx':[35.5]})
        return self.tradesdf
        #self.testtrades = self.data[(self.data.signal!=0) & (self.data.index == self.data.index.max())]
