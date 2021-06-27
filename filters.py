import pandas as pd
import numpy as np

class filters():

    def openMatchRvol(self,df,rvol = 2):
        #function to filter dataframe by opening matches greater than 30 day average volume * rvol
        self.data = df.copy()
        self.data.reset_index(inplace = True)
        self.rvolmatch = self.data[self.data['volume'] > self.data['10dayavgopenvol'] * rvol]
        self.rvolmatch.reset_index(inplace = True)
        self.rvolmatch = self.rvolmatch[['date','ticker']]
        self.data = pd.merge(self.data,self.rvolmatch,left_on=['date','ticker'],right_on=['date','ticker'],how = 'inner')#.drop_duplicates()
        self.data.set_index('datetime',inplace = True)
        return data

    def gapfinder(self,df,gap = 0.02,absolute = False):
        self.data = df.copy()
        self.data.reset_index(inplace = True)
        self.opendf = self.data[self.data['match']=='opening match']
        self.closedf = self.data[self.data['match']=='closing match']
        self.closedf['prevclose'] = self.closedf.groupby('ticker')['close'].shift(1)
        self.closedffiltered = self.closedf[['date','ticker','prevclose']]
        self.data = pd.merge(self.data,self.closedffiltered,left_on=['date','ticker'],right_on=['date','ticker'],how = 'inner')
        if absolute == False:
            if np.sign(gap) ==1:
                self.data['filter'] = np.where((self.data['match']=='opening match')&(self.data['close']/self.data['prevclose']-1 > gap),1,0)
            else:
                self.data['filter'] = np.where((self.data['match']=='opening match')&(self.data['close']/self.data['prevclose']-1 < gap),1,0)
        else:
            self.data['filter'] = np.where((self.data['match']=='opening match')&(abs(self.data['close']/self.data['prevclose']-1) > gap),1,0)

        self.filterdf = self.data[self.data['filter']==1][['ticker','date']]
        self.data = pd.merge(self.data,self.filterdf,left_on=['date','ticker'],right_on=['date','ticker'],how = 'inner')
        self.data.drop(columns = 'filter',inplace = True)
        self.data.set_index('datetime',inplace = True)
        return self.data
