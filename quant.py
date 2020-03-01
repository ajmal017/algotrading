import connectdb
import plotly.express as px
from plotly.offline import plot
import scipy
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import stockplot as sp
import imp
import datetime
imp.reload(sp)
dbconn = connectdb.connectdb()
dbconn.conn = dbconn.pgconnect()

query = """
SELECT * FROM asxminutedata1 where datetime < '31/10/2019' AND 
ticker in ('NEA','APT','LLC','APX') 
"""

queryall = """
SELECT distinct ticker FROM asxminutedata1
"""
colnames = ['open','high','low','close','volume','value','count','datetime','ticker','vwap','avat30','cavat30','wap']
x = dbconn.pgquery(dbconn.conn,query,None)
df = pd.DataFrame(x, columns= colnames).set_index('datetime')
df = df.sort_values(by = ['ticker', 'datetime'], ascending = [True, True])
df['date']  = df.index.date
df['time'] = df.index.time

def addmatches(df):
    #finds first non zero volume bar for each ticker and date
    data = df.copy()
    datanonzero = data[data['volume']!=0]
    datanonzero.reset_index(inplace=True)
    openmatchdf = datanonzero.groupby(['ticker',datanonzero.date])['datetime'].first().reset_index()
    closingmatchdf = datanonzero.groupby(['ticker',datanonzero.date])['datetime'].last().reset_index()
    openmatchdf['match'] = 'opening match'
    closingmatchdf['match'] = 'closing match'
    matchdf = pd.concat([openmatchdf,closingmatchdf])
    matchdf.drop(columns = ['date'],inplace  = True)
    data = pd.merge(data,matchdf,how ='left',left_on=['datetime','ticker'],right_on=['datetime','ticker'])
    #add 10day average volume for opening match
    openingmatchdf = data[data['match']=='opening match']
    openingmatchdf['10dayavgopenvol'] = openingmatchdf.groupby('ticker')['volume'].transform(lambda x: x.rolling(10).mean())
    openingmatchdf.reset_index(inplace=True)
    openingmatchdf = openingmatchdf[['ticker','datetime','10dayavgopenvol']]
    data = pd.merge(data,openingmatchdf,on = ['ticker','datetime'],how = 'left')
    data.set_index('datetime',inplace = True)
    return data

df = addmatches(df)

def openMatchRvol(df,rvol = 2):
    #function to filter dataframe by opening matches greater than 30 day average volume * rvol
    data = df.copy()
    data.reset_index(inplace = True)
    rvolmatch = data[data['volume'] > data['10dayavgopenvol'] * rvol]
    rvolmatch.reset_index(inplace = True)
    rvolmatch = rvolmatch[['date','ticker']]
    data = pd.merge(data,rvolmatch,left_on=['date','ticker'],right_on=['date','ticker'],how = 'inner')#.drop_duplicates()
    data.set_index('datetime',inplace = True)
    return data

bigmatch = openMatchRvol(df,1.5)

def gapfinder(df,gap = 0.02,absolute = False):
    data = df.copy()
    data.reset_index(inplace = True)
    opendf = data[data['match']=='opening match']
    closedf = data[data['match']=='closing match']
    closedf['prevclose'] = closedf.groupby('ticker')['close'].shift(1)
    closedffiltered = closedf[['date','ticker','prevclose']]
    data = pd.merge(data,closedffiltered,left_on=['date','ticker'],right_on=['date','ticker'],how = 'inner')
    if absolute == False:
        if np.sign(gap) ==1:
            data['filter'] = np.where((data['match']=='opening match')&(data['close']/data['prevclose']-1 > gap),1,0)
        else:
            data['filter'] = np.where((data['match']=='opening match')&(data['close']/data['prevclose']-1 < gap),1,0)
    else:
        data['filter'] = np.where((data['match']=='opening match')&(abs(data['close']/data['prevclose']-1) > gap),1,0)

    filterdf = data[data['filter']==1][['ticker','date']]
    data = pd.merge(data,filterdf,left_on=['date','ticker'],right_on=['date','ticker'],how = 'inner')
    data.drop(columns = 'filter',inplace = True)
    data.set_index('datetime',inplace = True)
    return data

gaps = gapfinder(df,-0.02,False)

bigmatchpag = openMatchRvol(gaps,2)

def addTradeColumns(data):
    data['signal'] = 0
    data['closesignal'] = 0
    data['state'] = 'no position'
    data['tradeid'] = 0
    return data

def gapUpFail(df,gap = 0.02,failby = datetime.time(10,15)):

    data = df.copy()
    data = gapfinder(data,0.02,False)
    data.reset_index(inplace= True)
    dayopendf = data[data['match']=='opening match'][['open','date','ticker']]
    dayopendf.rename(columns = {'open':'dayopen'},inplace = True)
    data = pd.merge(data,dayopendf,on=['ticker','date'],how = 'left')
    data.set_index('datetime',inplace = True)

    #conditions to filter data
    sellcondition = data['close'] < data['dayopen']
    timecondition = data['time'] < failby
    volcondition = data['volume'] > 0

    # dataframefiltered = data[sellcondition & timecondition & volcondition]
    #
    # filtereddates = dataframefiltered[['ticker','date']]
    #
    # dataframefilteredfullday = pd.merge(data,filtereddates,on = ['ticker','date'], how = 'left')
    # dataframefilteredfullday.set_index('datetime',inplace = True)
    #
    # dataframefilteredfullday = addTradeColumns(dataframefilteredfullday)
    #
    # groupdata = dataframefilteredfullday.groupby(['ticker',dataframefilteredfullday.index.date])
    #
    # for name , group in groupdata:
    #     for idx, col in group.iterrows():
    #         if col.time < failby and col.close < col.dayopen:
    #             col.signal = -1

    data['signal'] = np.where(sellcondition & timecondition & volcondition,-1,0)
    data = addInitalPositions(data)
    data = genTrades(data)
    data['positionval'] = data['position'].abs() * data['close']/100
    return data


x = gapUpFail(df)

def addInitalPositions(data):
    data['closesignal'] = 0
    data['state'] = 'no position'
    data['tradeid'] = 0
    data['decisionpx'] = np.where(data['signal']==1,data['close'],0)
    data['tradepx'] = np.where(data['signal'].shift(1)==1,data['wap'],np.where((data['signal'].shift(1)==-1),data['wap'],0))
    #init positions
    data['state'] = np.where(data['signal'].shift(1)==1,'long',np.where(data['signal'].shift(1)==-1,'short','no position'))
    data['position'] = np.where(data['signal'].shift(1)==1,10000/data['wap'],np.where((data['signal'].shift(1)==-1),-10000/data['wap'],0))
    data['longstop'] = data['low'].shift(1)/100
    data['shortstop'] = data['high'].shift(1)/100

    return data


i = gapUpFail(df)


tplot = sp.stockplots(bigmatchpag)
tplot.multiplot(2,7)
tplot.showplot()




def genEntrySignals(df):
    data = df.copy()
    data['signal'] =0
    data['closesignal'] = 0
    data['state'] = 'no position'
    data['tradeid'] = 0
    #trade signals
    volcondition  = data['volume'] > data['avat30'] * 15
    buycondition =  data['wap']>data['wap'].shift(1)
    sellcondition = data['wap']<data['wap'].shift(1)
    timecondition = data.index.time<datetime.time(15,55)

    data['signal'] = np.where((volcondition) & (buycondition) & (timecondition),1,np.where((volcondition)&(sellcondition) &(timecondition),-1,0))
    signalsdf = data[data['signal']!=0][['date','ticker','time']]
    data = data[data.set_index(['ticker','date']).index.isin(signalsdf.set_index(['ticker','date']).index)]
    data['decisionpx'] = np.where(data['signal']==1,data['close'],0)
    data['tradepx'] = np.where(data['signal'].shift(1)==1,data['wap'],np.where((data['signal'].shift(1)==-1),data['wap'],0))
    #init positions
    data['state'] = np.where(data['signal'].shift(1)==1,'long',np.where(data['signal'].shift(1)==-1,'short','no position'))
    data['position'] = np.where(data['signal'].shift(1)==1,10000/data['wap'],np.where((data['signal'].shift(1)==-1),-10000/data['wap'],0))
    data['longstop'] = data['low'].shift(1)/100
    data['shortstop'] = data['high'].shift(1)/100
    data = genTrades(data)
    data['positionval'] = data['position'].abs() * data['close']/100

    return data

def  genTrades(data):
    counter =1
    for i in range(1,len(data)):
        if data['position'].iloc[i-1]>0 and data['tradepx'].iloc[i] ==0:
            data['position'].iloc[i] = data['position'].iloc[i-1]
            data['state'].iloc[i] = data['state'].iloc[i-1]

            if (data['wap'].iloc[i]<data['longstop'].iloc[i]) or (data['position'].iloc[i] != 0 and data.index[i].time() ==datetime.time(16,9)):
                data['decisionpx'].iloc[i] = data['close'].iloc[i]
                data['closesignal'].iloc[i] = -1
                data['position'].iloc[i+1] =0
                data['tradepx'].iloc[i+1] = data['wap'].iloc[i+1]
                data['state'].iloc[i+1] = 'long'


        if data['position'].iloc[i-1]<0 and data['tradepx'].iloc[i] ==0:
            data['position'].iloc[i] = data['position'].iloc[i-1]
            data['state'].iloc[i] = data['state'].iloc[i-1]

            if (data['wap'].iloc[i]>data['shortstop'].iloc[i]) or (data['position'].iloc[i] != 0 and data.index[i].time() ==datetime.time(16,9)):
                data['decisionpx'].iloc[i] = data['close'].iloc[i]
                data['closesignal'].iloc[i] = 1
                data['position'].iloc[i+1] =0
                data['tradepx'].iloc[i+1] = data['wap'].iloc[i+1]
                data['state'].iloc[i+1] = 'short'

        if data['state'].iloc[i-1] == 'no position' and data['state'].iloc[i] != 'no position':
            data['tradeid'].iloc[i] = counter
            counter +=1
        elif data['state'].iloc[i-1] == data['state'].iloc[i]:
            data['tradeid'].iloc[i] = data['tradeid'].iloc[i-1]
        elif data['state'].iloc[i-1]!='no position' and data['state'].iloc[i]=='no position':
            data['tradeid'].iloc[i] = 0

    return data

def backtestAnalyse(df, comm=0.0008):

    data = df.copy()
    #data = data[(data['position']!=0)|(data['signal']!=0)|(data['closesignal']!=0)|(data['tradepx'])!=0]

    data['pl'] = 0
    #conditions
    longopen = (data['positionval']!=0) & (data['tradepx']!=0) & (data['position']>0)
    shortopen = (data['positionval']!=0) & (data['tradepx']!=0) & (data['position']<0)
    longclose = (data['positionval']==0) & (data['tradepx']!=0) & (data['position'].shift(1)>0)
    shortclose = (data['positionval']==0) & (data['tradepx']!=0) & (data['position'].shift(1)<0)
    longpositionhold = ((data['position'].shift(1) > 0) & (data['position'] > 0))
    shortpositionhold = ((data['position'].shift(1) < 0) & (data['position'] < 0))

    #vectorized to check whether opening or closing trade and which side or if just holding. adds p/l per minute column
    data['pl'] = np.where(longopen & (data['pl']==0),data['positionval'] - (data['position'] * data['wap']),np.where(longclose, \
                    data['positionval'].shift(1) - (data['position'].shift(1) * data['wap']),np.where(shortopen & (data['pl']==0), \
                    (data['position'].abs() * data['wap']) - data['positionval'],np.where(shortclose, \
                    (data['position'].shift(1).abs() * data['wap']) - data['positionval'].shift(1), \
                    np.where(longpositionhold , data['positionval'] - data['positionval'].shift(1),\
                    np.where(shortpositionhold ,data['positionval'].shift(1) - data['positionval'],0))))))

    data['tradecumpl'] = data.groupby('tradeid')['pl'].transform('cumsum')

    data['tradepxnet'] = np.where(data['state']=='long',data['tradepx']*(1-comm),np.where( \
        data['state']=='short',data['tradepx']*(1+comm),0))
    data['positioncalc'] = data['position']
    data['positioncalc'] = np.where((data['state']!='no position')&(data['position'].shift(1)!=0),data['position'].shift(1),data['positioncalc'])
    data['brokerage'] = np.where(data['tradepx']!=0,abs(data['positioncalc']*data['tradepx']*comm),0)
    return data

x= genEntrySignals(df)

test = backtestAnalyse(x)

testbtest = test[test['ticker']=='APT']

btest = sp.stockplots(test)
btest.multiplot(1,5)
btest.singleplot()
btest.backtestplot()
btest.showplot()



