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
import filters
import datetime

imp.reload(filters)
dbconn = connectdb.connectdb()
dbconn.conn = dbconn.pgconnect()

query = """
SELECT * FROM asxminutedata1 where datetime < '31/10/2019' AND 
ticker in ('APT','APX') 
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

#add additional features
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

i = df[df['match'].notna()]

starttime = df[df['match']=='opening match']
endtime = df[df['match']=='closing match']
df['signal'] = 0

def addbarcount(df):
    data = df.copy()
    data['barcount'] = 0
    barcountlist = []
    prevcount =0

    for row in data.itertuples():
        if row.match =='opening match':
            barcountlist.append(1)
            prevcount =1

        elif barcountlist[-1]>0:
            barcountlist.append(prevcount +1 )
            prevcount +=1
        else:
            barcountlist.append(0)

    return barcountlist

def addsma(df,period =[5]):
    data = df.copy()
    for i in period:
        data['sma'+str(i)] = data.groupby(['date','ticker'])['close'].transform(lambda x: x.rolling(i).mean())
        data['smaprev'+str(i)] = data.groupby(['date','ticker'])['sma'+str(i)].shift(1)
    return data

x = addsma(df,[5,10,20])

def movingavgcross(df,signal = 'buy'):
    data = df.copy()
    above = data['sma5']>data['sma20']
    crossed = data['smaprev5']<=data['smaprev20']
    data['signal'] = np.where((above) & (crossed),signal,'')
    return data

test = movingavgcross(x)

def gapUpFail(df,gap = 0.02,failby = datetime.time(10,15)):

    data = df.copy()
    data = addTradeColumns(data)
    data = gapfinder(data,0.03,False)
    data.reset_index(inplace= True)
    dayopendf = data[data['match']=='opening match'][['open','date','ticker']]
    dayopendf.rename(columns = {'open':'dayopen'},inplace = True)
    data = pd.merge(data,dayopendf,on=['ticker','date'],how = 'left')
    data.set_index('datetime',inplace = True)

    barcount = 0
    closelist = []
    prevrow = data.iloc[0:1,:]

    for row in data.itertuples():

        if row.match =='opening match':
            barcount =1

        if barcount>0:
            if barcount <12:

                if row.close  < row.dayopen:
                    closelist.append(1)
                else:
                    closelist.append(0)

                if sum(closelist[-3:]) == 3:
                    sigrow = (data['ticker']==row.ticker)&(data.index == row.Index)
                    data.loc[sigrow,'signal']=-1

                if row.Index.time() > datetime.time(16, 58):
                    prevrow = 0
                    barcount = 0

                else:
                    prevrow = row
                    barcount +=1
            else:
                prevrow = row
        else:
            prevrow = row

    return data



def backtest(df,target=0.02,stop=0.01):
    openlong = False
    data = df.copy()
    trades = pd.DataFrame(columns=['datetime','ticker','side','price','quantity'])

    for row in data.itertuples():
        if openlong == False:
            if row.signal != '':
                openlong = True
                tradedict =  {'datetime':row.Index,'ticker':row.ticker,'side':row.signal,'price':row.close,'quantity':1000}
                stoppx = round(row.close*(1-stop),4)
                targetpx = round(row.close*(1+target),4)
                trades = trades.append(tradedict,ignore_index=True)
        elif openlong == True:
            if row.close < stoppx or row.close> targetpx or row.Index.time() > datetime.time(16, 58):
                tradedict =  {'datetime':row.Index,'ticker':row.ticker,'side':'sell','price':row.close,'quantity':1000}
                trades = trades.append(tradedict,ignore_index=True)
                openlong = False

    return trades


i= backtest(test)

# def addTradeColumns(data):
#     data['signal'] = 0
#     data['closesignal'] = 0
#     data['state'] = 'no position'
#     data['tradeid'] = 0
#     return data
#
# def addInitalPositions(data):
#     data['closesignal'] = 0
#     data['state'] = 'no position'
#     data['tradeid'] = 0
#     data['decisionpx'] = np.where(data['signal']==1,data['close'],0)
#     data['tradepx'] = np.where(data['signal'].shift(1)==1,data['wap'],np.where((data['signal'].shift(1)==-1),data['wap'],0))
#     #init positions
#     data['state'] = np.where(data['signal'].shift(1)==1,'long',np.where(data['signal'].shift(1)==-1,'short','no position'))
#     data['position'] = np.where(data['signal'].shift(1)==1,10000/data['wap'],np.where((data['signal'].shift(1)==-1),-10000/data['wap'],0))
#     data['longstop'] = data['low'].shift(1)/100
#     data['shortstop'] = data['high'].shift(1)/100
#
#     return data
#
# def  genTrades(data):
#     counter =1
#     for i in range(1,len(data)):
#         if data['position'].iloc[i-1]>0 and data['tradepx'].iloc[i] ==0:
#             data['position'].iloc[i] = data['position'].iloc[i-1]
#             data['state'].iloc[i] = data['state'].iloc[i-1]
#
#             if (data['wap'].iloc[i]<data['longstop'].iloc[i]) or (data['position'].iloc[i] != 0 and data.index[i].time() ==datetime.time(16,9)):
#                 data['decisionpx'].iloc[i] = data['close'].iloc[i]
#                 data['closesignal'].iloc[i] = -1
#                 data['position'].iloc[i+1] =0
#                 data['tradepx'].iloc[i+1] = data['wap'].iloc[i+1]
#                 data['state'].iloc[i+1] = 'long'
#
#
#         if data['position'].iloc[i-1]<0 and data['tradepx'].iloc[i] ==0:
#             data['position'].iloc[i] = data['position'].iloc[i-1]
#             data['state'].iloc[i] = data['state'].iloc[i-1]
#
#             if (data['wap'].iloc[i]>data['shortstop'].iloc[i]) or (data['position'].iloc[i] != 0 and data.index[i].time() ==datetime.time(16,9)):
#                 data['decisionpx'].iloc[i] = data['close'].iloc[i]
#                 data['closesignal'].iloc[i] = 1
#                 data['position'].iloc[i+1] =0
#                 data['tradepx'].iloc[i+1] = data['wap'].iloc[i+1]
#                 data['state'].iloc[i+1] = 'short'
#
#         if data['state'].iloc[i-1] == 'no position' and data['state'].iloc[i] != 'no position':
#             data['tradeid'].iloc[i] = counter
#             counter +=1
#         elif data['state'].iloc[i-1] == data['state'].iloc[i]:
#             data['tradeid'].iloc[i] = data['tradeid'].iloc[i-1]
#         elif data['state'].iloc[i-1]!='no position' and data['state'].iloc[i]=='no position':
#             data['tradeid'].iloc[i] = 0
#
#     return data



df[df['volume']>0].iloc[0]

df.groupby(['date','ticker'])['volume'].nonzero()


x = data[data['signal']<0]
days = x[['ticker','date']].drop_duplicates()
data.reset_index(inplace=True)
gapfaildf = pd.merge(data,days,left_on=['date','ticker'],right_on=['date','ticker'],how = 'inner')


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

btest = sp.stockplots(gapfaildf)
btest.multiplot(3,4)
btest.singleplot()
btest.backtestplot()
btest.showplot()



