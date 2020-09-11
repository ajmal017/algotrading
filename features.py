import pandas as pd
import numpy as np
import itertools

def addsma(df,period =[30],column = 'close',spread = False):
    data = df.copy()
    for i in period:
        if spread == False:
            data['sma'+str(i)+column] = data.groupby(['date','ticker'])[column].transform(lambda x: x.rolling(i).mean())
            data['sma'+str(i)+'prev' + column] = data.groupby(['date','ticker'])['sma'+str(i)+column].shift(1)
        else:
            data['sma' + str(i) + column] = data.groupby('date')[column].transform(lambda x: x.rolling(i).mean())
            data['sma' + str(i) + 'prev' + column] = data.groupby('date')['sma' + str(i) + column].shift(1)

    return data

def tyronestdev(df,period = 30):
    df['tystdevclose' + str(period)] = df.groupby(['date','ticker'])['close'].transform(lambda x: x.rolling(period).std())
    df['tystdevlow' + str(period)] = df.groupby(['date','ticker'])['low'].transform(lambda x: x.rolling(period).std())
    df['tystdevhigh' + str(period)] = df.groupby(['date','ticker'])['high'].transform(lambda x: x.rolling(period).std())
    df['tystdev'] = df['tystdevhigh' + str(period)] + df['tystdevlow' + str(period)] + df['tystdevclose' + str(period)]
    df = df.drop(columns = {'tystdevhigh' + str(period),'tystdevlow' + str(period),'tystdevclose' + str(period)})
    return df

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

#create vwap feature
def vwap(df):
    df['vwap'] = df.groupby(['ticker',df.index.date])['value'].transform('cumsum')/df.groupby(['ticker',df.index.date])['volume']\
    .transform('cumsum')
    return df
#create average volume at time
def avat(df):
    df['avat30'] = df.groupby(['ticker',df.index.time])['volume'].transform(lambda x: round(x.rolling(30).mean(),0))
    #create cumulative volume at time
    df['cavat30'] = df.groupby(['ticker',df.index.date])['avat30'].transform('cumsum')
    return df
#
def wap(df):
    df['wap'] = df[['close','volume','value']].apply(lambda x: x['close'] if x['volume'] == 0 else round(x['value']/x['volume'],4),axis =1)
    return df

def g_h_filter(data, x0=2, dx=1, g=6./10, h=.1, dt=1.):
    x = x0
    results = []
    for z in data:
        #prediction step
        x_est = x + (dx*dt)
        dx = dx

        # update step
        residual = z - x_est
        dx = dx    + h * (residual) / dt
        x  = x_est + g * residual
        results.append(x)
    return np.array(results)

def kalman(df):
    df['kalman'] = df.groupby('ticker')['close'].transform(g_h_filter,x0=df.close[0], dx=1, g=.5, h=.5, dt=1.)
    return df

def zeds(df):
    def zscore(x):
        m = x.expanding().mean()
        s = x.expanding().std()
        z = (x-m)/s
        return z
    df['zed'] = df.groupby(['ticker','date'])['close'].transform(zscore)
    return df

def prevclose(df):
    df['prevdayclose'] = df.groupby(['ticker','date'])['close'].transform('last').shift(1)
    df['daychg'] =  df['close']/df['prevdayclose'] - 1
    return df


def spread(df,ticker:list,vals = ['close','volume']):
    spreaddf = df.pivot_table(index = 'datetime',columns='ticker',values=vals)
    cross = itertools.combinations(ticker,2)

    for i in cross:
        spreaddf[('close',str(i[0]) + str(i[1]))] = spreaddf[('close',i[0])]/spreaddf[('close',i[1])]

    namelist = [j + i for i, j in spreaddf.columns]
    spreaddf = spreaddf.droplevel(0, axis=1)
    spreaddf.columns = namelist
    spreaddf['date'] = spreaddf.index.date
    spreaddf['time'] = spreaddf.index.time
    return spreaddf

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
