import connectdb
import pandas as pd
from sqlalchemy import create_engine
import numpy as np
import datetime
engine = create_engine('postgresql://postgres:Blacksmif6@localhost:5432/asxdata')
conn = engine.raw_connection()

dbconn = connectdb.connectdb()
dbconn.conn = dbconn.pgconnect()

queryall = """
SELECT * FROM asxminutedata
"""
colnames = ['open','high','low','close','volume','value','count','datetime','ticker','vwap','avat30','cavat30']
x = dbconn.pgquery(dbconn.conn,queryall,None)
df = pd.DataFrame(x, columns= colnames)
df.set_index('datetime',inplace=True)

#make sure it is sorted by ticker then time
df = df.groupby(['ticker' , df.index]).sum().reset_index(level = 0)
df = df.sort_values(by = ['ticker', 'datetime'], ascending = [True, True])

#resample to fill in missing minutes
df = df.groupby('ticker').resample('1min').asfreq().reset_index(drop = True, level = 0)

#use only data from market hours on trading days
start_keep = datetime.time(9,59)
finish_keep = datetime.time(16,10)
df = df[ (df.index.time >=start_keep) & (df.index.time <= finish_keep) & (df.index.weekday >=0) & (df.index.weekday <= 4)]

#filter out any days where there is zero volume for all stocks
df = df[df.groupby(df.index.date)['volume'].transform('sum')!=0]

#ensure no volume for mising bars
df[['volume','value','count']] = df[['volume','value','count']].fillna(value=0)

#drag down missing closing prices - consistent with IB
df[['ticker','close']] = df[['ticker','close']].fillna(method='ffill')

#set all values to close where zero volume bar
filllist = ['open','high','low']

for i in filllist:
    df[i] = df.groupby(['ticker' , df.index]).transform('sum').apply(lambda x: x['close'] if x[i]==0 else x[i],axis = 1)

# sort again to be safe
df = df.sort_values(by = ['ticker', 'datetime'], ascending = [True, True])

#create vwap feature
df['vwap'] = df.groupby(['ticker',df.index.date])['value'].transform('cumsum')/df.groupby(['ticker',df.index.date])['volume']\
    .transform('cumsum')
#create average volume at time
df['avat30'] = df.groupby(['ticker',df.index.time])['volume'].transform(lambda x: round(x.rolling(30).mean(),0))
#create cumulative volume at time
df['cavat30'] = df.groupby(['ticker',df.index.date])['avat30'].transform('cumsum')

df['wap'] = df[['close','volume','value']].apply(lambda x: x['close']/100 if x['volume'] == 0 else round(x['value']/x['volume'],4),axis =1)

#insert query
df.to_sql('asxminutedata1',engine,if_exists = 'append', index = True, chunksize=100000)


