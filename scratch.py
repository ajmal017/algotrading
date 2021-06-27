import connectdb
import numpy as np
import pandas as pd
import stockplot as sp
import imp
pd.options.mode.chained_assignment = None
import filters
import features as ft
import datetime

imp.reload(ft)
imp.reload(sp)
dbconn = connectdb.connectdb()
dbconn.conn = dbconn.pgconnect()

query = """
SELECT * FROM asxminutedata where datetime > '31/10/2019' AND datetime < '29/02/2020' AND
ticker in ('APT')
"""

x = pd.read_csv('apt.csv')
colnames = ['open', 'high', 'low', 'close', 'volume', 'value', 'count', 'datetime', 'ticker']

df = pd.DataFrame(x, columns=colnames).set_index('datetime')
df = df.sort_values(by=['ticker', 'datetime'], ascending=[True, True])
df.index = pd.to_datetime(df.index)
df['date'] = df.index.date
df['time'] = df.index.time

plot = sp.stockplots(df)
plot.multiplot(3,4)
