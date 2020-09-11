


df.reset_index(inplace=True)
rolling = pf.ols.PandasRollingOLS(y = df.close,x = df.index,window = 15)
beta = rolling.beta
sterr = rolling.std_err
alpha = rolling.alpha
rollingreg = pd.concat([beta,sterr,alpha],axis =1).rename(columns = {'feature1':'beta15','std_err':'stderr15'})
df = df.merge(rollingreg,how = 'left',left_index=True,right_index=True)

stdevfilter = np.where(df.stderr15<1,1,0)
betafilterdown = np.where((df.beta15<=-0.4) ,1,0)


df['signal'] = np.where(stdevfilter & betafilterdown,1,0)

df2 = df[df['signal']==1]
df2 = df2[['date','ticker']]

data = pd.merge(df,df2.drop_duplicates(),how ='inner',left_on=['date','ticker'],right_on=['date','ticker'])
data['closesignal'] =0
data.ticker.unique()
test = sp.stockplots(data[data['ticker']=='APT'])
test.multiplot(4,7)
test.singleplot()
test.showplot()







