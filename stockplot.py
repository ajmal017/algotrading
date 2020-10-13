import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly as plotly
import numpy as np
import features as ft
import math


class stockplots():

    def __init__(self,dataframe):
        self.dataframe = dataframe.copy()
        self.dataframe.reset_index(inplace = True)
        self.uniquedays = self.dataframe.groupby(['date', 'ticker'])[['date', 'ticker']].max().reset_index(drop = True)
        self.dataframe['datetime'] = self.dataframe['datetime'].astype(str)
        self.dataframe.sort_values(by = 'datetime')
        self.plotcount = 0
        self.issingle = True
        self.isindicatorplot = False
        if 'side' not in self.dataframe.columns:
            self.dataframe['side'] =np.nan
        print(str(self.countplots()) + ' unique days')

    def countplots(self):
        self.plotcount = len(self.uniquedays)
        return(self.plotcount)

    def secondaxislist(self):
        secaxlist = [[]]*self.rows
        for i in range(self.cols):
            secaxlist[0].append({"secondary_y": True})
        return secaxlist

    def calcFigDimensinos(self,chartnumber):
        self.pages = math.ceil(chartnumber/20)
        self.rows = 2
        self.cols = math.ceil(chartnumber/self.rows)
        return(self.rows,self.cols)

    def addtraces(self,j,i,overlays = []):

        self.fig.add_trace(go.Candlestick(
            x=self.myx,
            open=self.dataframefiltered.open,
            high=self.dataframefiltered.high,
            low=self.dataframefiltered.low,
            close=self.dataframefiltered.close),
            row=j,
            col=i
        )

        for overlay in overlays:

            self.fig.add_trace(go.Scatter(
                x = self.myx,
                y = self.dataframefiltered[overlay],
                mode = 'lines',
                name = str(overlay),
                xaxis = 'x1',
                marker_color='rgba(63, 63, 191, 1)'),
                row = j,
                col = i,
            )

        self.fig.add_trace(go.Bar(
            x=self.myx,
            y=self.dataframefiltered.volume,
            name='Volume',
            marker_color = 'indianred'),
            row=j,
            col=i,
            secondary_y=True
        )

        for row in range(len(self.trades)):

            if self.trades.side.iloc[row] =='buy':
                self.text = 'BUY'
                self.arrowcolour = 'Green'
                self.orientation = 40

            elif self.trades.side.iloc[row] =='sell':
                self.text = 'SELL'
                self.arrowcolour = 'Red'
                self.orientation = -40

            if self.issingle == True:
                self.x = self.trades.datetime.iloc[row]
            else:
                self.x = self.trades.time.iloc[row]

            self.fig.add_annotation(go.layout.Annotation(
                x = self.x,
                y =  self.trades.close.iloc[row],
                text = self.text,
                showarrow = True,
                arrowhead =1,
                arrowcolor = self.arrowcolour,
                arrowsize = 3,
                arrowwidth = 1,
                ax = 0,
                ay= self.orientation),
                row = j,
                col =i)

    def updatelayout(self,j,i):
        self.fig.update_yaxes(title_text='Price', row=j, col=i, secondary_y=False)
        self.fig.update_yaxes(title_text='Volume', range=[0, self.dataframefiltered.volume.max() * 3], secondary_y=True,
                              row=j, col=i)
        self.fig.update_xaxes(title_text='Time', row=j, col=i,type = 'category')
        self.fig.update_xaxes(rangeslider=dict(visible=False), row=j, col=i)

    def plotnames(self):
        self.titlelist = []
        if self.rows ==1 and self.cols ==1:
            self.titlelist.append(str(self.uniquedays.ticker.iloc[0]))
        else:
            for z in range(0,min((self.rows * self.cols),len(self.uniquedays))):
                self.titlelist.append(str(self.uniquedays.ticker.iloc[z]) + ' (' + str(self.uniquedays.date.iloc[z]) + ')')
        return self.titlelist

    def multiplot(self,numrows, numcols,overlays = []):
        self.rows = numrows
        self.cols = numcols
        self.fig = make_subplots(rows=self.rows, cols=self.cols, specs=self.secondaxislist(),subplot_titles=tuple(self.plotnames()))
        self.counter =0

        if self.rows ==1 and self.cols ==1:
            self.dataframefiltered = self.dataframe.copy()
            self.myx = self.dataframefiltered.datetime
            self.trades = self.dataframefiltered[(self.dataframefiltered['side']=='buy')|(self.dataframefiltered['side']=='sell')]
            self.addtraces(1,1,overlays)
            self.updatelayout(1,1)
        else:
            self.issingle = False

            for j in range(1,self.rows+1):
                for i in range(1,self.cols+1):
                    if len(self.uniquedays)!=1 and self.counter< len(self.uniquedays):
                        self.dataframefiltered = self.dataframe[(self.dataframe['ticker'] == self.uniquedays.ticker.iloc[self.counter])&
                                                            (self.dataframe['date'] == self.uniquedays.date.iloc[self.counter])]
                        self.dataframefiltered = self.dataframefiltered.sort_values(by = 'datetime')
                        self.myx = self.dataframefiltered.time
                        self.trades = self.dataframefiltered[(self.dataframefiltered['side']=='buy')|(self.dataframefiltered['side']=='sell')]
                        self.addtraces(j,i,overlays)
                        self.updatelayout(j,i)
                        self.counter +=1


        self.fig.update_layout(showlegend = False)
        self.showplot()

    def showplot(self):
        plotly.offline.plot(self.fig)

    def singleplot(self,overlays = []):
        self.issingle = True
        self.multiplot(1,1,overlays)


    def indicatorplot(self,indicators = ['smooth15min'],overlays = ['vwap']):
        self.trades = []
        # Takes dataframe and list of indicators which are column names and plots as sub plots
        self.rows = len(indicators)+1
        self.cols =1
        self.isindicatorplot = True
        self.dataframefiltered = self.dataframe.copy()
        self.axes = self.secondaxislist()
        self.myx = self.dataframefiltered.datetime
        self.rowheights = [1.3/(self.rows +1) for i in range(self.rows)]
        self.rowheights[0] = self.rowheights[0]*2
        self.fig = make_subplots(
            rows = self.rows,cols =self.cols,
            shared_xaxes= True,
            vertical_spacing=0.02,
            row_heights=self.rowheights,
            horizontal_spacing=0.05,
            #subplot_titles=(str(self.dataframe.ticker.iloc[0]),'Stats'),
            specs=  self.axes
        )
        self.addtraces(1,1,['vwap'])
        for j in range(2,self.rows+1):
                self.fig.add_trace(go.Scatter(
                x = self.myx,
                y = self.dataframefiltered[indicators[j-2]],
                mode = 'lines',
                name = str(indicators[j-2]),
                xaxis = 'x1',
                marker_color='rgba(63, 63, 191, 1)'),
                row = j,
                col = 1,
            )
        self.updatelayout(1,1)
        self.updatelayout(2,1)
        self.fig.update_layout(showlegend = False)

    def updatelayoutbacktest(self):
        self.fig.update_xaxes(title_text='Time', row=3 , col= 1 ,type = 'category')
        for rows in range(self.rows):
                self.fig.update_xaxes(rangeslider=dict(visible=False), row=rows +1 , col=1)
                self.fig.update_xaxes(row=rows +1 , col= 1 ,type = 'category')


    def backtestplot(self, spread = []):
        self.issingle = True
        self.dataframefiltered = self.dataframe.copy()
        self.trades = self.dataframefiltered[(self.dataframefiltered['side']=='buy')|(self.dataframefiltered['side']=='sell')]

        if spread:
            self.rows = 6
            self.cols = 2
            self.ticker1 = spread[0]
            self.ticker2 = spread[1]
            self.rowheights = [0.3,0.3,0.3,0.15,0.3,0.3]
            self.specs =  [[{"secondary_y": True},{'type':'table'}], \
                           [{"secondary_y": True},{'type':'scatter'}], \
                           [{"secondary_y": True},{'type':'scatter'}], \
                           [{"secondary_y": True},{'type':'scatter'}], \
                           [{"secondary_y": True},{'type':'scatter'}],
                           [{"secondary_y": True},{'type':'scatter'}]]

            self.titles = (spread[0],'Stats',spread[1],
                           'P/L Distribution','Spread','','Position','','P/L','','Equity Net')

            self.spreaddf = ft.spread(self.dataframe,[self.ticker1,self.ticker2])
            self.spreaddf.rename(columns = {self.ticker1+self.ticker2 + 'close':self.ticker1 + self.ticker2},inplace = True)
            self.tradestemp = self.trades[self.trades['ticker']==self.ticker1]
            self.spreaddf = self.spreaddf.merge(self.tradestemp[['side','datetime']],left_on = 'datetime',right_on = 'datetime', how = 'left')
            self.spreaddf = self.spreaddf[['datetime', self.ticker1 + self.ticker2, 'side']].melt(id_vars=['datetime', 'side'], var_name='ticker', value_name='close')
            self.myx = self.spreaddf.datetime
            self.dataframe = pd.concat([self.spreaddf,self.dataframe])
        else:
            self.rows = 3
            self.cols =2
            self.rowheights = [0.7, 0.3, 0.3]
            self.specs = [[{"secondary_y": True},{'type':'table'}], \
                          [{"secondary_y": True},{'type':'scatter'}], \
                          [{"secondary_y": True},{'type':'scatter'}]]
            self.titles = (str(self.dataframe.ticker.iloc[0]),'Stats','P/L Distribution','Position','P/L')


        self.backteststats()

        self.fig = make_subplots(
            rows = self.rows,cols =self.cols,
            shared_xaxes= True,
            vertical_spacing=0.02,
            row_heights=self.rowheights,
            column_width=[14,8],
            horizontal_spacing=0.07,
            subplot_titles= self.titles,
            specs= self.specs
        )

        self.fig.add_trace(go.Table(
            # header = dict(values = [name for name in self.tablecolumns],font = dict(size = 10),
            #               align = 'left'),
            cells = dict(
                values = [self.backtestdf.transpose().index,self.backtestdf.transpose().iloc[:]],
                align = "left")),
            row = 1,
            col = 2
        )

        self.fig.add_trace(go.Histogram(
            x = self.equity,
            histnorm = 'probability'),
        row =2,
        col = 2
        )
        self.tradestemp = self.trades
        if spread:
            self.tickers = self.dataframe.ticker.unique()
            for count, ticker in enumerate(spread):
                self.dataframefiltered = self.dataframe[self.dataframe['ticker'] == ticker]
                self.trades = self.tradestemp[self.tradestemp['ticker'] == ticker]
                self.addtraces(count+1,1,[])
        else:
            self.addtraces(1,1,[])

        self.trades = self.tradestemp
        self.dataframefiltered = self.dataframe

        self.fig.add_trace(go.Scatter(
            x = self.myx,
            y = self.dataframefiltered.quantity,
            mode = 'lines+markers',
            name = 'Quantity',
            marker_color='rgba(63, 63, 191, 1)'),
            row = self.rows - 2,
            col = 1,
        )

        self.fig.add_trace(go.Scatter(
            x = self.myx,
            y = self.dataframefiltered.netpl,
            mode = 'lines+markers',
            name = 'P&L',
            marker_color='rgba(63, 63, 191, 1)'),
            row = self.rows-1,
            col = 1,
        )
        self.fig.add_trace(go.Scatter(
            x = self.myx,
            y = self.equity,
            mode = 'lines+markers',
            name = 'equity',
            marker_color='rgba(63, 63, 191, 1)'),
            row = self.rows,
            col = 1,
        )


        if spread:

            self.dataframefiltered = self.dataframe[self.dataframe['ticker'] == self.ticker1 + self.ticker2]

            self.tradestemp = self.dataframefiltered[(self.dataframefiltered['side']=='buy') | (self.dataframefiltered['side']=='sell')]

            self.fig.add_trace(go.Scatter(
                x = self.dataframefiltered.datetime,
                y = self.dataframefiltered['close'],
                mode = 'lines',
                name = 'P&L',
                marker_color='rgba(63, 63, 191, 1)'),
                row = self.rows - 3,
                col = 1,
            )


            for row in range(len(self.tradestemp)):

                if self.tradestemp.side.iloc[row] == 'buy':
                    self.text = 'BUY'
                    self.arrowcolour = 'Green'
                    self.orientation = 40

                elif self.tradestemp.side.iloc[row] == 'sell':
                    self.text = 'SELL'
                    self.arrowcolour = 'Red'
                    self.orientation = -40


                self.x = self.tradestemp.datetime.iloc[row]

                self.fig.add_annotation(go.layout.Annotation(
                    x=self.x,
                    y=self.tradestemp.close.iloc[row],
                    text=self.text,
                    showarrow=True,
                    arrowhead=1,
                    arrowcolor=self.arrowcolour,
                    arrowsize=3,
                    arrowwidth=1,
                    ax=0,
                    ay= self.orientation),
                    row= self.rows - 3,
                    col=1)


        self.updatelayoutbacktest()
        self.fig.update_layout(showlegend = False)
        self.showplot()

    def backteststats(self):

        self.backtrades =self.dataframe.groupby('tradeid')
        self.equity = self.dataframe.groupby(['tradeid','datetime'])['netpl'].sum().cumsum()

        self.backtestdf = pd.DataFrame(
            {'Start':[],'End':[],'Total net P/L':[],'Brokerage':[],'# Trades':[],'# Winners':[],'# Losers':[],'# Scratch':[],
             'Win Rate':[],'Win/Loss':[],'Average Win':[],'Average Loss' :[],'Equity Final':[],'Equity Max':[], 'Equity Min':[], \
             'Average Trade Duration':[],'Best Trade':[],'Worst Trade':[]})

        self.backtestdf.loc[0,'Start'] = self.dataframe.datetime.min()
        self.backtestdf.loc[0,'End'] = self.dataframe.datetime.max()
        self.backtestdf.loc[0,'Total net P/L'] = round(self.dataframe['netpl'].sum(),4)
        self.backtestdf.loc[0,'Brokerage'] = round(self.dataframe.brokerage.sum(),4)
        self.backtestdf.loc[0,'# Trades'] = self.dataframe.tradeid.max() - self.dataframe.tradeid.min()  + 1
        self.backtestdf.loc[0,'# Winners'] = (self.backtrades['netpl'].sum()>0).sum()
        self.backtestdf.loc[0,'# Losers'] = (self.backtrades['netpl'].sum()<0).sum()
        self.backtestdf.loc[0,'# Scratch'] = (self.backtrades['netpl'].sum()==0).sum()
        self.backtestdf.loc[0,'Win Rate'] = round(self.backtestdf.loc[0,'# Winners']/self.backtestdf.loc[0,'# Trades'],2)
        self.backtestdf.loc[0,'Average Win'] = round(self.backtrades.sum()[self.backtrades['netpl'].sum()>0]['netpl'].mean(),2)
        self.backtestdf.loc[0,'Average Loss'] = round(self.backtrades.sum()[self.backtrades['netpl'].sum()<0]['netpl'].mean(),2)
        self.backtestdf.loc[0,'Win/Loss'] = round(self.backtestdf.loc[0,'Average Win']/abs(self.backtestdf.loc[0,'Average Loss']), 2)
        self.backtestdf.loc[0,'Equity Max'] = round(self.equity.max(), 2)
        self.backtestdf.loc[0,'Equity Min'] = round(self.equity.min(),2)
        self.backtestdf.loc[0,'Best Trade'] = round(self.backtrades['netpl'].sum().max(),2)
        self.backtestdf.loc[0,'Worst Trade'] = round(self.backtrades['netpl'].sum().min(),2)



























