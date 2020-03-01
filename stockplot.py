import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly as plotly
import numpy as np
import math

class stockplots():

    def __init__(self,dataframe):
        self.dataframe = dataframe.copy()
        self.dataframe.reset_index(inplace = True)
        self.uniquedays = self.dataframe.groupby(['date', 'ticker'])['date', 'ticker'].max().reset_index(drop = True)
        self.dataframe['datetime'] = self.dataframe['datetime'].astype(str)
        self.dataframe.sort_values(by = 'datetime')
        self.plotcount = 0
        self.issingle = True
        if 'signal' not in self.dataframe.columns:
            self.dataframe['signal'] =0
            self.dataframe['closesignal'] = 0
        print(self.countplots())

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

    def addtraces(self,j,i):

        self.fig.add_trace(go.Candlestick(
            x=self.myx,
            open=self.dataframefiltered.open/100,
            high=self.dataframefiltered.high/100,
            low=self.dataframefiltered.low/100,
            close=self.dataframefiltered.close/100),
            row=j,
            col=i
        )

        self.fig.add_trace(go.Scatter(
            x = self.myx,
            y = self.dataframefiltered.vwap,
            mode = 'lines',
            name = 'vwap',
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

        for row in range(len(self.signals)):

            if self.signals.signal.iloc[row] > 0 or self.signals.closesignal.iloc[row]>0:
                self.text = 'BUY'
                self.arrowcolour = 'Green'
                self.orientation = 40

            elif self.signals.signal.iloc[row] < 0 or self.signals.closesignal.iloc[row]<0:
                self.text = 'SELL'
                self.arrowcolour = 'Red'
                self.orientation = -40

            if self.issingle == True:
                self.x = self.signals.datetime.iloc[row]
            else:
                self.x = self.signals.time.iloc[row]

            self.fig.add_annotation(go.layout.Annotation(
                x = self.x,
                y =  self.signals.close.iloc[row]/100,
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

    def multiplot(self,numrows, numcols):
        self.rows = numrows
        self.cols = numcols
        self.fig = make_subplots(rows=self.rows, cols=self.cols, specs=self.secondaxislist(),subplot_titles=tuple(self.plotnames()))
        self.counter =0

        if self.rows ==1 and self.cols ==1:
            self.dataframefiltered = self.dataframe
            self.myx = self.dataframefiltered.datetime
            self.signals = self.dataframefiltered[(self.dataframefiltered['signal']!=0)|(self.dataframefiltered['closesignal']!=0) ]
            self.addtraces(1,1)
            self.updatelayout(1,1)
        else:
            self.issingle = False

            for j in range(1,self.rows+1):
                for i in range(1,self.cols+1):
                    if len(self.uniquedays)!=1 and self.counter< len(self.uniquedays):
                        self.dataframefiltered = self.dataframe[(self.dataframe['ticker'] == self.uniquedays.ticker.iloc[self.counter])&
                                                            (self.dataframe['date'] == self.uniquedays.date.iloc[self.counter])]
                        self.myx = self.dataframefiltered.time
                        self.signals = self.dataframefiltered[(self.dataframefiltered['signal']!=0)|(self.dataframefiltered['closesignal']!=0)]
                        self.addtraces(j,i)
                        self.updatelayout(j,i)
                        self.counter +=1


        self.fig.update_layout(showlegend = False)

    def showplot(self):
        plotly.offline.plot(self.fig)

    def singleplot(self):
        self.issingle = True
        self.multiplot(1,1)

    def updatelayoutbacktest(self):
        self.fig.update_xaxes(title_text='Time', row=3 , col= 1 ,type = 'category')
        for rows in range(self.rows):
                self.fig.update_xaxes(rangeslider=dict(visible=False), row=rows +1 , col=1)
                self.fig.update_xaxes(row=rows +1 , col= 1 ,type = 'category')

    def backtestplot(self):
        self.rows = 3
        self.cols =2
        self.issingle = True
        self.dataframefiltered = self.dataframe
        self.myx = self.dataframefiltered.datetime
        self.signals = self.dataframefiltered[(self.dataframefiltered['signal']!=0)|(self.dataframefiltered['closesignal']!=0) ]
        self.backteststatssingle()
        self.fig = make_subplots(
            rows = self.rows,cols =self.cols,
            shared_xaxes= True,
            vertical_spacing=0.02,
            row_heights=[0.7,0.3,0.3],
            column_width=[14,5],
            horizontal_spacing=0.05,
            subplot_titles=(str(self.dataframe.ticker.iloc[0]),'Stats'),
            specs=  [[{"secondary_y": True},{'type':'table'}],[{"secondary_y": True}, \
                    {'type':'scatter'}],[{"secondary_y": True},{'type':'scatter'}]]
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

        self.addtraces(1,1)
        self.addtraces(1,1)

        self.fig.add_trace(go.Scatter(
            x = self.myx,
            y = self.dataframefiltered.position,
            mode = 'lines+markers',
            name = 'Position',
            marker_color='rgba(63, 63, 191, 1)'),
            row = 2,
            col = 1,
        )

        self.fig.add_trace(go.Scatter(
            x = self.myx,
            y = self.dataframefiltered.tradecumpl,
            mode = 'lines+markers',
            name = 'P&L',
            marker_color='rgba(63, 63, 191, 1)'),
            row = 3,
            col = 1,
        )

        self.updatelayoutbacktest()
        self.fig.update_layout(showlegend = False)

    def backteststatssingle(self):

        self.dataframe['equity'] = 10000
        self.dataframe['equity'] = self.dataframe['equity'].shift(1) + self.dataframe['pl'].cumsum()
        self.dataframe.loc[0,'equity'] =10000
        self.tradeslast = self.dataframe.groupby('tradeid').last()

        self.backtestdf = pd.DataFrame(
            {'Start':[],'End':[],'Total Return Gross':[],'Brokerage':[],\
             'Total Return Net':[],'# Trades':[],'# Winners':[],'# Losers':[],'# Scratch':[],'Win Rate':[],'Win/Loss':[], \
             'Average Win':[],'Average Loss' :[],'Equity Final':[],'Equity Max':[], 'Equity Min':[], \
             'Average Trade Duration':[],'Best Trade':[],'Worst Trade':[]
            })

        self.backtestdf.loc[0,'Start'] = self.dataframe.datetime.min()
        self.backtestdf.loc[0,'End'] = self.dataframe.datetime.max()
        self.backtestdf.loc[0,'Total Return Gross'] = self.dataframe['equity'].iloc[-1]/self.dataframe['equity'].iloc[0]-1
        self.backtestdf.loc[0,'Brokerage'] = round(self.dataframe.brokerage.sum(),4)
        self.backtestdf.loc[0,'Total Return Net'] = round((self.dataframe['equity'].iloc[-1] - self.dataframe['brokerage'].sum())/self.dataframe['equity'].iloc[0]-1,2)
        self.backtestdf.loc[0,'# Trades'] = len(self.tradeslast)
        self.backtestdf.loc[0,'# Winners'] = sum(self.tradeslast.tradecumpl > 0)
        self.backtestdf.loc[0,'# Losers'] = sum(self.tradeslast.tradecumpl<0)
        self.backtestdf.loc[0,'# Scratch'] = sum(self.tradeslast.tradecumpl==0)
        self.backtestdf.loc[0,'Win Rate'] = round(self.backtestdf.loc[0,'# Winners']/self.backtestdf.loc[0,'# Trades'],2)
        self.backtestdf.loc[0,'Average Win'] = round(self.tradeslast.tradecumpl[self.tradeslast.tradecumpl>0].sum() / self.backtestdf.loc[0,'# Winners'],2)
        self.backtestdf.loc[0,'Average Loss'] = round(self.tradeslast.tradecumpl[self.tradeslast.tradecumpl<0].sum() / self.backtestdf.loc[0,'# Losers'],2)
        self.backtestdf.loc[0,'Win/Loss'] = round(self.backtestdf.loc[0,'Average Win']/self.backtestdf.loc[0,'Average Loss'] ,2)
        self.backtestdf.loc[0,'Equity Final'] = self.dataframe['equity'].iloc[-1] - self.dataframe.brokerage.sum()
        self.backtestdf.loc[0,'Equity Max'] = round(self.dataframe['equity'].max(),2)
        self.backtestdf.loc[0,'Equity Min'] = round(self.dataframe['equity'].min(),2)
        self.backtestdf.loc[0,'Best Trade'] = round(self.tradeslast.tradecumpl.max(),2)
        self.backtestdf.loc[0,'Worst Trade'] = round(self.tradeslast.tradecumpl.min(),2)



























