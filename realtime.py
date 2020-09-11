from ibapi.wrapper import EWrapper
from ibapi.utils import *
from ibapi.client import EClient
from ibapi.contract import Contract
from ibapi.order_state import OrderState
from ibapi.common import BarData
from ibapi.order import Order
import pytz
from mycontracts import createContractObject, LimitOrder,createcontract
import pandas as pd
import connectdb as db
from datetime import datetime, timedelta
pd.options.mode.chained_assignment = None
import alphamodel as al
from dateutil import tz
TickerId = int
import time

#create database for live 5 second bars
newdbname = 'live_'+str(datetime.now().date().strftime('%d%m%Y'))
dbconnection = db.connectdb()
creatdbquery = 'CREATE TABLE if not exists ' + newdbname + """(
ticker varchar(10),
datetime timestamp,
open real,
high real,
low real,
close real,
volume bigint,
vwap double precision,
tradecount bigint
);
"""
selectqry = 'SELECT * FROM ' + newdbname
conn = dbconnection.pgconnect()
dbconnection.pgquery(conn,creatdbquery,False)

class App(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, wrapper=self)
        self.tickerlist = ['ANZ','WBC']#'APT','ALU', 'AWC', 'ANN', 'APA', 'ALX', 'AST', 'BOQ', 'BPT', 'BEN', 'BSL', 'BLD', 'BXB',
                           # 'CAR', 'CHC', 'CIM', 'CWY', 'CCL', 'COL', 'CBA', 'CPU', 'CWN', 'DXS', 'DMP', 'DOW', 'EVN']#,
                           # 'FLT', 'GMG', 'GPT', 'ILU', 'IPL', 'IAG', 'JHX', 'JBH', 'LLC', 'LNK', 'MQG', 'MFG', 'MPL',
                           # 'MGR', 'NAB', 'NCM', 'NHF', 'NEC', 'NST', 'OSH', 'ORI','ORG', 'ORA', 'OZL', 'QBE', 'QUB',
                           # 'RHC', 'REA', 'RWC', 'RMD', 'RIO', 'STO', 'SCG', 'SEK', 'SHL', 'SOL', 'S32', 'SKI', 'SGP',
                           # 'SUN', 'SYD', 'TAH', 'TLS', 'A2M', 'SGR', 'TPM', 'TCL', 'TWE', 'URW', 'VCX', 'VUK', 'WES',
                           # 'WBC', 'WHC', 'WPL', 'WOW', 'WOR', 'XRO']
        self.colnames = ['ticker','datetime','open','high','low','close','volume','vwap','count']
        self.contracts = createContractObject(self.tickerlist)
        self.tickerdict = {}
        self.lasttime = time.time()
        self.requestcounter = 0
        self.fiveseccount = 0
        self.ordermanager={}
        self.openpositiondict = {}
        self.openpositionlist = []
        self.openorderdict = {}
        self.spreads = [['ANZ','WBC']]
        self.lastclosedict = dict.fromkeys(self.tickerlist)
        self.closeparams = {'ANZWBC':{'stop':0.0009,'target':0.004}}
        self.openspreaddict = {}
        self.prevclose1 = 17.53
        self.prevclose2 = 16.81

    def nextOrderId(self):
        oid = self.nextValidOrderId
        self.nextValidOrderId += 1
        return oid

    def nextValidId(self, orderId:int):
        print("setting nextValidOrderId: ", orderId)
        self.nextValidOrderId = orderId
        for i,j in zip([str(z) for z in range(self.nextValidOrderId,self.nextValidOrderId+len(self.contracts))],self.tickerlist):
            self.tickerdict[i]=j
        self.start()

    def openOrder(self, orderId: int, contract: Contract, order: Order,
                  orderState: OrderState):
        super().openOrder(orderId, contract, order, orderState)
        print("OpenOrder. PermId: ", order.permId, "ClientId:", order.clientId, " OrderId:", orderId,
              "Account:", order.account, "Symbol:", contract.symbol, "SecType:", contract.secType,
              "Exchange:", contract.exchange, "Action:", order.action, "OrderType:", order.orderType,
              "TotalQty:", order.totalQuantity, "CashQty:", order.cashQty,
              "LmtPrice:", order.lmtPrice, "AuxPrice:", order.auxPrice, "Status:", orderState.status)

        order.contract = contract
        self.openorderdict[contract.symbol] = {'orderid':orderId}
        print(self.openorderdict)

    def position(self, account: str, contract: Contract, position: float,avgCost: float):
        self.openpositiondict[contract.symbol] = {'position':position}
        if self.openpositiondict[contract.symbol]['position'] == 0:
            self.openpositiondict.pop(contract.symbol)
        self.openpositionlist = list(self.openpositiondict)
        print('open position dict: '  + str(self.openpositiondict))

    # def orderStatus(self, orderId: int, status: str, filled: float,
    #                 remaining: float, avgFillPrice: float, permId: int,
    #                 parentId: int, lastFillPrice: float, clientId: int,
    #                 whyHeld: str, mktCapPrice: float):
    #     super().orderStatus(orderId, status, filled, remaining,
    #                         avgFillPrice, permId, parentId, lastFillPrice, clientId, whyHeld, mktCapPrice)
    #     print("OrderStatus. Id:", orderId, "Status:", status, "Filled:", filled,
    #           "Remaining:", remaining, "AvgFillPrice:", avgFillPrice,
    #           "PermId:", permId, "ParentId:", parentId, "LastFillPrice:",
    #           lastFillPrice, "ClientId:", clientId, "WhyHeld:",
    #           whyHeld, "MktCapPrice:", mktCapPrice)


    def realtimeBar(self, reqId:TickerId, time:int, open_: float, high: float, low: float, close: float,
                        volume: int, wap: float, count: int):

        print(self.tickerdict[str(reqId)],str(datetime.fromtimestamp(time)),open_,high,low,close,volume,wap,count)
        if datetime.fromtimestamp(time).second == 55:
            self.lastclosedict[self.tickerdict[str(reqId)]] = close
            self.fiveseccount +=1
        #create insert query
        insertquery = 'INSERT INTO '+ newdbname + ' VALUES (' + "'" +str(self.tickerdict[str(reqId)]) +"'" + ', ' +"'"+ \
                      str(datetime.fromtimestamp(time)) +"'"+ ', ' + str(open_) + ', ' + str(high) + ', ' + \
                      str(low) + ', ' + str(close) + ', ' + str(volume) + ', ' + str(wap) + ', ' + str(count) + ');'
        #insert into db
        dbconnection.pgquery(conn, insertquery, False)

        if self.fiveseccount>len(self.tickerlist):
            self.fiveseccount =0
        # every new minute read from database
        if self.fiveseccount == len(self.tickerlist):
            self.fiveseccount = 0
            self.timefilterend = datetime.now() - timedelta(seconds = datetime.now().time().second)
            #read from database only times less than or eaual to most recent completed minute
            self.selectqry = 'SELECT * FROM ' + newdbname + ' WHERE datetime <= ' + str(self.timefilterend)
            self.df= pd.DataFrame(dbconnection.pgquery(conn,selectqry,False),columns=self.colnames)
            #create new alpha model object if we can open positions
            if self.availablelist:
                self.alpha = al.alphamodel(self.df,self.prevclose1,self.prevclose2,self.spreads)
            #get latest data for open positions

            self.positiontracker = self.mapibpositions(self.openspreaddict,self.openpositiondict, self.lastclosedict)
            print('position tracker:' + str(self.positiontracker))


            #self.openposdf = self.alpha.data[self.alpha.data.ticker.isin(self.openpositionlist)][-1:]
            #get which stocks have no open positions
            self.availablelist = list(set(self.tickerlist) - set(self.openpositionlist))
            print('available list: ' + str(self.availablelist))
            #check if need to be closed
            if self.positiontracker:
                self.closetradesdf = self.GetCloseSpreadTrades(self.positiontracker)
                if not self.closetradesdf.empty:
                    self.sendorders(self.closetradesdf)

            #generate new trades
            #self.tradestoopen = self.alpha.generateNewTrades(self.availablelist)
            self.tradestoopen, self.availablelist = self.alpha.generateNewSpreadTrades(self.availablelist)

            # if not self.tradestoopen.empty:
            #     self.ordermanager = self.orderManage(self.tradestoopen)
            #     self.sendorders(self.tradestoopen)


            if not self.tradestoopen.empty:
                self.ordermanager = self.orderManage(self.tradestoopen)
                print('order manager: ' + str(self.ordermanager))
                self.sendorders(self.tradestoopen, spread = True)

    def mapibpositions(self,algo,ib,price):
        #function to map ib output to algo input
        for key in algo:
            for ticker in range(2):
                if ticker  == 0:
                    pos1 = ib[key[:3]]['position']
                    price1 = price[key[:3]]
                    algo[key]['position'].append(pos1)
                    algo[key]['stocksclose'].append(price1)
                else:
                    pos2 = ib[key[3:]]['position']
                    price2 = price[key[3:]]
                    algo[key]['position'].append(pos2)
                    algo[key]['close'] = price1/price2
                    algo[key]['stocksclose'].append(price2)

        return  algo

    def orderManage(self,data):
        #takes a dataframe of new trades and returns a dict of ticker, target and stop
        for row in data.itertuples():
            if row.signal =='buy':
                target = round(row.close * ( 1 + self.closeparams[row.ticker]['target']),4)
                stop = round(row.close * (1 - self.closeparams[row.ticker]['stop']),4)
                self.ordermanager[row.ticker] = {'last' : row.close,'stop':stop,'target':target,'timeexpiry':
                datetime(datetime.now().year,datetime.now().month, datetime.now().day, 15,50)}

            elif row.signal == 'sell':
                target = round(row.close * ( 1 - self.closeparams[row.ticker]['target']),4)
                stop = round(row.close * (1 + self.closeparams[row.ticker]['stop'],4))
                self.ordermanager[row.ticker] = {'last' : row.close,'stop':stop,'target':target,'timeexpiry':
                datetime(datetime.now().year,datetime.now().month, datetime.now().day, 15,50)}

        return self.ordermanager


    def GetCloseTrades(self,data):
        #iterates through open trades df compares last to ordermanage dict to see if needs to close returns df of trades
        #calcels any open order and cancels before closing
        closedict = {'ticker':[],'signal':[],'close':[],'quantity':[]}

        for row in data.itertuples():
            ticker = row.ticker
            position = self.openpositiondict[ticker]['position']
            if position>0:
                if row.close >self.ordermanager[ticker]['target'] or row.close<self.ordermanager[ticker]['stop'] or row.Index >self.ordermanager[ticker]['timeexpiry']:
                    signal = 'sell'
                    closedict['ticker'].append(ticker)
                    closedict['signal'].append(signal)
                    closedict['close'].append(row.close)
                    closedict['quantity'].append(abs(position))
                    if ticker in self.openorderdict.keys():
                        self.cancelOrder(self.openorderdict[ticker]['orderid'])

            elif position <0:
                if row.close<self.ordermanager[ticker]['target'] or row.close>self.ordermanager[ticker]['stop'] or row.Index >self.ordermanager[ticker]['timeexpiry']:
                    signal = 'buy'
                    closedict['ticker'].append(ticker)
                    closedict['signal'].append(signal)
                    closedict['close'].append(row.close)
                    closedict['quantity'].append(abs(position))
                    if ticker in self.openorderdict.keys():
                        self.cancelOrder(self.openorderdict[ticker]['orderid'])

        return pd.DataFrame(closedict)

    def GetCloseSpreadTrades(self,data):
        closedict = {'ticker': [], 'signal': [], 'close': [], 'quantity': []}
        for key in data.keys():
            if data[key]['position'][0] > 0:
                if key['close'] > self.ordermanager[key]['target'] or key['close'] <self.ordermanager[key]['stop'] or datetime.now() > self.ordermanager[key]['timeexpiry']:
                    self.positiontracker.pop(key, None)
                    self.ordermanager.pop(key,None)
                    self.openspreaddict.pop(key,None)
                    for i in range(2):
                        if i ==0:
                            ticker = key[:3]
                            signal = 'sell'
                            closedict['ticker'].append(ticker)
                            closedict['signal'].append(signal)
                            closedict['close'].append(data[key]['stocksclose'][0])
                            closedict['quantity'].append(data[key]['position'][0])
                            if ticker in self.openorderdict.keys():
                                self.cancelOrder(self.openorderdict[ticker]['orderid'])
                        else:
                            ticker = key[3:]
                            signal = 'buy'
                            closedict['ticker'].append(ticker)
                            closedict['signal'].append(signal)
                            closedict['close'].append(data[key]['stocksclose'][1])
                            closedict['quantity'].append(data[key]['position'][1])
                            if ticker in self.openorderdict.keys():
                                self.cancelOrder(self.openorderdict[ticker]['orderid'])

            if data[key]['position'][0] < 0:
                if key['close'] < self.ordermanager[key]['target'] or key['close'] > self.ordermanager[key]['stop'] or datetime.now() > self.ordermanager[key]['timeexpiry']:
                    self.positiontracker.pop(key, None)
                    self.ordermanager.pop(key, None)
                    self.openspreaddict.pop(key, None)
                    for i in range(2):
                        if i ==0:
                            ticker = key[:3]
                            signal = 'buy'
                            closedict['ticker'].append(ticker)
                            closedict['signal'].append(signal)
                            closedict['close'].append(data[key]['stocksclose'][0])
                            closedict['quantity'].append(data[key]['position'][0])
                            if ticker in self.openorderdict.keys():
                                self.cancelOrder(self.openorderdict[ticker]['orderid'])
                        else:
                            ticker = key[3:]
                            signal = 'sell'
                            closedict['ticker'].append(ticker)
                            closedict['signal'].append(signal)
                            closedict['close'].append(data[key]['stocksclose'][1])
                            closedict['quantity'].append(data[key]['position'][1])
                            if ticker in self.openorderdict.keys():
                                self.cancelOrder(self.openorderdict[ticker]['orderid'])

        return pd.DataFrame(closedict)


    def sendorders(self,trades,spread = False):


        for index,row in trades.iterrows():
            if spread == True:
                for leg in range(2):
                    if leg ==0:
                        ticker = createcontract(row['ticker1'])
                        side = row['signal']
                        quantity = row['quantity']
                        if side == 'sell':
                            limitpx = round(self.lastclosedict[ticker.symbol] * 0.998, 2)
                        else:
                            print(self.lastclosedict[ticker.symbol])
                            limitpx = round(self.lastclosedict[ticker.symbol] * 1.002, 2)
                        self.placeOrder(self.nextOrderId(), ticker, LimitOrder(side, quantity, limitpx))
                        print(str(quantity) + ' ' + str(ticker.symbol))
                        self.openspreaddict = {row['ticker']:{'close':None,'position':[],'stocksclose':[]}}
                    else:
                        ticker = createcontract(row['ticker2'][-3:])
                        if side == 'buy':
                            side = 'sell'
                        else:
                            side = 'buy'
                        quantity = row['quantity']
                        if side == 'sell':
                            limitpx = round(self.lastclosedict[ticker.symbol] * 0.998, 2)
                        else:
                            limitpx = round(self.lastclosedict[ticker.symbol] * 1.002, 2)
                        print(str(quantity) + ' ' + str(ticker.symbol))
                        self.placeOrder(self.nextOrderId(), ticker, LimitOrder(side, quantity, limitpx))
                print('open spreaddict: ' + str(self.openspreaddict))
            else:
                if side =='sell':
                    limitpx = round(row['close']*0.98,2)
                else:
                    limitpx = round(row['close']*1.02,2)
                if side == 'buy':
                    desc = 'buying'
                elif side == 'sell':
                    desc = 'selling'
                print(desc + ' ' + str(quantity) + ' ' + str(ticker.symbol) )
                self.placeOrder(self.nextOrderId(), ticker,LimitOrder(side,quantity,limitpx))

    def throttle(self):
        self.waittime= self.lasttime + 601 - time.time()
        if self.requestcounter%50 ==0 and self.waittime>0:
            print('waiting for ' + str(self.waittime) + ' seconds')
            time.sleep(self.waittime)
        self.lasttime = time.time()

    def error(self, reqId, errorCode, errorString):
        print("Error. Id: " , reqId, " Code: " , errorCode , " Msg: " , errorString)

    def start(self):
        self.reqPositions()
        for i in range(len(self.contracts)):
            print('sending ' + str(self.contracts[i].symbol))
            self.requestcounter +=1
            self.throttle()
            self.reqRealTimeBars(self.nextValidOrderId + i,self.contracts[i],5,'TRADES',True,[])


app = App()
app.connect(host = "127.0.0.1", port = 7497, clientId=0)
app.run()
