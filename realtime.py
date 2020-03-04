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
        self.tickerlist = ['ALQ', 'ALU', 'AWC', 'ANN', 'APA', 'ALX', 'AST', 'BOQ', 'BPT', 'BEN', 'BSL', 'BLD', 'BXB',
                            'CAR', 'CHC', 'CIM', 'CWY', 'CCL', 'COL', 'CBA', 'CPU', 'CWN', 'DXS', 'DMP', 'DOW', 'EVN']#,
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

    def orderStatus(self, orderId: int, status: str, filled: float,
                    remaining: float, avgFillPrice: float, permId: int,
                    parentId: int, lastFillPrice: float, clientId: int,
                    whyHeld: str, mktCapPrice: float):
        super().orderStatus(orderId, status, filled, remaining,
                            avgFillPrice, permId, parentId, lastFillPrice, clientId, whyHeld, mktCapPrice)
        print("OrderStatus. Id:", orderId, "Status:", status, "Filled:", filled,
              "Remaining:", remaining, "AvgFillPrice:", avgFillPrice,
              "PermId:", permId, "ParentId:", parentId, "LastFillPrice:",
              lastFillPrice, "ClientId:", clientId, "WhyHeld:",
              whyHeld, "MktCapPrice:", mktCapPrice)

    def realtimeBar(self, reqId:TickerId, time:int, open_: float, high: float, low: float, close: float,
                        volume: int, wap: float, count: int):

        print(self.tickerdict[str(reqId)],str(datetime.fromtimestamp(time)),open_,high,low,close,volume,wap,count)
        if datetime.fromtimestamp(time).second == 0:
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
#TODO I don't think this will work if a stock is suspended so eventually need to exlude all stocks from tickerlist that are suspended
        if self.fiveseccount == len(self.tickerlist):
            self.fiveseccount = 0
            self.timefilterend = datetime.now() - timedelta(seconds = datetime.now().time().second)
            #read from database only times less than most recent completed minute
            self.selectqry = 'SELECT * FROM ' + newdbname + ' WHERE datetime <= ' + str(self.timefilterend)
            self.df= pd.DataFrame(dbconnection.pgquery(conn,selectqry,False),columns=self.colnames)
            #create new alpha model object
            self.alpha = al.alphamodel(self.df)
            #get all trades from signals
            self.tradesdf = self.alpha.gettrades()
            self.sendorders(self.tradesdf)


    def sendorders(self,trades):
        for index,row in trades.iterrows():
            ticker = createcontract(row['ticker'])
            side = row['side']
            quantity = row['quantity']
            limitpx = row['limitpx']

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
        for i in range(len(self.contracts)):
            print('sending ' + str(self.contracts[i].symbol))
            self.requestcounter +=1
            self.throttle()
            #self.sendorder()
            self.reqRealTimeBars(self.nextValidOrderId + i,self.contracts[i],5,'TRADES',True,[])


app = App()
app.connect(host = "127.0.0.1", port = 7497, clientId=0)
app.run()
