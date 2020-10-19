from ibapi.wrapper import EWrapper
from ibapi.utils import *
from ibapi.client import EClient
from ibapi.contract import Contract, ContractDetails
from ibapi.order_state import OrderState
from ibapi.common import BarData
from ibapi.order import Order
import pickle
import pytz
from mycontracts import createContractObject, LimitOrder,createcontract, createSpreadContract,ComboLimitOrder,createcontractstk
import pandas as pd
import connectdb as db
from datetime import datetime, timedelta
pd.options.mode.chained_assignment = None
from ibapi.account_summary_tags import *
from ibapi.ticktype import *
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
        self.tickerlist = ['NEA','APX','BHP','FMG','RIO','FMG','APT','ALU','APX','WTC','SEK','CAR','COH','PME',
                           'ANN','SHL','RHC','AGL','ORG','ASX','TCL','XRO','NCM','NST','SGM','WOW','WES','CPU','DMP',
                           'HVN','JBH','STO','WPL','IEL','SOL','FLT','WEB','FPH','CSL','IAG','SUN','PDL','MFG','JHG','LNK',
                           'NXT','GMG','MP1']#,'OSH','RMD','REA','DHG','TNE','RWC','TWE','SYD','AST','SKI','BIN','CWY']

        self.colnames = ['ticker','datetime','open','high','low','close','volume','vwap','count']
        self.contracts = createContractObject(self.tickerlist)
        self.conids = {}
        self.tickerdict = {}
        self.lasttime = time.time()
        self.requestcounter = 0
        self.fiveseccount = 0
        self.prevcloseready =False
        self.ordermanager= self.loadit('ordermanager')
        self.wait = False
        self.openpositiondict = {}
        self.shortable = {}
        self.marketdepth = {}
        self.openpositionlist = []
        self.depthid = {}
        self.shortabledict = {}
        self.openorderdict = {}
        self.tradestoopen = pd.DataFrame()
        self.spreads = [['NEA','APX'],['NEA','APT'],['BHP','FMG'],['RIO','FMG'],['ALU','APX'],['APX','APT'],
                        ['APT','ALU'],['WTC','APT'],['WTC','ALU'],['WTC','APX'],['SEK','CAR'],['COH','PME'],
                        ['ANN','WTC'],['SHL','RHC'],['AGL','ORG'],['ASX','TCL'],['XRO','ALU'],['XRO','APT'],
                        ['RIO','SGM'],['COH','SEK'],['WOW','WES'],['ALU','CPU'],['DMP','HVN'],['NCM','NST'],
                        ['HVN','JBH'],['STO','WPL'],['IEL','SOL'],['FLT','WEB'],['FPH','COH'],['CSL','FPH'],
                        ['IAG','SUN'],['PDL','MFG'],['JHG','PDL'],['LNK','CPU'],['NXT','MP1']]#,['NXT','GMG'],
                        # ['ORG','WPL'],['OSH','STO'],['OSH','WPL'],['RMD','FPH'],['RMD','COH'],['CSL','RMD'],
                        # ['REA','DHG'],['SOL','COH'],['TNE','ALU'],['RWC','TWE'],['XRO','WTC'],['SYD','TCL'],
                        # ['AST','SKI'],['XRO','APX'],['ANN','COH'],['BIN','CWY'], ['SGM','BHP']]

        self.lastclosedict = dict.fromkeys(self.tickerlist)

        self.closeparams = {'NEAAPX':{'stop':0.018,'target':0.1},'NEAAPT':{'stop':0.018,'target':0.1},
                            'BHPFMG':{'stop':0.016,'target':0.024},'RIOFMG':{'stop':0.016,'target':0.026},
                            'ALUAPX':{'stop':0.016,'target':0.1},'APXAPT':{'stop':0.018,'target':0.1},
                            'APTALU':{'stop':0.018,'target':0.1},'WTCAPT':{'stop':0.018,'target':0.1},
                            'WTCALU':{'stop':0.016,'target':0.1},'WTCAPX':{'stop':0.018,'target':0.1},
                            'SEKCAR':{'stop':0.016,'target':0.1},'COHPME':{'stop':0.012,'target':0.1},
                            'ANNWTC':{'stop':0.018,'target':0.1},'SHLRHC':{'stop':0.018,'target':0.1},
                            'AGLORG':{'stop':0.018,'target':0.1},'ASXTCL':{'stop':0.012,'target':0.1},
                            'XROALU':{'stop':0.014,'target':0.1},'XROAPT':{'stop':0.018,'target':0.1},
                            'NCMNST':{'stop':0.012,'target':0.1},'RIOSGM':{'stop':0.018,'target':0.1},
                            'COHSEK':{'stop':0.016,'target':0.1},'WOWWES':{'stop':0.012,'target':0.026},
                            'ALUCPU':{'stop':0.018,'target':0.1},'DMPHVN':{'stop':0.012,'target':0.1},
                            'HVNJBH':{'stop':0.012,'target':0.1},'STOWPL':{'stop':0.012,'target':0.1},
                            'IELSOL':{'stop':0.018,'target':0.1},'FLTWEB':{'stop':0.014,'target':0.1},
                            'FPHCOH':{'stop':0.012,'target':0.1},'CSLFPH':{'stop':0.016,'target':0.1},
                            'IAGSUN':{'stop':0.012,'target':0.1},'PDLMFG':{'stop':0.012,'target':0.1},
                            'JHGPDL':{'stop':0.014,'target':0.1},'LNKCPU':{'stop':0.016,'target':0.1},
                            'NXTMP1':{'stop':0.018,'target':0.1},'NXTGMG':{'stop':0.016,'target':0.1},
                            'ORGWPL':{'stop':0.012,'target':0.1},'OSHSTO':{'stop':0.018,'target':0.1},
                            'OSHWPL':{'stop':0.016,'target':0.1},'RMDFPH':{'stop':0.016,'target':0.1},
                            'RMDCOH':{'stop':0.014,'target':0.1},'CSLRMD':{'stop':0.012,'target':0.1},
                            'READHG':{'stop':0.018,'target':0.1},'SOLCOH':{'stop':0.012,'target':0.1},
                            'TNEALU':{'stop':0.012,'target':0.1},'RWCTWE':{'stop':0.018,'target':0.1},
                            'XROWTC':{'stop':0.012,'target':0.1},'SYDTCL':{'stop':0.018,'target':0.1},
                            'ASTSKI':{'stop':0.018,'target':0.1},'XROAPX':{'stop':0.018,'target':0.1},
                            'ANNCOH':{'stop':0.018,'target':0.1},'BINCWY':{'stop':0.018,'target':0.1},
                            'SGMBHP':{'stop':0.018,'target':0.1}}

        self.openparams = {'NEAAPX' : {'roclow':-0.002, 'rochigh':0.002,'upper':0.028, 'lower':-0.028},
                           'NEAAPT' : {'roclow':-0.006, 'rochigh':0.006,'upper':0.028, 'lower':-0.028},
                           'BHPFMG' : {'roclow':-0.00, 'rochigh':0.00,'upper':0.018, 'lower':-0.018},
                           'RIOFMG' : {'roclow':-0.00, 'rochigh':0.00,'upper':0.018, 'lower':-0.018},
                           'ALUAPX' : {'roclow':-0.002, 'rochigh':0.002,'upper':0.022, 'lower':-0.022},
                           'APXAPT' : {'roclow':-0.00, 'rochigh':0.00,'upper':0.028, 'lower':-0.028},
                           'APTALU' : {'roclow':-0.008, 'rochigh':0.008,'upper':0.026, 'lower':-0.026},
                           'WTCAPT' : {'roclow':-0.002, 'rochigh':0.002,'upper':0.026, 'lower':-0.026},
                           'WTCALU' : {'roclow':-0.002, 'rochigh':0.002,'upper':0.032, 'lower':-0.032},
                           'WTCAPX' : {'roclow':-0.008, 'rochigh':0.008,'upper':0.03, 'lower':-0.03},
                           'SEKCAR' : {'roclow':-0.002, 'rochigh':0.002,'upper':0.02, 'lower':-0.02},
                           'COHPME' : {'roclow':-0.002, 'rochigh':0.002,'upper':0.024, 'lower':-0.024},
                           'ANNWTC' : {'roclow':-0.006, 'rochigh':0.006,'upper':0.03, 'lower':-0.03},
                           'SHLRHC': {'roclow': -0.006, 'rochigh': 0.006,'upper':0.016,'lower':-0.016},
                           'AGLORG' : {'roclow':-0.002, 'rochigh':0.002,'upper':0.024, 'lower':-0.024},
                           'ASXTCL' : {'roclow':-0.006, 'rochigh':0.006,'upper':0.018, 'lower':-0.018},
                           'XROALU' : {'roclow':-0.00, 'rochigh':0.00,'upper':0.024, 'lower':-0.024},
                           'XROAPT' : {'roclow':-0.00, 'rochigh':0.00,'upper':0.03, 'lower':-0.03},
                           'NCMNST' : {'roclow':-0.00, 'rochigh':0.00,'upper':0.02, 'lower':-0.02},
                           'RIOSGM' : {'roclow':-0.00, 'rochigh':0.00,'upper':0.026, 'lower':-0.026},
                           'COHSEK' : {'roclow':-0.00, 'rochigh':0.00,'upper':0.024, 'lower':-0.024},
                           'WOWWES' : {'roclow':-0.00, 'rochigh':0.00,'upper':0.02, 'lower':-0.02},
                           'ALUCPU' : {'roclow':-0.00, 'rochigh':0.00,'upper':0.026, 'lower':-0.026},
                           'DMPHVN' : {'roclow':-0.00, 'rochigh':0.00,'upper':0.026, 'lower':-0.026},
                           'HVNJBH' : {'roclow':-0.004, 'rochigh':0.004,'upper':0.024, 'lower':-0.024},
                           'STOWPL' : {'roclow':-0.004, 'rochigh':0.004,'upper':0.02, 'lower':-0.02},
                           'IELSOL' : {'roclow':-0.00, 'rochigh':0.00,'upper':0.024, 'lower':-0.024},
                           'FLTWEB' : {'roclow':-0.008, 'rochigh':0.008,'upper':0.028, 'lower':-0.028},
                           'FPHCOH' : {'roclow':-0.004, 'rochigh':0.004,'upper':0.024, 'lower':-0.024},
                           'CSLFPH' : {'roclow':-0.00, 'rochigh':0.00,'upper':0.024, 'lower':-0.024},
                           'IAGSUN' : {'roclow':-0.0, 'rochigh':0.00,'upper':0.028, 'lower':-0.028},
                           'PDLMFG' : {'roclow':-0.006, 'rochigh':0.006,'upper':0.026, 'lower':-0.026},
                           'JHGPDL' : {'roclow':-0.00, 'rochigh':0.00,'upper':0.026, 'lower':-0.026},
                           'LNKCPU' : {'roclow':-0.002, 'rochigh':0.002,'upper':0.028, 'lower':-0.028},
                           'NXTMP1': {'roclow': -0.008, 'rochigh': 0.008, 'upper': 0.024, 'lower': -0.024},
                           'NXTGMG': {'roclow': -0.002, 'rochigh': 0.002, 'upper': 0.024, 'lower': -0.024},
                           'ORGWPL': {'roclow': -0.002, 'rochigh': 0.002, 'upper': 0.022, 'lower': -0.022},
                           'OSHSTO': {'roclow': -0.006, 'rochigh': 0.006, 'upper': 0.024, 'lower': -0.024},
                           'OSHWPL': {'roclow': -0.004, 'rochigh': 0.004, 'upper': 0.024, 'lower': -0.024},
                           'RMDFPH': {'roclow': -0.004, 'rochigh': 0.004, 'upper': 0.022, 'lower': -0.022},
                           'RMDCOH': {'roclow': -0.00, 'rochigh': 0.00, 'upper': 0.022, 'lower': -0.022},
                           'CSLRMD': {'roclow': -0.006, 'rochigh': 0.006, 'upper': 0.022, 'lower': -0.022},
                           'READHG': {'roclow': -0.002, 'rochigh': 0.002, 'upper': 0.028, 'lower': -0.028},
                           'SOLCOH': {'roclow': -0.002, 'rochigh': 0.002, 'upper': 0.026, 'lower': -0.026},
                           'TNEALU': {'roclow': -0.002, 'rochigh': 0.002, 'upper': 0.022, 'lower': -0.022},
                           'RWCTWE': {'roclow': -0.002, 'rochigh': 0.002, 'upper': 0.022, 'lower': -0.022},
                           'XROWTC': {'roclow': -0.002, 'rochigh': 0.002, 'upper': 0.022, 'lower': -0.022},
                           'SYDTCL': {'roclow': -0.002, 'rochigh': 0.002, 'upper': 0.022, 'lower': -0.022},
                           'ASTSKI': {'roclow': -0.002, 'rochigh': 0.002, 'upper': 0.022, 'lower': -0.022},
                           'XROAPX': {'roclow': -0.002, 'rochigh': 0.002, 'upper': 0.022, 'lower': -0.022},
                           'ANNCOH': {'roclow': -0.002, 'rochigh': 0.002, 'upper': 0.022, 'lower': -0.022},
                           'BINCWY': {'roclow': -0.002, 'rochigh': 0.002, 'upper': 0.022, 'lower': -0.022},
                           'SGMBHP': {'roclow': -0.002, 'rochigh': 0.002, 'upper': 0.022, 'lower': -0.022}
                           }

        self.openspreaddict = self.loadit('openspreaddict')
        self.prevclosedict = {}
        self.maxpositiongross = {'NEAAPX':20000,'NEAAPT':20000,'BHPFMG':20000,'RIOFMG':20000,'ALUAPX':20000,
                                 'APXAPT':20000,'APTALU':20000,'WTCAPT':20000,'WTCALU':20000,'WTCAPX':20000,
                                 'SEKCAR':20000,'COHPME':20000,'ANNWTC':20000,'SHLRHC':20000,'AGLORG':20000,
                                 'ASXTCL':20000,'XROALU':20000,'XROAPT':20000,'NCMNST':20000,'RIOSGM':20000,
                                 'COHSEK':20000,'WOWWES':40000,'ALUCPU':20000,'DMPHVN':20000,'HVNJBH':20000,
                                 'STOWPL':40000,'IELSOL':20000,'FLTWEB':20000,'FPHCOH':20000,'CSLFPH':20000,
                                 'IAGSUN':40000,'PDLMFG':20000,'JHGPDL':20000,'LNKCPU':20000,'NXTMP1': 20000,
                                 'NXTGMG': 20000,'ORGWPL': 20000,'OSHSTO': 20000,'OSHWPL': 20000,'RMDFPH': 20000,
                                 'RMDCOH': 20000,'CSLRMD': 20000,'READHG': 20000,'SOLCOH': 20000,'TNEALU': 20000,
                                 'RWCTWE': 20000, 'XROWTC': 20000,'SYDTCL': 20000,'ASTSKI': 20000,'XROAPX': 20000,
                                 'ANNCOH': 20000,'BINCWY': 20000, 'SGMBHP': 20000,
                                 }
        self.positiontracker = {}
        self.availablespreads = {}
        self.availablefunds =0

    # def createConidDict(self):
    #     for ticker in self.tickerlist:
    #         contract  = createcontract(ticker)
    #         self.reqContractDetails(self.nextValidOrderId, contract)
    #


    # def contractDetails(self, reqId: int, contractDetails: ContractDetails):
    #     super().contractDetails(reqId, contractDetails)
    #     self.conids[contractDetails.contract.symbol] = contractDetails.contract.conId
    #
    #     if  len(self.conids) == 5:
    #         self.placeOrder(self.nextOrderId(), createSpreadContract('ANZ','WBC',self.conids), ComboLimitOrder('BUY', 1000, 0.4,True))
    #
    def getprevclose(self,contract):
        ordernumber = self.nextOrderId()
        self.prevclosedict[ordernumber] = [contract.symbol]
        queryTime = (datetime.today() - timedelta(days=1)).strftime("%Y%m%d %H:%M:%S")
        self.reqHistoricalData(ordernumber, contract, queryTime,"1 D", "1 day", "TRADES", 1, 1, False, [])


    def historicalData(self, reqId:int, bar: BarData):
        #print("HistoricalData. ReqId:", reqId, "BarData.", bar)
        self.prevclosedict[reqId].append(bar.close)

        if 1 not in [len(self.prevclosedict[i]) for i in self.prevclosedict.keys()]:
            self.prevclosedict = {self.prevclosedict[i][0]:self.prevclosedict[i][1] for i in self.prevclosedict.keys()}
            print(self.prevclosedict)
            self.prevcloseready = True





    def nextOrderId(self):
        oid = self.nextValidOrderId
        self.nextValidOrderId += 1
        return oid

    def nextValidId(self, orderId:int):
        #print("setting nextValidOrderId: ", orderId)
        self.nextValidOrderId = orderId
        for i,j in zip([str(z) for z in range(self.nextValidOrderId,self.nextValidOrderId+len(self.contracts))],self.tickerlist):
            self.tickerdict[i]=j
        self.start()

    def dumpit(self,data,filename):
        file = open(filename,'wb')
        pickle.dump(data,file)
        file.close()

    def loadit(self,filename):
        try:
            file = open(filename,'rb')
            myobj = pickle.load(file)
            file.close()
        except:
            myobj = {}
        return myobj


    def openOrder(self, orderId: int, contract: Contract, order: Order,
                  orderState: OrderState):
        super().openOrder(orderId, contract, order, orderState)
        # print("OpenOrder. PermId: ", order.permId, "ClientId:", order.clientId, " OrderId:", orderId,
        #       "Account:", order.account, "Symbol:", contract.symbol, "SecType:", contract.secType,
        #       "Exchange:", contract.exchange, "Action:", order.action, "OrderType:", order.orderType,
        #       "TotalQty:", order.totalQuantity, "CashQty:", order.cashQty,
        #       "LmtPrice:", order.lmtPrice, "AuxPrice:", order.auxPrice, "Status:", orderState.status)

        order.contract = contract
        self.openorderdict[contract.symbol] = {'orderid':orderId,'quantity':order.totalQuantity,'limitpx':order.lmtPrice}
        if orderState.status == "Filled":
            self.openorderdict.pop(contract.symbol)
        print('open orders ' + str(self.openorderdict))

    def position(self, account: str, contract: Contract, position: float,
                 avgCost: float):
        super().position(account, contract, position, avgCost)
        # print("Position.", "Account:", account, "Symbol:", contract.symbol, "SecType:",
        #       contract.secType, "Currency:", contract.currency,
        #       "Position:", position, "Avg cost:", avgCost)
        self.openpositiondict[contract.symbol] = {'position':position}
        if self.openpositiondict[contract.symbol]['position'] == 0:
            self.openpositiondict.pop(contract.symbol)
        self.openpositionlist = list(self.openpositiondict)
        print('open position dict: '  + str(self.openpositiondict))

    # def position(self, account: str, contract: Contract, position: float,avgCost: float):
    #     print('positions' + account,str(position))
    #     self.openpositiondict[contract.symbol] = {'position':position}
    #     if self.openpositiondict[contract.symbol]['position'] == 0:
    #         self.openpositiondict.pop(contract.symbol)
    #     self.openpositionlist = list(self.openpositiondict)
    #     print('open position dict: '  + str(self.openpositiondict))

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

        for key in self.openspreaddict.keys():
            for leg in range(2):
                if orderId in self.openspreaddict[key]['orderids'][leg].keys():
                    side = self.openspreaddict[key]['side']
                    if (leg == 0 and side =='sell') or (leg ==1 and side =='buy'):
                        self.openspreaddict[key]['orderids'][leg][orderId] = -filled
                    else:
                        self.openspreaddict[key]['orderids'][leg][orderId] = filled

                    self.openspreaddict[key]['position'][leg] = sum(self.openspreaddict[key]['orderids'][leg].values())
                    print('openspreads ' + str(self.openspreaddict))


        self.reqAccountSummary(1000000, "All", AccountSummaryTags.AvailableFunds)
        self.dumpit(self.openspreaddict,'openspreaddict')

    def realtimeBar(self, reqId:TickerId, time:int, open_: float, high: float, low: float, close: float,
                        volume: int, wap: float, count: int):

        if self.tickerdict[str(reqId)] not in self.prevclosedict.keys():
            cont = createcontractstk(self.tickerdict[str(reqId)])


        #print(self.tickerdict[str(reqId)],str(datetime.fromtimestamp(time)),open_,high,low,close,volume,wap,count)
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

            #self.updateShortable()

            self.availablelist = self.getavailablespreads()
            print('available list: ' + str(self.availablelist))

            if self.availablelist and datetime.now().time()< datetime(datetime.now().year,datetime.now().month,datetime.now().day,15,30).time() and self.prevcloseready == True:
                print('$$$$$$$$$$$$$$$$$$$ Making that money $$$$$$$$$$$$$$$$$$$')
                print(self.prevclosedict)
                print(self.df)
                self.alpha = al.alphamodel(self.df,self.prevclosedict)
                self.tradestoopen = self.alpha.generateNewSpreadTrades(self.availablelist,self.openparams)
            #get latest data for open positions

            #map spreads to streaming stock data
            if self.openpositionlist:
                self.positiontracker = self.mapibpositions(self.openspreaddict, self.lastclosedict)
                self.excludeDualopen()
                if self.positiontracker:
                    print('position tracker:' + str(self.positiontracker))

            #check if need to be closed
            if self.positiontracker:
                self.closetradesdf = self.GetCloseSpreadTrades(self.positiontracker)
                if not self.closetradesdf.empty:
                    self.sendorders(self.closetradesdf)

            if not self.tradestoopen.empty:
                self.tradestoopen = self.removeUnborrowable(self.tradestoopen)
                print(self.tradestoopen)
                if not self.tradestoopen.empty:
                    self.sizeit(self.tradestoopen)
                    self.ordermanager = self.orderManage(self.tradestoopen)
                    print('order manager: ' + str(self.ordermanager))
                    #self.positiontracker = self.createspreadsdict(self.tradestoopen)
                    #self.executionmanager(self.tradestoopen)

                    self.sendorders(self.tradestoopen, spread = True)

    def sizeit(self,tradestoopen):

        tradestoopen['maxpos'] = tradestoopen['ticker'].map(self.maxpositiongross)
        tradestoopen['ticker1last'] = tradestoopen['ticker1'].map(self.lastclosedict)
        tradestoopen['ticker2last'] = tradestoopen['ticker2'].map(self.lastclosedict)
        tradestoopen['quantity1'] = round((tradestoopen['maxpos']/2) / tradestoopen['ticker1last'])
        tradestoopen['quantity2'] = round((tradestoopen['maxpos']/2) / tradestoopen['ticker2last'])

        print('available funds: '+ str(self.availablefunds))

        if float(self.availablefunds) < 5000:
            tradestoopen = pd.DataFrame()

        elif tradestoopen['maxpos'].sum()/2.5 > float(self.availablefunds):
            print(tradestoopen)
            scalar = self.availablefunds / ((tradestoopen['maxpos'].sum())/2.5)
            tradestoopen['quantity1'] = round(tradestoopen['quantity1'] * scalar)
            tradestoopen['quantity2'] = round(tradestoopen['quantity2'] * scalar)
            if tradestoopen['quantity1'].iloc[0] * tradestoopen['ticker1last'].iloc[0] < 9000:
                tradestoopen = pd.DataFrame()

        self.reqAccountSummary(1000000, "All", AccountSummaryTags.AvailableFunds)

        return tradestoopen

    def excludeDualopen(self):
        count = 0
        for ticker in self.tickerlist:
            for spread in self.openspreaddict.keys():
                if ticker in spread:
                    count+=1
                if count ==2:
                    for i in self.spreads:
                        if ticker in i:
                            self.spreads.pop(self.spreads.index(i))
                            print('dualopen spreads: ' + str(self.spreads))

            count =0

    def removeUnborrowable(self,trades):
        keeptickers = []
        for index, row in trades.iterrows():
            if row['signal'] =='buy':
                sellticker = row['ticker2']
                if self.shortable[sellticker]==3:
                    keeptickers.append(row['ticker'])
            elif row['signal'] =='sell':
                sellticker = row['ticker1']
                if self.shortable[sellticker]==3:
                    keeptickers.append(row['ticker'])
        return trades[trades['ticker'].isin(keeptickers)]


    def accountSummary(self, reqId: int, account: str, tag: str, value: str,
                       currency: str):
        super().accountSummary(reqId, account, tag, value, currency)
        # print("AccountSummary. ReqId:", reqId, "Account:", account,
        #       "Tag: ", tag, "Value:", value, "Currency:", currency)
        self.availablefunds = value
        print('available funds' + str(value))
        self.cancelAccountSummary(1000000)

    def getavailablespreads(self):
        cantrade = []
        availabletickers =  self.df.ticker.unique()
        #check if we have data today
        for spread in self.spreads:
            if spread[0] in availabletickers and spread[1] in availabletickers:
                cantrade.append([spread[0],spread[1]])

        spreadtickers = [i[0]+i[1] for i in cantrade]
        # check if spread already open
        cantrade = list(set(spreadtickers) - set(self.openspreaddict.keys()))
        cantrade = [[i[:3],i[3:]] for i in cantrade]
        return cantrade

    def resetpickle(self):
        for i in ['openspreaddict','ordermanager']:
            data = {}
            file = open(i, 'wb')
            pickle.dump(data, file)
            file.close()

    def mapibpositions(self,algo,price):
        #function to map ib output to algo input for spreads
        for key in algo:
            for ticker in range(2):
                if ticker  == 0:
                    price1 = price[key[:3]]
                    algo[key]['stocksclose'][0] = price1
                else:
                    price2 = price[key[3:]]
                    algo[key]['stocksclose'][1] = price2

            if not price1 == None and not price2 == None:
                algo[key]['close'] = price1 / price2

        return algo

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
                stop = round(row.close * (1 + self.closeparams[row.ticker]['stop']),4)
                self.ordermanager[row.ticker] = {'last' : row.close,'stop':stop,'target':target,'timeexpiry':
                datetime(datetime.now().year,datetime.now().month, datetime.now().day, 15,50)}
        self.dumpit(self.ordermanager,'ordermanager')
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
        dicttoclear = []
        closedict = {'ticker': [], 'signal': [], 'close': [], 'quantity': [],'sectype':[]}
        for key in data.keys():
            if data[key]['position'][0] > 0:
                if data[key]['close'] > self.ordermanager[key]['target'] or data[key]['close'] <self.ordermanager[key]['stop'] or datetime.now() > self.ordermanager[key]['timeexpiry']:
                    dicttoclear.append(key)
                    for i in range(2):
                        if i ==0:
                            ticker = key[:3]
                            if ticker in list(self.openpositiondict.keys()):
                                signal = 'sell'
                                closedict['ticker'].append(ticker)
                                closedict['signal'].append(signal)
                                closedict['close'].append(data[key]['stocksclose'][0])
                                closedict['quantity'].append(abs(data[key]['position'][0]))
                                closedict['sectype'].append(data[key]['sectype'][0])

                            if ticker in self.openorderdict.keys():
                                self.cancelOrder(self.openorderdict[ticker]['orderid'])

                        else:
                            ticker = key[3:]
                            if ticker in list(self.openpositiondict.keys()):
                                signal = 'buy'
                                closedict['ticker'].append(ticker)
                                closedict['signal'].append(signal)
                                closedict['close'].append(data[key]['stocksclose'][1])
                                closedict['quantity'].append(abs(data[key]['position'][1]))
                                closedict['sectype'].append(data[key]['sectype'][1])
                            if ticker in self.openorderdict.keys():
                                self.cancelOrder(self.openorderdict[ticker]['orderid'])


            elif data[key]['position'][0] < 0:
                if data[key]['close'] < self.ordermanager[key]['target'] or data[key]['close'] > self.ordermanager[key]['stop'] or datetime.now() > self.ordermanager[key]['timeexpiry']:
                    dicttoclear.append(key)
                    for i in range(2):
                        if i ==0:
                            ticker = key[:3]
                            if ticker in list(self.openpositiondict.keys()):
                                signal = 'buy'
                                closedict['ticker'].append(ticker)
                                closedict['signal'].append(signal)
                                closedict['close'].append(data[key]['stocksclose'][0])
                                closedict['quantity'].append(abs(data[key]['position'][0]))
                                closedict['sectype'].append(data[key]['sectype'][0])
                            if ticker in self.openorderdict.keys():
                                self.cancelOrder(self.openorderdict[ticker]['orderid'])

                        else:
                            ticker = key[3:]
                            if ticker in list(self.openpositiondict.keys()):
                                signal = 'sell'
                                closedict['ticker'].append(ticker)
                                closedict['signal'].append(signal)
                                closedict['close'].append(data[key]['stocksclose'][1])
                                closedict['quantity'].append(abs(data[key]['position'][1]))
                                closedict['sectype'].append(data[key]['sectype'][1])
                            if ticker in self.openorderdict.keys():
                                self.cancelOrder(self.openorderdict[ticker]['orderid'])

        self.cleardicts(dicttoclear)
        return pd.DataFrame(closedict)

    def cleardicts(self,dicttoclear):
        if dicttoclear:
            for i in dicttoclear:
                self.positiontracker.pop(i, None)
                self.ordermanager.pop(i, None)
                self.dumpit(self.ordermanager, 'ordermanager')
                self.openspreaddict.pop(i, None)
                self.dumpit(self.openspreaddict, 'openspreaddict')

    # def updateMktDepth(self, reqId: TickerId, position: int, operation: int,
    #                    side: int, price: float, size: int):
    #     super().updateMktDepth(reqId, position, operation, side, price, size)
    #     print("UpdateMarketDepth. ReqId:", reqId, "Position:", position, "Operation:",
    #           operation, "Side:", side, "Price:", price, "Size:", size)
    #
    #     if reqId in self.marketdepth:
    #         self.marketdepth[self.depthid[reqId]][side]['size'] = size
    #         self.marketdepth[self.depthid[reqId]][side]['price'] = price
    #
    #     else:
    #         self.marketdepth[self.depthid[reqId]] = {0:{'size':None,'price':None}, 1:{'size':None,'price':None}}
    #         self.marketdepth[self.depthid[reqId]][side]['size'] = size
    #         self.marketdepth[self.depthid[reqId]][side]['price'] = price
    #
    #         self.spreadTradeR(self.tradestoopen)
    #
    #     for key in self.executedict.keys():
    #         for leg in range(2):
    #             if leg==0:
    #                 ticker = key[:3]
    #                 if orderId in self.openspreaddict[key]['orderids'][leg].keys():
    #                     side = self.openspreaddict[key]['side']
    #                     if (leg == 0 and side =='sell') or (leg ==1 and side =='buy'):
    #                         self.openspreaddict[key]['orderids'][leg][orderId] = -filled
    #                     else:
    #                         self.openspreaddict[key]['orderids'][leg][orderId] = filled
    #
    #                     self.openspreaddict[key]['position'][leg] = sum(self.openspreaddict[key]['orderids'][leg].values())
    #             else:
    #

    def spreadTradeR(self):
        #takes in streaming tick data from trades to open. Calculates child orders and manages child orders according
        #to trading rules"""
        if len(self.marketdepth) > 2 and None not in [self.marketdepth[key][subkey][subsubkey] for
                                key in self.marketdepth for subkey  in self.marketdepth[key].keys() for subsubkey in
                                                      self.marketdepth[key][subkey].keys()]:

            for spread in self.openspreaddict:
                for leg in range(2):
                    if leg ==0:
                        ticker1 = spread[:3]
                        side1 = self.openspreaddict[spread]['side']
                        quantity1 = self.openspreaddict[spread]['parentquantity'][0]
                        if side1 == 'buy':
                            mktleg1 = self.marketdepth[ticker1][1]['price']
                            mktleg1size = self.marketdepth[ticker1][1]['size']
                            neartouchleg1 = self.marketdepth[ticker1][0]['price']
                            neartouchleg1size= self.marketdepth[ticker1][0]['size']
                        elif side1 =='sell':
                            mktleg1 = self.marketdepth[ticker1][0]['price']
                            mktleg1size = self.marketdepth[ticker1][0]['size']
                            neartouchleg1 = self.marketdepth[ticker1][1]['price']
                            neartouchleg1size = self.marketdepth[ticker1][1]['size']
                    else:
                        ticker2 = spread[-3:]
                        quantity2 = self.openspreaddict[spread]['parentquantity'][1]
                        if side1 == 'buy':
                            side2 = 'sell'
                        else:
                            side2 = 'buy'
                        if side2 == 'buy':
                            mktleg2 = self.marketdepth[ticker2][1]['price']
                            mktleg2size = self.marketdepth[ticker2][1]['size']
                            neartouchleg2 = self.marketdepth[ticker2][0]['price']
                            neartouchleg2size = self.marketdepth[ticker2][0]['size']
                        elif side2 == 'sell':
                            mktleg2 = self.marketdepth[ticker2][0]['price']
                            mktleg2size = self.marketdepth[ticker2][0]['size']
                            neartouchleg2 = self.marketdepth[ticker2][1]['price']
                            neartouchleg2size = self.marketdepth[ticker2][1]['size']

                if side1 == 'buy':
                    if self.openspreaddict[spread]['limit'] > mktleg1 - mktleg2:
                        nextorder = self.nextOrderId()
                        self.placeOrder(nextorder, ticker1, LimitOrder('buy', quantity1, mktleg1))
                        nextorder = self.nextOrderId()
                        self.placeOrder(nextorder, ticker2, LimitOrder('sell', quantity2, mktleg2))

                    elif self.openspreaddict[spread]['limit'] >= mktleg1 - neartouchleg2 or self.openspreaddict[spread]['limit'] >= neartouchleg1 - mktleg2:

                        if neartouchleg1size >= neartouchleg2size:
                            nextorder = self.nextOrderId()
                            self.placeOrder(nextorder, ticker1,  ('buy', quantity1, neartouchleg1))
                        else:
                            nextorder = self.nextOrderId()
                            self.placeOrder(nextorder, ticker1, LimitOrder('sell', quantity2, neartouchleg2))


                elif side1 == 'sell':
                    if self.openspreaddict[spread]['limit'] < mktleg1 - mktleg2:
                        nextorder = self.nextOrderId()
                        self.placeOrder(nextorder, ticker1, LimitOrder('sell', quantity1, mktleg1))
                        nextorder = self.nextOrderId()
                        self.placeOrder(nextorder, ticker2, LimitOrder('buy', quantity2, mktleg2))






                        # self.openspreaddict[row['ticker']] = {'close': None, 'limit': None, position': [None, None], '
        #                                       orderids': [{}, {}],'side': side, 'stocksclose': [None, None], 'parentquantity': [
        #     None, None]}

    def executionmanager(self,trades):
        self.getdepths(trades)


    def getdepths(self,trades):

        for index, row in trades.iterrows():
            for leg in range(2):
                if leg == 0:
                    ticker = createcontract(row['ticker1'])
                    ticker.secType = 'STK'
                    ticker.exchange = 'ASX'
                    nextorder  = self.nextOrderId()
                    self.depthid[nextorder] = ticker.symbol
                    print('getting depth for ' +str(ticker) + ' ' + str(nextorder))
                    print(self.depthid)
                    self.reqMktDepth(nextorder, ticker, 1, False, [])
                else:
                    ticker = createcontract(row['ticker2'])
                    ticker.secType = 'STK'
                    ticker.exchange = 'ASX'
                    nextorder  = self.nextOrderId()
                    self.depthid[nextorder] = ticker.symbol
                    self.reqMktDepth(nextorder, ticker, 1, False, [])


    def createspreadsdict(self,trades):
        for index, row in trades.iterrows():
            for leg in range(2):
                if leg == 0:
                    side = row['signal']
                    quantity = row['quantity1']
                    self.openspreaddict[row['ticker']] = {'close': None,'limit': None ,'position': [None, None],
                                                          'orderids': [{}, {}], 'side': side,
                                                          'stocksclose': [None, None], 'parentquantity': [None,None]}
                    self.executedict[row['ticker']] = {'side':side,'bid':[],'offer':[],'bidsize':[],'offersize':[],'parent':[],
                           'children':[{},{}]}
                    self.openspreaddict[row['ticker']]['parentquantity'][0][quantity] = None
                    self.openspreaddict[row['ticker']]['limit'] = self.lastclosedict[row['ticker1']] - self.lastclosedict[row['ticker2']]
                    self.openspreaddict[row['ticker']]['close'] = self.lastclosedict[row['ticker1']] - self.lastclosedict[row['ticker2']]
                else:
                    quantity = row['quantity2']
                    self.openspreaddict[row['ticker']]['parentquantity'][1][quantity] = None

    def sendorders(self,trades,spread = False):
        for index,row in trades.iterrows():
            if spread == True:
                self.spreads.pop(self.spreads.index(list([row['ticker'][:3],row['ticker'][-3:]])))
                for leg in range(2):
                    if leg ==0:
                        ticker = createcontract(row['ticker1'])
                        side = row['signal']
                        quantity = row['quantity1']
                        if side == 'sell':
                            limitpx = round(self.lastclosedict[ticker.symbol], 2)
                        else:
                            print(self.lastclosedict[ticker.symbol])
                            limitpx = round(self.lastclosedict[ticker.symbol], 2)
                        nextorder = self.nextOrderId()
                        self.openspreaddict[row['ticker']] = {'close':None,'position':[None,None],'orderids': [{}, {}],
                                                              'sectype': ['CFD','CFD'],'side':side,'stocksclose':[None,None]}
                        self.openspreaddict[row['ticker']]['orderids'][0][nextorder] = None
                        self.placeOrder(nextorder, ticker, LimitOrder(side, quantity, limitpx))
                        print(str(quantity) + ' ' + str(ticker.symbol))

                    else:
                        ticker = createcontract(row['ticker2'])
                        side = row['signal']
                        quantity = row['quantity2']
                        if side == 'buy':
                            side = 'sell'
                        else:
                            side = 'buy'
                            quantity = row['quantity2']
                        if side == 'sell':
                            limitpx = round(self.lastclosedict[ticker.symbol], 2)
                        else:
                            limitpx = round(self.lastclosedict[ticker.symbol], 2)
                        print(str(quantity) + ' ' + str(ticker.symbol))
                        nextorder = self.nextOrderId()
                        self.placeOrder(nextorder, ticker, LimitOrder(side, quantity, limitpx))
                        self.openspreaddict[row['ticker']]['orderids'][1][nextorder] = None
                print('open spreaddict: ' + str(self.openspreaddict))
                self.dumpit(self.openspreaddict,'openspreaddict')
            else:
                if row['sectype'] == 'STK':
                    ticker = createcontractstk(row['ticker'])
                elif row['sectype'] == 'CFD':
                    ticker = createcontract(row['ticker'])
                side = row['signal']
                quantity = row['quantity']
                if side =='sell':
                    limitpx = round(row['close']*0.95,2)
                else:
                    limitpx = round(row['close']*1.05,2)
                if side == 'buy':
                    desc = 'buying'
                elif side == 'sell':
                    desc = 'selling'
                print(desc + ' ' + str(quantity) + ' ' + str(ticker.symbol) )
                self.placeOrder(self.nextOrderId(), ticker,LimitOrder(side,quantity,limitpx))

        self.tradestoopen = pd.DataFrame()


    def throttle(self):
        self.waittime= self.lasttime + 601 - time.time()
        if self.requestcounter%50 ==0 and self.waittime>0:
            print('waiting for ' + str(self.waittime) + ' seconds')
            time.sleep(self.waittime)
        self.lasttime = time.time()

    def error(self, reqId, errorCode, errorString):
        print("Error. Id: " , reqId, " Code: " , errorCode , " Msg: " , errorString)

        if errorCode == 201:
            for ticker in self.openspreaddict:
                if reqId in self.openspreaddict[ticker]['orderids'][0].keys():
                    code = ticker[:3]
                    contract = createcontractstk(code)
                    self.openspreaddict[ticker]['orderids'][0].pop(reqId)
                    nextorder = self.nextOrderId()
                    self.openspreaddict[ticker]['sectype'][0] = 'STK'
                    self.openspreaddict[ticker]['orderids'][0][nextorder] = None
                    side = self.openspreaddict[ticker]['side']
                    quantity = self.openorderdict[code]['quantity']
                    limitpx = self.openorderdict[code]['limitpx']
                    self.placeOrder(nextorder, contract, LimitOrder(side, quantity, limitpx))

                elif reqId in self.openspreaddict[ticker]['orderids'][1].keys():
                    code = ticker[-3:]
                    contract = createcontractstk(code)
                    self.openspreaddict[ticker]['orderids'][1].pop(reqId)
                    nextorder = self.nextOrderId()
                    self.openspreaddict[ticker]['sectype'][1] = 'STK'
                    self.openspreaddict[ticker]['orderids'][1][nextorder] = None
                    if self.openspreaddict[ticker]['side'] =='buy':
                        side = 'sell'
                    else:
                        self.openspreaddict[ticker]['side']=='sell'
                        side = 'buy'

                    quantity = self.openorderdict[code]['quantity']
                    limitpx = self.openorderdict[code]['limitpx']
                    self.placeOrder(nextorder, contract, LimitOrder(side, quantity, limitpx))



            self.openspreaddict

        #self.cancelMktDepth(2002, True)

    # def updateShortable(self):
    #     self.shortabledict = {}
    #     self.shortable = []
    #     for i in range(len(self.contracts)):
    #         self.shortabledict[self.nextOrderId()] = self.contracts[i]
    #         self.reqMktData(list(self.shortabledict.keys())[i], self.contracts[i], "236", False, False, [])

    def tickGeneric(self, reqId: TickerId, tickType: TickType, value: float):
        super().tickGeneric(reqId, tickType, value)
        if tickType == 46:
            self.shortable[self.shortabledict[reqId]] = value


    def start(self):
        self.reqAccountSummary(1000000, "All", AccountSummaryTags.AvailableFunds)
        self.reqPositions()
        if datetime.now().time() < datetime(datetime.now().year, datetime.now().month,datetime.now().day, 9,59).time():
            self.resetpickle()
        for i in range(len(self.contracts)):
            self.shortabledict[i] = self.contracts[i].symbol
            print('sending ' + str(self.contracts[i].symbol))
            self.requestcounter +=1
            self.getprevclose(self.contracts[i])
            self.throttle()
            self.reqRealTimeBars(list(self.tickerdict.keys())[i],self.contracts[i],5,'TRADES',True,[])
            self.reqMktData(list(self.shortabledict.keys())[i], self.contracts[i], "236", False, False, [])

            #self.createConidDict()





app = App()
app.connect(host = "127.0.0.1", port = 7496, clientId=0)
app.run()
