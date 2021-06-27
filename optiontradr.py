from ibapi.wrapper import EWrapper
from ibapi.utils import *
from ibapi.client import EClient
from ibapi.contract import Contract, ContractDetails
from ibapi.order_state import OrderState
from ibapi.common import BarData
from ibapi.order import Order
import pickle
import pytz
from ibapi.common import * # @UnusedWildImport
from mycontracts import createContractObject, LimitOrder,createcontract, createSpreadContract,ComboLimitOrder,createcontractstk
import pandas as pd
import connectdb as db
from datetime import datetime, timedelta
pd.options.mode.chained_assignment = None
from ibapi.account_summary_tags import *
from ibapi.ticktype import *
import alphamodel as al
from ibapi.samples.Python.Testbed.AvailableAlgoParams import AvailableAlgoParams
from ibapi.samples.Python.Testbed.OrderSamples import OrderSamples
from ibapi.tag_value import TagValue
import csv
import numpy as np
from dateutil import tz
TickerId = int
import time
import mibian
"""
BS([underlyingPrice, strikePrice, interestRate, daysToExpiration], volatility=x, callPrice=y, putPrice=z)
"""
c = mibian.BS([1.4565,1.45,1,2,30],volatility=20)
c.callPrice

class App(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, wrapper=self)
        self.getavailablecodes = True
        self.contract = Contract()
        self.contract.symbol = "RPM"
        #self.contract.localSymbol = "RPMO"
        self.contract.secType = "WAR"
        self.contract.exchange = "ASX"
        self.contract.currency = "AUD"
        # self.contract.lastTradeDateOrContractMonth = "20210826"
        # self.contract.strike = 0.25
        # self.contract.right = "C"
        # self.contract.multiplier = "1"
        self.options = {}
        self.availableoptionsdf = pd.DataFrame(columns = ['underlying','exdate','exprice','multiplier','type'])
        self.requestcounter = 0
        self.lasttime = time.time()
        #read in all asx codes to find which have warrants (where self.getavailablecodes ==True)
        with open('allasx.csv',newline='')as f:
            reader = csv.reader(f)
            self.allcodes = list(reader)
            print(self.allcodes)


        #read in all saved warrants
        if self.getavailablecodes == False:
            with open('options.csv',newline='')as f:
                reader = csv.reader(f)
                self.availablecodes = list(reader)

            #create dictionary
            self.availablecodesdict = {}
            for i in self.availablecodes:
                self.availablecodesdict[i[0]] = i[1:]

        #format dictionary
            for i in self.availablecodesdict:
                self.availablecodesdict[i][1] =  int(self.availablecodesdict[i][1])
                self.availablecodesdict[i][2] = datetime.strftime(self.availablecodesdict[i][2],'%d%m%Y')
                self.availablecodesdict[i][4] = int(self.availablecodesdict[i][4])
                self.availablecodesdict[i].append('')
                self.availablecodesdict[i].append('')
                self.availablecodesdict[i].append('')
                self.availablecodesdict[i].append('')


    def contractDetails(self, reqId: int, contractDetails: ContractDetails):
        super().contractDetails(reqId, contractDetails)
        print(contractDetails)
        detailslist = str(contractDetails).split(",")
        print(detailslist)
        self.options[detailslist[10].split('.')[0]] = {'underlying':detailslist[1],'exdate':detailslist[3],'exprice':detailslist[4],'multiplier':detailslist[6],
                                        'type':detailslist[5]}

        print(self.options)
        print(pd.DataFrame(self.options).T)


    def contractDetailsEnd(self, reqId: int):
        super().contractDetailsEnd(reqId)
        print("ContractDetailsEnd. ReqId:", reqId)
        pd.DataFrame(self.options).T.to_csv('allwarrants.csv')


    def realtimeBar(self, reqId:TickerId, time:int, open_: float, high: float, low: float, close: float,
                        volume: int, wap: float, count: int):

        print(close)


    def nextValidId(self, orderId:int):
        self.nextValidOrderId = orderId
        self.start()

    def nextOrderId(self):
        oid = self.nextValidOrderId
        self.nextValidOrderId += 1
        return oid

    def throttle(self):
        self.waittime= self.lasttime + 601 - time.time()
        if self.requestcounter%50 ==0 and self.waittime>0:
            print('waiting for ' + str(self.waittime) + ' seconds')
            time.sleep(self.waittime)
        self.lasttime = time.time()

    def start(self):

        if self.getavailablecodes == True:
            for code in self.allcodes:
                #self.contract.symbol = code[0]
                #self.contract.symbol = 'RPM'
                self.contract.symbol = code[0]
                print(self.contract)
                #self.reqRealTimeBars(self.nextOrderId(), self.contract, 5, 'ASK', True, [])
                #self.reqMatchingSymbols(215, "RPM")
                self.reqContractDetails(self.nextOrderId(), self.contract)
                self.requestcounter += 1
                self.throttle()
                #self.reqSecDefOptParams(self.nextOrderId(), "RPM", "", "STK", 374163828)
                #self.reqSecDefOptParams(self.nextOrderId(), "BHP", "", "STK", 4036812)

        else:
            for key in self.availablecodesdict:
                print('u')



app = App()
app.connect(host = "127.0.0.1", port = 7496, clientId=0)
app.run()