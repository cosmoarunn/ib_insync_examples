
import time
from random import randrange
from ib_insync import *
from sqlite_util import *

class TickerData:
    "get historical ticker data for the supplied ticker"
    def __init__(self, ib, ticker, hist_interval='1 hour', hist_duration = '2 D', database='ib_market_data'):
        self.ib = ib
        print(ticker)
        self.ticker = ticker.strip()
        self.bars =  self.contracts =  self.df = []
        self.history_interval = hist_interval
        self.history_duration = hist_duration
        self.sqlite_db = database
        self.contracts = []
        self.exchange='ISLAND'  #if failed look in SMART 

    def start(self):
        print("starting..")
        self.qualifyContract()
        self.reqHistoricalData() #contract.exchange, self.history_interval)
        if len(self.bars):
            print(self.bars)
            self.export_sql_table(self.ticker,self.ticker + "_" + self.history_interval.strip() + "_" + self.history_duration.strip())
            return self.bars
        else:
            print("\nNo bars to export to sql..")
            return []
            

    def qualifyContract(self):
        self.contracts=[Stock(self.ticker, 'ISLAND', 'USD') ]
        self.ib.qualifyContracts(*self.contracts)
        
     
    def reqHistoricalData(self):
        if(self.contracts[0]):
            stock = self.contracts[0]
            bars =  self.ib.reqHistoricalData(
                stock,
                endDateTime='',
                durationStr = self.history_duration,
                barSizeSetting=self.history_interval,
                whatToShow='TRADES',
                useRTH=True,
                formatDate=1
                )
            
        elif stock.exchange  == self.exchange:
            print(f'No historical data received for {self.ticker} on {self.exchange}. Trying SMART..')
            bars =  self.ib.reqHistoricalData(
                Stock(self.ticker,'SMART', 'USD'),
                endDateTime='',
                durationStr = self.history_duration,
                barSizeSetting=self.history_interval,
                whatToShow='TRADES',
                useRTH=True,
                formatDate=1)
        else:
            print(f'No historical data received for {self.ticker} on SMART routing either. Giving up..')

        if len(bars):
                self.bars = util.df(bars)

    def export_sql_table(self, db_name, tbl_name):
        import pandas as pd
        print("\nWriting SQL Table definition..")
        #query = ""
        #res = createTables(db_name, tbl_name, query)
        conn = create_connection("{}_{}".format(db_name,randrange(1100,9999)))
        return self.bars.to_sql(tbl_name, conn)

'''
# USAGE


if __name__ == '__main__':
    import sys
    from utils.serverutils import ib_client_address

    host,port = ib_client_address

    ib = IB()
    ib.connect(host, port, clientId=randrange(100,200))
    time.sleep(2)
    print('Connection success!\n')
    time.sleep(1)

    print('\nReceiving datas..\n')
    #duration =  "{} S".format(8 * 3600)   #calculate duration from diff inputs 
    ticker_data = TickerData(ib, "AAPL", hist_interval= "5 secs", hist_duration="2 D")
    print(ticker_data.bars)

    try:

        pass
    except :
        print('Quitting because could not connect to IB Trader Workstation. Is the client already running?')
        sys.exit(1)


    '''
