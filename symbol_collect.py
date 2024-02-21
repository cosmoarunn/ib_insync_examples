from collections import OrderedDict
import sys
from ib_insync import *
import itertools
from sqlite_util import create_connection

import time



class Symbols:
    def __init__(self,ib,  alphabets,  combination_length, db_name, tbl_name):
        self.ib = ib
        self.alphabets = alphabets
        self.repeat = combination_length  
        self.db_name = db_name
        self.tbl_name = tbl_name
        self.combinations = []
        self.cds = []
        self.total_rows = 0

        self.contractTypes = OrderedDict ({
            'bonds' : 0, 
            'indices' : 0,
            'stocks' : 0,
            'cryptos': 0,
            'funds' : 0,
            'others' : 0
        })


    def generateCombinationList(self):
        #prepare the symbol alikes.
        for c in itertools.product(self.alphabets, repeat = self.repeat):
            self.combinations.append("".join(c))


    def write_table(self, tbl_name, row_data):
        cnn = create_connection(f'{self.db_name}.db')
        if cnn is not None:
            try:
                c = cnn.cursor()
                rows = []
                
                for rd in row_data:

                    values = []
                    #ToDo: to be made inline 
                    for v in rd.values(): 
                        if isinstance(v, list):
                            values.append(''.join(v))
                        elif isinstance(v, int):
                            v = values.append(str(v))
                        elif isinstance(v, float):
                            v = values.append(str(v))
                        elif v is None:
                            v = values.append(' ')
                        else:
                            values.append(v)
                        
                    #append the row to rows collection    
                    rows.append(values)
                    
                placeholder = "".join(map(lambda x: (x+',') * 21 , "?")) + '?'

                c.executemany(f'REPLACE INTO {tbl_name}  VALUES ({placeholder})', rows)
                    
                cnn.commit()

            except Exception as e:
                print (e)

    def create_table(self, tbl_name):
        
        cnn = create_connection(f'{self.db_name}.db')
        
        if cnn is not None:
            sql = f"""CREATE TABLE IF NOT EXISTS {tbl_name} 
                    (id integer PRIMARY KEY, 
                    secType TEXT,
                    conId integer,  
                    symbol TEXT,
                    lastTradeDateOrContractMonth TEXT,
                    strike REAL,
                    right TEXT,
                    multiplier REAL,
                    exchange TEXT,
                    primaryExchange TEXT,
                    currency TEXT,
                    localSymbol TEXT,
                    tradingClass TEXT,
                    includeExpired TEXT,
                    secIdType TEXT,
                    secId TEXT,
                    description TEXT,
                    issuerId TEXT,
                    comboLegs TEXT,
                    comboLegsDescrip TEXT, 
                    deltaNeutralContract TEXT, 
                    derivativeSecTypes TEXT
                    );"""
                   
            try:
                c = cnn.cursor()
                c.execute(sql)
                
                return True
            except:
                print('\nError creating table. Exiting..\n')
                return None      
            
            cnn.close()
        else:
            print('\ndatabase connection failed. Exiting..\n')
            return None
        

    def _init(self):
        print('Initializing contract search..') 
        pass

    
    def objTypesCount(self, cds):
        #count the type of objects received such as stocks, bonds and indices etc
    
        if len(cds):
            for cd in cds:
                c = OrderedDict(cd.contract.__dict__)
                match c['secType']:
                    case 'BOND':
                        self.contractTypes['bonds'] += 1
                    case 'STK':
                        self.contractTypes['stocks'] += 1
                    case 'IND':
                        self.contractTypes['indices'] += 1
                    case 'CRYPTO':
                        self.contractTypes['cryptos'] += 1
                    case 'FUND':
                        self.contractTypes['funds'] += 1
                    # If an exact match is not confirmed, this last case will be used if provided
                    case _:
                        self.contractTypes['others'] += 1

        else:
            return
        
    def getVerboseTime(self, secs, tt='Elapsed'):
        h = int(secs // 3600) #floor division
        s = round((secs % 3600), 1)
        m = int(secs // 60)
        
        return f"{tt} time : %02d:%02d:%02d" % (h, m, s%60) 

    # Print iterations progress
    def printProgressBar (self, iteration, total, prefix = 'Progress: ', suffix = '', decimals = 1, length = 100, fill = '█', printEnd = "\r", verbose_data=[]):
        """
        Call in a loop to create terminal progress bar
        @params:
            iteration   - Required  : current iteration (Int)
            total       - Required  : total iterations (Int)
            prefix      - Optional  : prefix string (Str)
            suffix      - Optional  : suffix string (Str)
            decimals    - Optional  : positive number of decimals in percent complete (Int)
            length      - Optional  : character length of bar (Int)
            fill        - Optional  : bar fill character (Str)
            printEnd    - Optional  : end character (e.g. "\r", "\r\n") (Str)
        """
        percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
        filledLength = int(length * iteration // total)
        print ("\033[A                             \033[A")
        print ("\033[A                             \033[A")
        print ("\033[A                             \033[A")
        print ("\033[A                             \033[A")
        print ("\033[A                             \033[A")
        

        bar = fill * filledLength + '-' * (length - filledLength)

        cts = self.contractTypes

        #elapsed time
        elapsed = self.getVerboseTime((time.perf_counter() - st) % (24 * 3600))
        #estimated 
        estimated = self.getVerboseTime(((1 - (float(percent) /100)) * (total * 0.5)), 'Estimated')

        print("\rStatus: ")
        print("\r========")
        print(f"\Receiving symbols like  '{verbose_data[0]}' : {verbose_data[1]} objects processed! (Total: {verbose_data[2]}) ")
        print(f"\rStonks: {cts['stocks']}  |  bonds: {cts['stocks']}   |  indices: {cts['indices']}  | cryptos: {cts['cryptos']}  | funds: {cts['funds']}  | others: {cts['others']}")
        print(f"\r{elapsed}  |  {estimated} ")
        print(f'\r{prefix} |{bar}| {percent}% {suffix}', end = printEnd)
        
        # Print New Line on Complete
        if iteration == total+1: 
            print()

    def symbolSearch(self):
        if isinstance(self.combinations, list) and len(self.combinations):
            contract_descriptions = []
            row_id = 0
            #reqMatchingSymbols can be replaced with symbolsamples
            for c in self.combinations:
                #update progress
                self.printProgressBar(self.combinations.index(c), len(self.combinations) -1, verbose_data=[c,len(contract_descriptions), self.total_rows])
                
                if not isinstance(c, str) or c == '': 
                    continue
                time.sleep(0.1) #atleast 1 sec interval between requests as advised by IBApi, feel free to test
                contract_descriptions = self.ib.reqMatchingSymbols(c)
                self.objTypesCount(contract_descriptions)
                
                time.sleep(0.1)
                if len(contract_descriptions):
                    #create / connect table
                    res = self.create_table(self.tbl_name)
                    if res is not None:
                        row_data = []

                        #ToDo: to be modified to inline function
                        for cd in contract_descriptions:
                            c = OrderedDict(cd.contract.__dict__)
                            row_id += 1
                            c['id'] = row_id
                            c.move_to_end('id', False)
                            c['derivativeSecTypes'] = ','.join(cd.derivativeSecTypes) 
                            row_data.append(c)

                        #Buffering for future use but not necessary
                        self.cds = util.df(contract_descriptions)
                        
                        self.write_table(self.tbl_name, row_data=row_data)

                self.total_rows += len(contract_descriptions)

            print(f'\n\nTotal {self.total_rows} contract description objects received and processed!')        

        else: 
            print('No symbol combination to search.. Exiting..')


def parseArguments():
    import argparse
    parser = argparse.ArgumentParser(description='Collect Symbols & Contract Descriptions from IB TWS')
    parser.add_argument('-a', '--alphabets', required=True,
                        help='List of alphanumeric items (alphabets and numbers) for the combination string to search for symbols. \nExample: "a,b,k,2,4"',
                        type=str)
    parser.add_argument('-c', '--combinations', required=True, type=int,
                        help='An integer to specify the length of the combination string to search for.')
    parser.add_argument('-d', '--database', required=False, type=str,
                        help='Name of the database to save results. Caution: any database with that name if exits will be overwritten.')
    parser.add_argument('-t', '--table', required=False, type=str,
                        help='Name of the database table. by default table name is "symbol_contract_descriptions" ')
    parser.add_argument('-hn', '--host', required=False, type=str,
                       help='The host name or ip to connect to. eg localhost or 127.0.0.1 (if not localhost, the external host connection must be enabled on TWS settings.)')
    parser.add_argument('-p', '--port', required=False, type=int,
                       help='The port TWS is listening to. (eg 7436 or 7497). By default its 7437')
    

    return parser.parse_args()
    


if __name__ == '__main__':
    import sys
    st = time.perf_counter()
    
    args = parseArguments()
    alphanumeric = [item for item in args.alphabets.split(',')]
    combi_len = args.combinations
    database = args.database if isinstance(args.database, str)  else 'symbols'
    table = args.table if isinstance(args.table, str)  else 'symbol_contract_descriptions'
    port = args.port if isinstance(args.port, int)  else 7497
    host = args.host if isinstance(args.host, str)  else '127.0.0.1'
    


    try:
        print(f'\nconnecting to ib TWS on {host}:{port}..')
        ib = IB()
        ib.connect(host, port, clientId=426)
        time.sleep(2)
        print('Connection success!\n')
        time.sleep(1)
        print('\n\n\n\n')
    except:
        print('Quitting because could not connect to IB Trader Workstation. Is the client already running?')
        sys.exit(1)



    symbols = Symbols(ib, alphabets=alphanumeric, combination_length=combi_len, db_name=database, tbl_name=table)
    symbols.generateCombinationList()
    symbols.symbolSearch()

    et = time.perf_counter()

    print('All done! \nDisconnecting from IB Trader Workstation..')
    ib.disconnect()

    print(f'Script executed in {round(et-st, 2)} seconds!')
