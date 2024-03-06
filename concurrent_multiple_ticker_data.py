import asyncio
from random import randrange
from ib_insync import *
import nest_asyncio
from ticker_data import TickerData

ib = IB()

async def getTickerHistory(ticker, interval="5 Secs", duration = "1 D", database="multiple_5S_1D"):
    if ticker:
        td = TickerData(ib, ticker, hist_interval= interval, hist_duration=duration, database=database)
        return await td.start()
    else:
        print("\nAtleast one ticker or symbol is needed to fetch history..")


async def tickerHistory(ticker):
     return await getTickerHistory(ticker)


async def concurrent_multiple_tickers_get_history(tickers): 
    async with asyncio.Semaphore(20):
        return await asyncio.gather(
                        *(tickerHistory(ticker) for ticker in tickers))


async def main(tickers:list) -> None:
    "Main async coroutine gather all tasks needed to run"
    return await asyncio.gather( concurrent_multiple_tickers_get_history(tickers) )


if __name__ == "__main__":
    nest_asyncio.apply()

    ib.connect('localhost', 7497, clientId=randrange(100,200))
    ib.sleep(2)
    print('Connection success!\n')
    ib.sleep(1)

    tickers = ["AAPL", "TSLA", "PR", "AMD", "NVDA"]
    
    #getTickerHistory("AAPL")  #for single ticker
    asyncio.run(concurrent_multiple_tickers_get_history(tickers))
