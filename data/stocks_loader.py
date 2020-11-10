import yfinance as yf
import pandas as pd

class StocksLoader(object):
    '''
    DataLoader class handles data load of stock market data.
    1. stock market data - based on yfinanace -> TODO move to IEX
    '''

    def __init__(self, verbose=True):
        '''
        Class constructor or initialization method.
        '''
        self.verbose = verbose

    #   methods for loading tweets from our database

    def get_stock_data(self,symbol, since, until, interval):
        print("Start downloading stock ({}) data between {} to {}".format(symbol, since, until))
        stock = yf.Ticker(symbol)
        hist = stock.history(start=since, end=until, interval=interval, prepost=True)
        hist.index = pd.DatetimeIndex([d.replace(second=0, microsecond=0).tz_localize(None) for d in list(hist.index)])
        return hist

    def download_stock_data(self,symbol, since, until, interval):
        pass
