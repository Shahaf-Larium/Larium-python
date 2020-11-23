import pandas as pd


def append(to, append_me):
    df = to.append(append_me, sort=False)
    df = df.sort_index(ascending=False)
    df = df[~df.index.duplicated(keep='first')]
    return df


def read_time_series(file_path, datetimeindex):
    """
    Load a time series data
    :param file_path: csv file path that includes datetime index!
    :param datetimeindex: the datetime index format
    :example:
         read_csv('data/tweets/AAPL/11-11-20.csv','%a %b %d %H:%M:%S +0000 %Y')
    :return: Dataframe
    """
    return pd.read_csv(str(file_path), index_col=datetimeindex, parse_dates=True)
