from data.tweets_loader import TweetsLoader
import modules.utils as utils
import modules.timeutils as timeutils
import modules.pdutils as pdutils
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
from config import config


class FileManager(TweetsLoader):
    '''
    This loader is for Tweepy created database.
    '''
    def __init__(self, database_folder, verbose=False):
        super().__init__(verbose=verbose)
        self.database_folder = Path(database_folder)

    def __load__(self, stock, dates=None, datetimeindex=config['tweepy']['our_api']['datetime_index_name']):
        load_files = []
        base_folder = self.database_folder / stock
        utils.create_dir_if_not_exist(base_folder)
        load_files = utils.get_files(folder=str(base_folder))
        if dates:
            load_files = utils.filter_files(load_files, filters=dates)
        data = None
        for file_name in load_files:
            file_path = base_folder / file_name
            if file_path.is_file():
                df = pd.read_csv(str(file_path), index_col=datetimeindex, parse_dates=True)
                if data is None:
                    data = df
                else:
                    data = data.append(df, sort=False)
                    data = data.sort_index(ascending=False)
        return data

    def load(self, stock, dates=None, datetimeindex=config['tweepy']['our_api']['datetime_index_name']):
        data = self.__load__(stock, dates, datetimeindex)
        if data is not None and self.verbose:
            print("[FileManager] Loaded {} tweets of {}".format(len(data.index), stock))
        return data

    def save(self, data, dates, append=True):
        for stock, df in data.items():
            if df.empty:
                continue
            for date in dates:
                file_path = self.choose_file(stock, date)
                utils.create_dir_if_not_exist(file_path.parent)
                df_to_save = df.loc[date]
                if append and file_path.is_file():
                    already_exist_df = self.__load__(stock=stock, dates=[date])
                    df_to_save = pdutils.append(to=already_exist_df, append_me=df_to_save)
                df_to_save.to_csv(str(file_path))

            if self.verbose:
                print("[FileManager] Saved {} tweets of {}".format(len(df_to_save.index), stock))

    def choose_file(self, stock, date, suffix='.csv'):
        base_folder = self.database_folder / stock
        if type(date) is str:
            file_name = str(date + suffix)
        else:
            file_name = str(timeutils.day_format(date) + suffix)
        return base_folder / file_name

    def file_name(self, start, end):
        return 'on_' + datetime.strftime(start, '%Y-%m-%d') + '_from_' + datetime.strftime(start, '%H-%M-%S') +\
                '_to_' + datetime.strftime(end, '%H-%M-%S') + '.csv'
