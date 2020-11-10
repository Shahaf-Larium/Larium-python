'''
The pulse module is running the twitter monitor which pulling tweets from twitter and handles the api to the outside world.
Use 'talk_to_me' to get an answer from the module contains the following:
    Per stock:
        daily_interset  [int]           : today total interest volume.
        last_interset   [int]           : last interest in 30 min interval.
        current_change  [float]         : the intereset change in the last 10 minutes in compare to last 60 minutes.
        is_event        [bool]          : wheter we detected event or not (if true expect to get events_data with value)
        events_data     [dataframe]     : list of events. each event is a dictionary with two keys
                                        {'alerted_at', 'created_at', 'text', 'user_name', 'retweet_count', 'like_count'}
    fetched_at      [date_time (utc)]   : what time you asked for the data.
'''
from pathlib import Path
import math
import TickerFlask.modules.pdutils as pdutils
from TickerFlask.modules.timeutils import day_format, today, tprint, now, floor_dt
from config import config, root_path
from TickerFlask.products.event_detector.volume_analyzer import Analyzer
from TickerFlask import products as bottom_line
from TickerFlask.data.loader import FileManager
from time import sleep
import traceback
from TickerFlask.modules.our_tweepy import OurTweepy
from TickerFlask.modules.DataManager import DataManager


class Pulse:
    def __init__(self, verbose=False):
        self.stocks_list = config['stocks']  # TODO strange bug with date_time?!
        self.twitter_api = OurTweepy(verbose=verbose)
        self.refresh_rate = math.ceil((60 * config['tweepy']['limitations']['interval'] / (
                    config['tweepy']['limitations']['max_requests_per_interval'] / len(self.stocks_list))))
        self.stocks_dir = root_path / Path(config['databases']['tweets']['stocks'])
        self.files_manager = FileManager(database_folder=self.stocks_dir, verbose=verbose)
        self.data_manager = DataManager()
        self.data = self.__initdata__()
        self.stop = False
        self.is_live = False
        self.save_every_iter = 2
        print("[Pulse] \tInitialized Pulse with {} stocks".format(len(self.stocks_list)))

    def __initdata__(self):
        '''
        Load tweets data into self.data
        TODO Support 'fill the gap' mode which fills all the gaps in today pulled tweets
        '''
        data = {}
        for stock in self.stocks_list:
            stock_tweets_df = self.files_manager.load(stock=stock,
                                                      dates=[day_format(today())])  # load tweets from today (utc).
            if stock_tweets_df is None:
                stock_tweets_df, len = self.twitter_api.get_n_last_tweets(query='$' + stock, n=5, df=True)
            data[stock] = stock_tweets_df
        return data

    def run(self):
        iter = 0
        self.is_live = True
        while not self.stop:
            tprint()
            print("[Pulse] \tFetching tweets: ")
            self.update()
            print("[Pulse] \tWriting to Database: ")
            try:
                self.data_manager.write_to_db(self.talk_to_me())
            except Exception as e:
                print("[Pulse] \tWrite to DB failed.")
                track = traceback.format_exc()
                print(track)

            if iter % self.save_every_iter == 0:
                print("[Pulse] \tSaving tweets: ")
                self.save()
            iter += 1
            print("[Pulse] \tSleeps for {} seconds".format(self.refresh_rate))
            try:
                sleep(self.refresh_rate)
            except KeyboardInterrupt:
                print("[Pulse] \tQuit run()")
                return
            print()
        self.save()
        self.is_live = False
        self.stop = False

    def save(self):
        self.files_manager.save(data=self.data, dates=[day_format(today())])

    def update(self):
        # TODO Error : [{'message': 'Rate limit exceeded', 'code': 88}] handle this one.
        for stock, df in self.data.items():
            if df.empty:
                continue
            try:
                latest_id = df['id'].iloc[0]
                stock_tweets_df, len = self.twitter_api.get_n_last_tweets(query='$' + stock, since_id=latest_id,
                                                                          df=True)
                if len > 0 and stock_tweets_df is not None:
                    self.data[stock] = pdutils.append(to=df, append_me=stock_tweets_df)
            except Exception as e:
                track = traceback.format_exc()
                print(track)
                continue

    def talk_to_me(self):
        '''
        :return: answer (see format in header)
        '''
        answer = dict.fromkeys(self.data.keys())
        for stock, df in self.data.items():
            answer[stock] = {'stock': stock, 'is_event': False, 'events_data': None, 'daily_interset': 0,
                             'last_interest': 0, 'current_change': 0}
            if not df.empty:
                analyzer = Analyzer(ts=df.index.to_series(keep_tz=True))
                answer[stock]['daily_interset'] = analyzer.get_interest(in_last='1d')
                answer[stock]['last_interest'] = analyzer.get_interest(in_last='30min')
                answer[stock]['current_change'] = analyzer.get_change()
                if analyzer.is_event_detected(since=floor_dt(dt=now(), on='minutes', to=30)):
                    answer[stock]['is_event'] = True
                    answer[stock]['events_data'] = bottom_line.get_top_tweets(df_tweets=df)
        answer['fetched_at'] = now().strftime("%m/%d/%Y, %H:%M:%S")
        return answer
