import math
import time
from datetime import datetime, timedelta
# from os import path as Path
from pathlib import Path
from data.loader import FileManager
import pandas as pd
import plotly.graph_objs as go
import plotly.io as pio
from plotly import subplots
import modules as timeutils
import modules.utils as utils
from config import config, root_path
from modules.our_tweepy import OurTweepy


class TwitterMonitor(OurTweepy):

    def __init__(self, stocks_list, continue_same_day=False, init_tweets_num=5, verbose=False):
        super().__init__(verbose=verbose)
        self.stocks_list = stocks_list
        self.tweets_data = dict.fromkeys(self.stocks_list)
        self.stats_df = pd.DataFrame(columns=(self.stocks_list.append('date_time')))
        self.tweets_path = root_path / Path(config['databases']['tweets']['stocks'])
        self.logs_path = root_path / Path(config['logs']['monitor']['stats'])
        self.update_interval = math.ceil((60 * config['tweepy']['limitations']['interval'] / (
                    config['tweepy']['limitations']['max_requests_per_interval'] / len(stocks_list))) * 1.1)
        self.last_time_saved = datetime.now()
        self.init_tweets_num = init_tweets_num
        self.continue_same_day = continue_same_day
        self.filemanager = FileManager(database_folder=self.tweets_path, verbose=True)
        self.init_data()

    def init_data(self):
        print("Initializing TwitterMonitor():")
        for stock, fetched_tweets in self.tweets_data.items():
            if self.continue_same_day:
                today = utils.day_format(datetime.now())
                self.tweets_data[stock] = self.filemanager.load(stock=stock, dates=today)
            else:
                search_query = str("$" + stock)
                self.tweets_data[stock], _ = self.fetch(query=search_query, n=self.init_tweets_num)

    def fetch(self, query, n=100, since_id=None):
        print("\tSearching for new tweets with " + query + " cashtag...")
        if not since_id:
            latest_tweet, len = self.get_n_last_tweets(query=query, n=n, df=True)
        else:
            latest_tweet, len = self.get_n_last_tweets(query=query, n=n, since_id=since_id, df=True)
        print("\t\tAdded {} new tweets.".format(len))
        return latest_tweet, len

    def run(self, hours, save_every=60):
        init_amount_of_tweets = self.stats().sum().sum()
        end_time = datetime.now() + timedelta(seconds=(hours * 60 * 60))
        while end_time > datetime.now() + timedelta(seconds=self.update_interval):
            try:
                self.update()
                if datetime.now().hour is not self.last_time_saved.hour:
                    self.save()  # saves every hour
                print("[{}]\nInterval between updates: {} seconds\nExpected end time: {}\nForce stop: Ctrl+C".
                      format(datetime.strftime(datetime.now(), format="%H:%M:%S"),
                             self.update_interval,
                             datetime.strftime(end_time, format="%H:%M:%S")))
                time.sleep(self.update_interval)
            except KeyboardInterrupt:
                print("run: killed by user.")
                return
            except Exception as e:
                print("Error occured ({}) in the run loop - trying to sleep for 15 min to avoid API ban.".format(e))
                time.sleep(15 * 60)
        # exit while loop
        total_tweets_pulled = self.stats().sum().sum() - init_amount_of_tweets
        print("[{}] Time is up. Pulled {} tweets.".format(datetime.strftime(datetime.now(), format="%H:%M:%S"),
                                                          total_tweets_pulled))

    def update(self):
        # TODO Error : [{'message': 'Rate limit exceeded', 'code': 88}] handle this one.
        stocks_volume_absolute_change = dict.fromkeys(self.stocks_list)
        for stock, fetched_tweets in self.tweets_data.items():
            try:
                last_fetched_tweet_id = fetched_tweets['id'].iloc[0]
                search_query = str("$" + stock)
                new_tweets_df, n_tweets = self.fetch(query=search_query,
                                                     since_id=last_fetched_tweet_id)  # fetch all tweets since the last fetched tweet (max limit is 100)

                if n_tweets > 0 and new_tweets_df is not None:
                    self.tweets_data[stock] = pd.concat([fetched_tweets, new_tweets_df], sort=False). \
                        sort_values(by=['id'], ascending=False)
                stocks_volume_absolute_change[stock] = n_tweets
            except:
                print("Something went wrong fetching {} new tweets.".format(stock))
                continue
        self.__update_stats(stocks_volume_absolute_change)

    def show(self):
        for stock, fetched_tweets in self.tweets_data.items():
            print(stock)
            for index, row in fetched_tweets.iterrows():
                print("\t", end="")
                self.__print_tweet(row)
            print("Total tweets: {}".format(len(fetched_tweets.index)))
            print()

    def data(self):
        return self.tweets_data

    def stats(self):
        if self.stats_df.empty:
            return self.stats_df
        return self.stats_df
        # return self.stats_df.set_index(pd.DatetimeIndex(self.stats_df.date_time))#.drop('date_time')

    def plot_stats(self):
        pio.renderers.default = "browser"  # for default plot in browser
        df = self.stats()
        how_many_plots = len(df.columns)
        rows = int(math.ceil(how_many_plots / 3))
        cols = 3
        fig = subplots.make_subplots(rows=rows, cols=cols, subplot_titles=df.columns,
                                     shared_xaxes=True, vertical_spacing=0.01)
        j = 0
        for i in df.columns:
            row = j % rows + 1
            col = j % cols + 1
            fig.add_trace(
                go.Scatter(
                    {'x': df.index,
                     'y': df[i]}),
                row=row, col=col)
            j += 1
        fig.update_layout(title="Twitter Monitor", title_x=0.5, showlegend=False)
        fig.show()

    def save(self, force_all=False):
        self.save_stats(force_all=force_all)
        self.save_tweets(force_all=force_all)
        self.last_time_saved = datetime.now()

    def save_stats(self, force_all=False):
        df = self.stats()
        if not force_all:
            df = df[df.index > self.last_time_saved]  # save only the stats created from the last time saved.
        if df.empty: return  # if no new stats, skip.
        count = len(df.index)
        first_time = df.index[0]
        last_time = df.index[count - 1]
        file_name = 'on_' + datetime.strftime(first_time, '%Y-%m-%d') + '_from_' + \
                    datetime.strftime(first_time, '%H-%M-%S') + '_to_' + \
                    datetime.strftime(last_time, '%H-%M-%S') + '.csv'
        df.to_csv(str( self.logs_path / file_name))
        print("saved to file: " + str( self.logs_path / file_name))

    def save_tweets(self, force_all=False):
        self.filemanager.save(data=self.tweets_data, dates=[timeutils.now()])
        # for stock, fetched_tweets in self.tweets_data.items():
        #     full_folder_path = self.tweets_path / stock
        #     utils.create_dir_if_not_exist(str(full_folder_path))
        #     if not force_all:
        #         last_fetched_tweets = fetched_tweets[fetched_tweets['fetched_at'] > self.last_time_saved]
        #     else:
        #         last_fetched_tweets = fetched_tweets
        #     if last_fetched_tweets.empty: continue  # if no tweets, skip.
        #
        #     count = len(last_fetched_tweets.index)
        #     last_time = last_fetched_tweets['date_time'].iloc[0]
        #     first_time = last_fetched_tweets['date_time'].iloc[count - 1]
        #     file_name = 'on_' + datetime.strftime(first_time, '%Y-%m-%d') + '_from_' + datetime.strftime(first_time,
        #                                                                                                  '%H-%M-%S') + '_to_' + datetime.strftime(
        #         last_time, '%H-%M-%S') + '.csv'
        #     last_fetched_tweets.to_csv(str(full_folder_path / file_name))
        #     print("saved to file: " + str(full_folder_path / file_name))

    def __update_stats(self, stats_dict):
        stats_dict['date_time'] = datetime.now().replace(microsecond=0)
        stats_df = pd.DataFrame([stats_dict]).fillna(0)
        stats_df.index = pd.DatetimeIndex(stats_df.date_time)
        stats_df = stats_df.drop(['date_time'], axis=1)
        self.stats_df = pd.concat([self.stats_df, stats_df], sort=False)

    def __print_tweet(self, tweet):
        print("id: {} created_at: {}".format(tweet['id'], tweet['date_time']))
