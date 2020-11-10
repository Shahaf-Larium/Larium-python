from TickerFlask.modules.utils import *
import pandas as pd
from os import path as Path
from os import mkdir
import GetOldTweets3 as got


class TweetsLoader(object):
    '''
    DataLoader class handles local tweets database (This loader is for GetOldTweets3 created database.)
    1. Load from local database.
    2. Update local database - using GetOldTweets3. # nlimited tweets database older than a week.
    '''

    def __init__(self, verbose=True, date_time_col="date_time"):
        '''
        Class constructor or initialization method.
        '''
        self.verbose = verbose
        self.date_time_col = date_time_col

    #   methods for loading tweets from our database

    def load_tweets_from_file(self, file_path):
        '''
        Load tweets data from a given .csv file.
        Uses the 'date_time' column from the csv as a Datetime index for dataframe.
        Convert the time_zone to 'America/New_York' and remove it from the Datetime representation.
        :param file_path: path to csv file contains tweets data loaded using GetOldTweets3
        :return: dataframe with the data from the csv file
        '''
        df = pd.read_csv(file_path)
        if self.verbose:
            print("Tweets data loaded from {}.".format(file_path))
            print("Database columns are: {} \nWith {} rows.".format(str(list(df.columns)), df.count()[0] ))
        df.date_time = pd.DataFrame(pd.to_datetime(df[self.date_time_col], format='%Y-%m-%d %H:%M:%S%z'))
        df.date_time = pd.DataFrame(pd.to_datetime(df[self.date_time_col], format='%Y-%m-%d %H:%M:%S'))
        df.date_time = [d.replace(second=0, microsecond=0).tz_convert("America/New_York").tz_localize(None) for d in list(df.date_time)]
        df = df.set_index(pd.DatetimeIndex(df.date_time))
        return df

    def load_all_tweets_from_folder(self, data_folder):
        '''
        find all .csv files in the given folder and loades them into a dataframe.
        :param dir: folder path
        :return: dataframe
        '''
        csv_files_in_folder = get_files(data_folder)
        tweets_data = None
        for csv_file_name in csv_files_in_folder:
            csv_file_path = Path.join(data_folder, csv_file_name)
            df = self.load_tweets_from_file(file_path=csv_file_path)
            if tweets_data is None:
                tweets_data = df
            else:
                tweets_data = pd.concat([tweets_data, df])
        return tweets_data

    def load_tweets_in_dates_range(self, data_folder, since, until):
        all_tweets = self.load_all_tweets_from_folder(data_folder=data_folder)
        return all_tweets[(all_tweets['date_time'] >= since) & (all_tweets['date_time'] <= until)]

    #   methods for download tweets using GetOldTweets3 API

    def get_old_tweets(self, query, since, until, max_count=None):
        '''
        :param query: search query
        :param since: from which date
        :param until: to which date (not included)
        :param max_count: [optional] limit the number of results.
        :return: dataframe
        load all tweets include the query between since and until dates range (until is not included).
        '''
        tweetCriteria = got.manager.TweetCriteria().setQuerySearch(query).setSince(since).setUntil(until)
        if max_count is not None:
            tweetCriteria = got.manager.TweetCriteria().setQuerySearch(query) \
                .setSince(since) \
                .setUntil(until) \
                .setMaxTweets(max_count)
        print("Start searching for all tweets with '{}' from '{}' to '{}'... may take a little while.".format(query, since, until))
        tweets = got.manager.TweetManager.getTweets(tweetCriteria)
        print("Search is done.")
        return tweets

    def add_tweets_to_database(self, output_folder, query, since, until):
        '''

                :param output_folder:
                :param query:
                :param since:
                :param until:
                :param max_count:
                :return:
                save all tweets include the query between since and until dates range (until is not included) to the output_folder.
                split the result into .csv file per month.
        '''
        periods = split_to_date_range_to_months_range(since, until)
        if not Path.exists(output_folder):
            mkdir(output_folder)
        for p in periods:
            from_date, to_date = p[0], p[1]
            tweets = self.get_old_tweets(query, str(from_date.date()), str(to_date.date()))
            lst = [[tw.username,
                    tw.text,
                    tw.date,
                    tw.retweets,
                    tw.mentions,
                    tw.hashtags] for tw in tweets]
            df_result = pd.DataFrame(lst,
                                     columns=['user', 'text', 'date_time', 'retweets', 'mentions', 'hashtags'])
            file_name = str(from_date.date())+'_'+str(to_date.date())+'.csv'
            full_path = Path.join(output_folder, file_name)
            df_result.to_csv(full_path)
            print("Tweets downloaded and saved to: {}".format(full_path))



