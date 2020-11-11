import json
import re
import time
import pandas as pd
import tweepy  # Has 7 days old limitation
from pandas.io.json import json_normalize
from config import config
from modules.timeutils import *
from config import access_token_secret, access_token, consumer_secret, consumer_key

class OurTweepy(object):
    '''
    Our Twitter API class.
    '''

    def __init__(self, verbose=True):
        '''
        Class constructor or initialization method.
        '''
        # keys and tokens from the Twitter Dev Console
        # consumer_key = 'p8lA0TpForYSBnbLCTNLpFMn3'
        # consumer_secret = 'weBxKVnLcTdanGxrzDQ2kF6WTkaIIHL43OwsaNQfmMkOgpEbE9'
        # access_token = '1208323412042698753-LGQw8VUOK1Jm3dORVW8HCugSVdgYeb'
        # access_token_secret = 'ce6QXKBRxjqgLmxhRgVvzbgS79mdvuIdgoylQBj8rTyVq'
        self.verbose = verbose
        self.tweepy_date_time_format = config['tweepy']['general']['datetime_format']
        self.tweepy_time_zone = config['tweepy']['general']['timezone']
        # attempt authentication
        try:
            # create OAuthHandler object
            self.auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
            # set access token and secret
            self.auth.set_access_token(access_token, access_token_secret)
            # create tweepy API object to fetch tweets
            self.api = tweepy.API(self.auth)
            if self.verbose:
                print("[OurTweepy] Twitter Client succsufuly created a connection with twitter API (tweepy)")
        except:
            print("Error: Authentication Failed")

    # get functions
    def get_n_last_tweets(self, query, n=100, since_id=None, df=False):
        '''
        Fetch tweets using Tweepy
        Docs: https://developer.twitter.com/en/docs/twitter-api/v1/tweets/search/api-reference/get-search-tweets
        '''
        try:
            # call twitter api to fetch tweets
            if since_id:
                fetched_tweets = self.api.search(q=query, count=n, result_type="recent", since_id=since_id, tweet_mode='extended')
            else:
                fetched_tweets = self.api.search(q=query, count=n, result_type="recent", tweet_mode='extended')
            time.sleep(0.5)
            if fetched_tweets is None:
                return fetched_tweets, 0

            if self.verbose:
                print("[OurTweepy] \tFetched {} tweets of {}".format(len(fetched_tweets), query))

            if df:
                return self.search_results_to_df(fetched_tweets), len(fetched_tweets)
            return fetched_tweets, len(fetched_tweets)
        except tweepy.TweepError as e:
            # print error (if any)
            print("Error : " + str(e))
            return None, 0

    def get_last_tweet(self, query):
        search_results, _ = self.get_n_last_tweets(query=query, n=1)
        if search_results is not None:
            if len(search_results) > 0:
                return search_results[0]
        return None

    def get_last_tweet_id(self, query):
        last_tweet, _ = self.get_n_last_tweets(query=query, n=1)
        if last_tweet is not None:
            return last_tweet.id
        return 0

    def search_results_to_df(self, sr):
        df = json_normalize(self.search_results_to_list(sr))
        if 'created_at' in df.columns:
            df['created_at'] = pd.to_datetime(df['created_at'],
                                             format=self.tweepy_date_time_format)  # TODO legacy -> remove
            created_at = pd.DatetimeIndex(  pd.to_datetime(df['created_at'], format=self.tweepy_date_time_format),
                                            name=config['tweepy']['our_api']['datetime_index_name']).tz_localize(self.tweepy_time_zone)
            df = df.set_index(created_at).\
                    drop(['created_at'], axis=1).\
                    sort_index(ascending=False)
            df['fetched_at'] = now()
        return df

    def search_results_to_list(self, sr):
        return [self.jsonify_tweepy(tweet) for tweet in list(sr)]

    def jsonify_tweepy(self, tweepy_object):
        json_str = json.dumps(tweepy_object._json)
        return json.loads(json_str)

    def clean_tweet(self, tweet):
        '''
        Utility function to clean tweet text by removing links, special characters
        using simple regex statements.
        '''
        return ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t]) | (\w+:\ / \ / \S+)", " ", tweet).split())
