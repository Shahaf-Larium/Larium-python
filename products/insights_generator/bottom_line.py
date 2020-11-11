from datetime import timedelta


def find_event_source(df_tweets, interval):
    df_tweets.index = df_tweets.index.floor(interval)
    df_tweets = df_tweets.sort_values('retweets', ascending=False)
    df_tweets = df_tweets[~df_tweets.index.duplicated(keep='first')]
    return df_tweets.text


def get_top_tweets(df_tweets, count=5, text_col='full_text', user_name_col='user.screen_name'):
    # DUMMY
    top_tweets = df_tweets[[user_name_col, text_col]][0:count - 1]
    top_tweets = top_tweets.rename(columns={text_col: "text", user_name_col: "user_name"})
    top_tweets['retweet_count'] = 0
    top_tweets['like_count'] = 0
    top_tweets['created_at'] = top_tweets.index
    top_tweets['alerted_at'] = top_tweets['created_at']
    top_tweets = top_tweets.reset_index(drop=True)
    return top_tweets


def get_most_retweeted(df_tweets, time_window):
    last_index = len(df_tweets.index) - 1
    # print(df_tweets.index)
    last_tweet_time = df_tweets.index[0]
    # last_tweet_time = max(df_tweets.index[last_index], df_tweets.index[0]) # fix ordering issue
    from_time = last_tweet_time - timedelta(minutes=time_window)
    df = df_tweets[df_tweets.index > from_time]
    return df[df['retweet_count'] == df['retweet_count'].max()].full_text
