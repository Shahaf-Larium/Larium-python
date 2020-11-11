from cassandra.cluster import Cluster, ConsistencyLevel
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import BatchStatement
from cassandra import RequestExecutionException
from cassandra.query import dict_factory
from config import root_path
from random import randrange

from dbConnector import Queries
from dbConnector.tweetsIdForSymbol import tweetsIds


class DbConnector:

    def __init__(self):
        self.id = 0
        cloud_config = {
            'secure_connect_bundle': root_path / 'secure-connect-ticker-db.zip'
        }
        auth_provider = PlainTextAuthProvider('ticker', 'ticker')
        cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
        self.session = cluster.connect('tickers')

    def add_ticker(self, ticker_data):
        print("Insert batch to DB")
        insert_ticker = self.session.prepare(
            "INSERT INTO stocks_data (id, stock, daily_interset, current_change, events_data, last_interest,fetched_at, last_event) VALUES (?,?,?,?,?,?,?,?)")

        insert_latest = self.session.prepare(
            "UPDATE stocks SET daily_interset = ?, current_change = ?, events_data = ?, last_interest = ?,fetched_at = ? where stock = ?")

        insert_latest_with_event = self.session.prepare(
            "UPDATE stocks SET daily_interset = ?, current_change = ?, events_data = ?, last_interest = ?,fetched_at = ?, last_event =? where stock = ?")

        batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
        for stock, ticker in ticker_data.items():
            if stock == 'fetched_at':
                break

            if ticker["events_data"] is not None:
                self.add_tweet(ticker["events_data"], stock)

            # Insert the stock data into the log table
            batch.add(insert_ticker,
                      [self.id, ticker["stock"], ticker["daily_interset"], ticker["current_change"],
                       ticker["is_event"],
                       ticker["last_interest"], ticker_data["fetched_at"], ticker["last_event"]])

            # Insert the stocks data into the latest changes table (this table is presented in the UI)
            if ticker["is_event"]:
                batch.add(insert_latest_with_event,
                          [ticker["daily_interset"], ticker["current_change"], ticker["is_event"],
                           ticker["last_interest"], ticker_data["fetched_at"], ticker["last_event"], ticker["stock"]])
            else:
                batch.add(insert_latest,
                          [ticker["daily_interset"], ticker["current_change"], ticker["is_event"],
                           ticker["last_interest"], ticker_data["fetched_at"], ticker["stock"]])
            self.id += 1
        try:
            self.session.execute(batch)
            print("Insert done successfully")
        except RequestExecutionException:
            print("Fail to execute the query.. Trying one more time")
            try:
                self.session.execute(batch)
                print("Insert done successfully")
            except RequestExecutionException:
                print("Something is wrong with the DB check this out!")

    def get_queries(self, query, params=None):
        self.session.row_factory = dict_factory
        print("Reading data from DB")
        result = self.session.execute(query, params).all()
        return result

    def parametrized_query(self, query, params):
        param_query = self.session.prepare(query)
        return self.get_queries(param_query, params)

    def add_tweet(self, tweets, stock):
        print("Insert batch of tweets to DB")
        latest_tweets = self.session.prepare(Queries.INSERT_TWEETS)
        tweets_history = self.session.prepare(Queries.INSERT_TWEETS_HISTORY)

        batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)

        user_names = tweets['user_name']
        retweet_count = tweets['retweet_count']
        alerted_at = tweets['alerted_at']
        created_at = tweets['created_at']
        text = tweets['text']
        like_count = tweets['like_count']
        idNumStart, idNumEnd = tweetsIds[stock]
        for num in range(len(user_names)):
            params_list = [stock, retweet_count[num], alerted_at[num].strftime("%m/%d/%Y, %H:%M:%S"),
                           created_at[num].strftime("%m/%d/%Y, %H:%M:%S"),
                           text[num], user_names[num], like_count[num]]
            # Add the tweets to the "All tweets table" with non importance id
            params_list.insert(0, self.id)
            batch.add(tweets_history, params_list)
            # Add the tweets to the latest tweets of each symbol with uniq id per symbol
            params_list.pop(0)
            params_list.insert(0, idNumStart)
            batch.add(latest_tweets, params_list)

            idNumStart += 1

        try:
            self.session.execute(batch)
            print("Insert done successfully")
        except RequestExecutionException:
            print("Fail to execute the query.. Trying one more time")
            try:
                self.session.execute(batch)
                print("Insert done successfully")
            except RequestExecutionException:
                print("Something is wrong with the DB check this out!")
