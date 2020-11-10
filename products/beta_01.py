import copy
import json
import time
from datetime import datetime
from pathlib import Path
import pandas as pd
import TickerFlask.modules.utils as utils
from config import config, root_path
#from products.event_detector.twitter_monitor import TwitterMonitor
from TickerFlask.products import Pulse
from TickerFlask.products import Analyzer
from TickerFlask.products import get_most_retweeted
from TickerFlask.dbConnector.DbConnectorCassandra import DbConnector
from TickerFlask import dbConnector as query


class WebInterface:
    '''
    TODO LIST
    TODO add date index to the logs files.
    TODO print logs to graphs.
    TODO add the most active tweet.
    '''

    def __init__(self, refresh_rate=30, start_world=False, verbose=False):

        self.json_path = root_path / config['web_interface']['json']['file']
        self.headers = config['web_interface']['json']['headers']
        self.name = config['web_interface']['json']['name']
        self.stocks_list = config['stocks']  # TODO strange bug with date_time?!
        self.logs_folder = root_path / Path(config['web_interface']['logs']['folder'])
        self.dbConnector = DbConnector()
        self.logs_data = dict.fromkeys(self.stocks_list)
        if start_world:
            # init json:
            self.json_data = self.create_json_template()
            self.write_to_json()

        self.logs_data = self.load_logs()
        self.json_data = self.read_from_json()
        self.pulse = Pulse(verbose=True)
        self.refresh_rate = self.pulse.refresh_rate
        self.save_logs_iters = 3
        self.stop = False
        self.is_live = False

    def update(self):
        print("Updating tweets data base:")
        self.pulse.update()  # update tweets data
        tweets_data = self.pulse.data  # pull all fetched tweets data from monitor
        print()
        self.update_json_data(tweets_data)
        print()
        self.write_to_log()
        print()
        self.write_to_json()


    def run(self):
        iter = 0
        self.is_live = True
        while not self.stop:
            try:
                self.update()
                print()
                if iter % self.save_logs_iters == 0:
                    self.save_logs()
                print("sleep for {} seconds.".format(self.refresh_rate))
                iter += 1
                time.sleep(self.refresh_rate)

            except KeyboardInterrupt:
                print("run(): killed by the user.")
                self.pulse.save()
                self.is_live = False
                return

        print("While loop is over")
        self.pulse.save()
        self.is_live = False
        self.stop = False

    def get_status(self):
        return self.is_live

    def update_json_data(self, tweets_data):
        tickers = self.json_data[self.name]
        for entity in tickers:
            stock = entity[self.headers[0]]  # stock ticker name
            data_analyzer = Analyzer(
                ts=tweets_data[stock].index)  # init data analyzer with the time series of the stock tweets
            entity[self.headers[1]] = data_analyzer.get_interest()  # interest
            entity[self.headers[2]] = data_analyzer.get_change()
            entity[self.headers[3]] = get_most_retweeted(tweets_data[stock], time_window=60)[0]  # most retweeted in the last 60 minutes
            entity[self.headers[4]] = data_analyzer.is_event_detected(minutes=10)

        print("json data has been updated.")

    def read_from_json(self):
        with open(self.json_path, 'r') as json_file:
            json_data = json.load(json_file)
        return json_data

    def read_from_db(self):
        result = self.dbConnector.get_queries(query.GET_LATEST_STOCK_DATA)
        json_result = {"ticker_list": result}
        return json_result

    def write_to_json(self):
        ticker_list = self.json_data["ticker_list"]
        self.dbConnector.add_ticker(ticker_list)
        with open(self.json_path, 'w') as json_file:
            json.dump(self.json_data, json_file)

        print("write to file: {} succeeded.".format(self.json_path))

    def create_json_template(self):
        json_data = {self.name: []}
        for stock in self.stocks_list:
            stock_entity = {self.headers[0]: stock,
                            self.headers[1]: 0,
                            self.headers[2]: 0,
                            self.headers[3]: "",
                            self.headers[4]: False}
            json_data[self.name].append(stock_entity)
        return json_data

    def write_to_log(self):
        tickers = copy.deepcopy(self.json_data[self.name])
        for entity in tickers:
            stock = entity[self.headers[0]]  # stock ticker name
            del entity[self.headers[0]]  # we dont want to add the 'stock_ticker' to the log.
            entity_df = pd.DataFrame([entity])
            entity_df['created_at'] = pd.DatetimeIndex([datetime.now()])
            if self.logs_data[stock] is None:
                self.logs_data[stock] = entity_df
            else:
                self.logs_data[stock] = self.logs_data[stock].append(entity_df, sort=False, ignore_index=True)
                # self.logs_data[stock] = self.logs_data[stock].append(entity_df, sort=False)#, ignore_index=True)

        print("logs data has been updated.")

    def save_logs(self):
        for stock, stock_data in self.logs_data.items():
            log_file_path = self.logs_folder / self.get_log_file_name(stock)
            if stock_data is not None:  # TODO check why there is NONE value for 'date_time' key?!
                stock_data.to_csv(log_file_path)
        print("logs saved.")

    def get_log_file_name(self, stock):
        today = utils.day_format(datetime.now())
        return str(stock + '_' + today + ".csv")

    def load_logs(self):
        logs_data = dict.fromkeys(self.logs_data.keys())
        for stock in self.logs_data.keys():
            csv_file_path = self.logs_folder / self.get_log_file_name(stock)
            try:
                logs_data[stock] = pd.read_csv(str(csv_file_path))
            except FileNotFoundError:
                print("can not find file {}".format(csv_file_path))
        return logs_data
