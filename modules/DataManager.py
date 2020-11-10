from TickerFlask.dbConnector.DbConnectorCassandra import DbConnector
from TickerFlask import dbConnector as Query


class DataManager:

    def __init__(self):
        self.dbConnector = DbConnector()

    def write_to_db(self, ticker_list):
        self.dbConnector.add_ticker(ticker_list)

    def read_from_db(self):
        result = self.dbConnector.get_queries(Query.GET_LATEST_STOCK_DATA)
        json_result = {"ticker_list": result}
        return json_result
