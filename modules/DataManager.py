from dbConnector.DbConnectorCassandra import DbConnector
from dbConnector import Queries


class DataManager:

    def __init__(self):
        self.dbConnector = DbConnector()

    def write_to_db(self, ticker_list):
        self.dbConnector.add_ticker(ticker_list)

    def read_from_db(self):
        result = self.dbConnector.get_queries(Queries.GET_LATEST_STOCK_DATA)
        json_result = {"ticker_list": result}
        return json_result
