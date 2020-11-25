from config import config, root_path, stocks_list
from pathlib import Path
import modules.utils as utils
from datetime import datetime
import pandas as pd
pd.options.plotting.backend = "plotly"
import plotly.io as pio
pio.renderers.default = "browser"

class Logger():
    def __init__(self):
        self.stocks_list = stocks_list
        self.logs_folder = root_path / Path(config['web_interface']['logs']['folder'])
        self.logs_data = dict.fromkeys(self.stocks_list)

    def load_logs(self, day=None):
        logs_data = dict.fromkeys(self.logs_data.keys())
        for stock in self.logs_data.keys():
            csv_file_path = self.logs_folder / self.get_log_file_name(stock, day)
            try:
                logs_data[stock] = pd.read_csv(str(csv_file_path))
            except FileNotFoundError:
                print("can not find file {}".format(csv_file_path))
        return logs_data

    def show_logs(self, stocks=None, day=None):
        if not stocks:
            stocks = self.stocks_list
        self.logs_data = self.load_logs(day)
        for stock, logs_df in self.logs_data.items():
            if stock not in stocks:
                continue
            fig = logs_df[["interest", "change"]].plot()
            fig.show()
            # try:
            #     logs_df.plot()
            # except ValueError:
            #     print("try to plot None value.")

    def get_log_file_name(self, stock, day=None):
        if not day:
            date = utils.day_format(datetime.now())
        else:
            date = day
        return str(stock + '_' + date + ".csv")