import argparse
from datetime import datetime
from os import path as Path
import yaml

parser = argparse.ArgumentParser(description='Load tweets data with a given query in a given dates range.')
parser.add_argument('stock', metavar='Q', type=str, help='Stock ticker')
parser.add_argument('since', metavar='S', type=str, help='date format: Y-m-d')
parser.add_argument('until', metavar='T', type=str, help='date format: Y-m-d')
parser.add_argument('interval', metavar='I', type=str, choices=['1d', '30min'])
args = parser.parse_args()

with open("config.yaml", 'r') as file:
    data = yaml.load(file, yaml.Loader)
    TWEETS_ARCHIVE_DATA_FOLDER = data['databases']['tweets']['archive']
    LIVE_TWEETS_DATA_FOLDER = data['databases']['tweets']['recent']


if args.interval == "30min":
    STOCKS_INTERVAL = "60m"
else:
    STOCKS_INTERVAL = args.interval
TWEETS_DATA_PATH = Path.join(TWEETS_ARCHIVE_DATA_FOLDER, args.stock)
SINCE = datetime.strptime(args.since, "%Y-%m-%d")
UNTIL = datetime.strptime(args.until, "%Y-%m-%d")
