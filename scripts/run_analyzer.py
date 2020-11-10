import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))
import argparse
from TickerFlask.products import Analyzer
from TickerFlask.data.loader import FileManager
parser = argparse.ArgumentParser(description='Analyze tweets data.')
parser.add_argument('stock', type=str, help='Stock ticker')
parser.add_argument('dates', type=str, nargs='+', help='list of dates to load. date format: Y-m-d')
args = parser.parse_args()
loader = FileManager(database_folder="data/tweets/stocks")
anz = Analyzer(ts=loader.load(stock=args.stock, dates=args.dates).index)
anz.show()
