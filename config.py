import os
from pathlib import Path

import yaml

dir_path = Path(__file__).parent
config_file = os.path.join(dir_path, "config.yaml")

with open(config_file, 'r') as file:
    config = yaml.load(file, yaml.Loader)

# Set the root path for the local computer
root_path = dir_path

# List of all out stocks sorted alphabetically
stocks_list = []
for k, v in config['stocks_dict'].items():
    stocks_list.append(v['ticker'])
stocks_list = sorted(stocks_list)

#   Twitter API   #
consumer_key = 'SD6ctJ73B3WlWFeQ0G07rHZrP'
consumer_secret = 'V6U3Kd4hyWCOAJuCvyMRK5up0K8nhaZAi3cXylKjUCnMW4EiIi'
bearer_token = 'AAAAAAAAAAAAAAAAAAAAAJ6bJAEAAAAAKmMffbpleYZEwovPt8CehP1MrMc%3DIWURAS4FqIiPhJ32HzI7euF5Qkphp2FO68BJeBr3DMmp9Et7jr'
access_token = '1208323412042698753-YIPvJw4x0cAw1Yn7kyvjTDDzjswyBA'
access_token_secret = 'eN4aFgzUstLsIiZy3d35u8ETYZwZXCMOdCnKOEFvv6v0t'

#   S3   #
KEY = 'AKIAR6LIIEWYT4TP4BSB'
SECRET = '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'
s3_path = 's3://larium.bucket/'

# General #
datetimeindex_format = config['tweepy']['our_api']['datetime_index_name']
