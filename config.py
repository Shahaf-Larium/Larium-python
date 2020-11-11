import os
from pathlib import Path

import yaml

dir_path = Path(__file__).parent
config_file = os.path.join(dir_path, "config.yaml")

with open(config_file, 'r') as file:
    config = yaml.load(file, yaml.Loader)

root_path = dir_path
stock_list = sorted(config['stocks'])

consumer_key = 'SD6ctJ73B3WlWFeQ0G07rHZrP'
consumer_secret = 'V6U3Kd4hyWCOAJuCvyMRK5up0K8nhaZAi3cXylKjUCnMW4EiIi'
bearer_token = 'AAAAAAAAAAAAAAAAAAAAAJ6bJAEAAAAAKmMffbpleYZEwovPt8CehP1MrMc%3DIWURAS4FqIiPhJ32HzI7euF5Qkphp2FO68BJeBr3DMmp9Et7jr'
access_token = '1208323412042698753-YIPvJw4x0cAw1Yn7kyvjTDDzjswyBA'
access_token_secret = 'eN4aFgzUstLsIiZy3d35u8ETYZwZXCMOdCnKOEFvv6v0t'
