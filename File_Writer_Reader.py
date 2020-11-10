import json
from config import root_path


def write_to_file(jsonData):
    file = open("tableStocksData.json", "w")

    toTheFile = {'tickerList': []}
    tickers = jsonData["tickerList"]
    for entity in tickers:
        stock = {'stock': entity['stock'], 'interest': entity['interest'], 'velocity': entity['velocity'],
                 'mostactive': entity['mostactive']}
        toTheFile['tickerList'].append(stock)
        print(entity["stock"], entity["velocity"])
    json.dump(toTheFile, file)
    file.close()


def read_from_file():
    # file_path = root_path /
    file = open("tableStocksData.json", "r")

    fromJson = {"tickerList": []}
    jsonString = json.load(file)
    for entity in jsonString["tickerList"]:
        fromJson["tickerList"].append(entity)

    file.close()

    return fromJson
