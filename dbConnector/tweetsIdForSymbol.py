from config import stock_list

tweetsIds = {}
idNum = 1
for stock in stock_list:
    tweetsIds[stock] = [idNum, idNum + 4]
    idNum += 5
