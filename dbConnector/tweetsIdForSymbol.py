from config import stocks_list

tweetsIds = {}
idNum = 1
for stock in stocks_list:
    tweetsIds[stock] = [idNum, idNum + 4]
    idNum += 5
