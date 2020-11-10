GET_ALL_STOCK_DATA = "SELECT * FROM stocks_data"
GET_DATA_BY_STOCK_NAME = "SELECT * FROM stocks_data WHERE stock = %(stock)s ALLOW FILTERING"
GET_LATEST_STOCK_DATA = "SELECT * FROM stocks"
GET_LATEST_STOCK_DATA_BY_NAME = "SELECT * FROM latest WHERE stock = %(stock)s ALLOW FILTERING"

INSERT_TWEETS = "INSERT INTO tweets (id, stock, retweet_count, alerted_at, created_at, text,user_name, like_count) VALUES (?,?,?,?,?,?,?,?)"
