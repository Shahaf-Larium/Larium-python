import boto3
import s3fs

from config import KEY, SECRET, s3_path, datetimeindex_format
from modules import pdutils

s3 = boto3.resource('s3', aws_access_key_id=KEY, aws_secret_access_key=SECRET)
myBucket = s3.Bucket('larium.bucket')
fs = s3fs.S3FileSystem(key=KEY, secret=SECRET)

directory_list = []


def save_to_s3(data, dir_path, file_name):
    bytes_to_write = data.to_csv(None).encode()
    with fs.open(s3_path + dir_path + '/' + file_name, 'wb') as f:
        f.write(bytes_to_write)


def init_directory_list(prefix):
    for obj in myBucket.objects.filter(Prefix=prefix):
        dir_name = obj.key.split('/')[-2]
        directory_list.append(dir_name)


def load_tweet_data_from_s3(file_name, stock):
    """
        :param file_name: The name of the csv file you wish too load
        :param stock: Stock name you wish to get the csv from
        :example:
            fs.open('s3://larium.bucket/tweets/AAPL/11-11-20.csv', 'rb')
        :return:
            return the DataFrame
        """
    loaded_df = pdutils.read_time_series(fs.open(s3_path + 'tweets/' + stock + '/' + file_name, 'rb'),
                                         datetimeindex=datetimeindex_format)
    return loaded_df


# init_directory_list('tweets/')
#
# df = pd.DataFrame([[1, 1, 1], [2, 2, 2]], columns=['a', 'b', 'c'])
# save_to_s3(df, 'tweets/test', 'test.csv')
# load_tweet_data_from_s3('test.csv', 'test')
