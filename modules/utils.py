from os import listdir, mkdir
from os import path as Path
from datetime import datetime, timedelta
from calendar import monthrange

from pandas import date_range


def filter_files(files_list, filters):
    result = []
    if type(filters) is list:
        for filter in filters:
            result.extend([f for f in files_list if filter in f])
    else:
        result = [f for f in files_list if filters in f]
    return result


def get_files(folder, suffix=".csv"):
    filenames = listdir(folder)
    return [filename for filename in filenames if filename.endswith(suffix)]


def split_to_date_range_to_months_range(self, since, until):
    periods = []
    last_date = datetime.strptime(until, "%Y-%m-%d")
    curr_date = datetime.strptime(since, "%Y-%m-%d")
    last_day_in_month = monthrange(curr_date.year, curr_date.month)[1]
    next_date = datetime(curr_date.year, curr_date.month, last_day_in_month) + timedelta(days=1)
    while next_date < last_date:
        periods.append((curr_date, next_date))
        curr_date = next_date
        last_day_in_month = monthrange(curr_date.year, curr_date.month)[1]
        next_date = datetime(curr_date.year, curr_date.month, last_day_in_month) + timedelta(days=1)

    periods.append((curr_date, last_date))
    return periods


def create_dir_if_not_exist(folder_path):
    if not Path.exists(folder_path):
        mkdir(folder_path)


def day_format(date_time):
    return datetime.strftime(date_time, '%Y-%m-%d')


def timeformat(date_time):
    return datetime.strftime(date_time, '%H-%M-%S')


def today(tz=True):
    dt = datetime.today()
    dt = datetime(dt.year, dt.month, dt.day)
    return dt


def floor_dt(dt, on, to):
    if on == 'hours':
        new_val = dt.hour - dt.hour % to
        dt = dt.replace(hour=new_val)
    elif on == 'minutes':
        new_val = dt.minute - dt.minute % to
        dt = dt.replace(minute=new_val)
    elif on == 'seconds':
        new_val = dt.second - dt.second % to
        dt = dt.replace(second=new_val)
    return dt


def dates_in_range(range):
    dts = date_range(start=range[0], end=range[1])
    return [day_format(d) for d in dts]
