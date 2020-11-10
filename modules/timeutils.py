from datetime import datetime, timedelta
import pytz
from pytz import timezone
from iso8601 import parse_date

utc = pytz.utc


def now(use_utc_tz=True):
    '''
    :param use_utc_tz [bool] : if true return now() with time zone of utc. else no tz info for datetime obj.
    :return: now date time.
    '''
    if use_utc_tz:
        return datetime.now(tz=utc)
    else:
        return datetime.now()


def totz(dt: datetime, tz: str):
    '''
    :param dt: datetime object
    :param tz: e.g. 'UTC', 'US/Eastern' etc...
    :return: convert the time zone of the given date.
    '''
    tz_obj = timezone(tz)
    if not dt.tzinfo:
        print("WARNING: trying to convert time zone of datetime object with NO time zone.")
        print("Localize to UTC then convert.")
        utc.localize(dt)
    return dt.astimezone(tz_obj)


def toutc(dt):
    return totz(dt=dt, tz='UTC')


def day_format(date_time):
    return datetime.strftime(date_time, '%Y-%m-%d')


def timeformat(date_time):
    return datetime.strftime(date_time, '%H-%M-%S')


def today(tz=True):
    dt = now(use_utc_tz=tz)
    dt = datetime(dt.year, dt.month, dt.day, tzinfo=utc)
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


def tprint(utc_tz=False):
    dt = now(utc_tz)
    print("[{}]".format(dt.strftime("%Y-%m-%d %H:%M:%S <%Z>")))
