import pandas as pd
from datetime import timedelta, datetime
from plotly import graph_objs as go

pd.options.plotting.backend = "plotly"
import plotly.io as pio

pio.renderers.default = "browser"
import modules.utils as utils
import modules.timeutils as timeutils


def get_tweets_volume(df_tweets, interval):
    df_tweets = df_tweets.assign(volume=1)
    df_tweets.index = df_tweets.index.floor(interval)
    return pd.DataFrame(df_tweets.groupby([df_tweets.index])['volume'].count_rows())


def anomaly_detector_in_tweets_volume(df_tweets_vol, thres=2, min_vol=10):
    thresholds = df_tweets_vol.mean() * thres
    df_anomalies = df_tweets_vol.copy() - thresholds
    df_anomalies[df_anomalies < 0] = 0
    df_anomalies[df_anomalies < min_vol] = 0
    df_anomalies = df_anomalies[df_anomalies > 0] + thresholds
    return df_anomalies


class Analyzer():
    def __init__(self, ts, long_window=60, short_window=10, alpha=2):
        self.time_series = ts
        self.data = self.count_rows(interval='1min', )
        self.resample_interval(interval='2min')
        self.resample_interval(interval='5min')
        self.resample_interval(interval='10min')
        self.resample_interval(interval='30min')
        self.long_window = long_window
        self.short_window = short_window
        self.data['median'] = self.data['1min'].rolling(window=self.long_window).median()
        self.data['mean'] = self.data['1min'].rolling(window=self.long_window).mean()
        self.data['mean_short'] = self.data['1min'].rolling(window=self.short_window).mean()
        self.data['std'] = self.data['1min'].rolling(window=self.long_window).std(ddof=0)
        self.data['zscore'] = self.zscore(self.data['1min'], self.long_window)
        self.data['upper_band'] = self.data['mean'] + (self.data['std'] * alpha)
        self.data['cumsum'] = self.data['1min'].cumsum()
        self.data['change'] = ((self.data['mean_short'] - self.data['mean']) / self.data['mean']) * 100
        self.last_index = len(self.data.index) - 1

    def count_rows(self, interval, fill_gaps=True, interpolate=True):
        df = self.time_series.to_frame(name="date_time")
        df = df.assign(count=1)
        df.index = pd.DatetimeIndex(df.date_time)
        df.index = df.index.floor(interval)
        df = pd.DataFrame(df.groupby([df.index])['count'].count())
        df = df.rename(columns={'count': interval})
        if fill_gaps:
            df = df[interval].resample(interval).sum().to_frame()
        if interpolate:
            df[interval] = df[interval].interpolate()
        return df

    def resample_interval(self, interval, interpolate=True):
        self.data[interval] = self.data['1min'].resample(interval).sum()
        if interpolate:
            self.data[interval] = self.data[interval].interpolate()

    def zscore(self, x, window):
        r = x.rolling(window=window)
        m = r.mean().shift(1)
        s = r.std(ddof=0).shift(1)
        z = (x - m) / s
        return z

    def show(self):
        events_detected = pd.DataFrame()
        events_detected['above_3'] = self.data['1min'].mask(self.data['zscore'] < 3)
        events_detected['above_4'] = self.data['1min'].mask(self.data['zscore'] < 4)
        events_detected['above_5'] = self.data['1min'].mask(self.data['zscore'] < 5)
        fig = self.data.plot()
        fig.add_trace(go.Scatter(x=self.data.index, y=events_detected['above_3'],
                                 mode='markers', name='markers',
                                 marker=dict(color='red')))
        fig.add_trace(go.Scatter(x=self.data.index, y=events_detected['above_4'],
                                 mode='markers', name='markers',
                                 marker=dict(color='yellow')))
        fig.add_trace(go.Scatter(x=self.data.index, y=events_detected['above_5'],
                                 mode='markers', name='markers',
                                 marker=dict(color='orange')))
        fig.show()

    def get_interest(self, in_last=None):
        interest = 0
        if in_last is None:
            interest = int(self.data['cumsum'].iloc[self.last_index])
        else:
            if in_last == '1d':
                df = self.data[self.data.index >= timeutils.today()]
                interest = int(df['1min'].sum())
            elif in_last == '30min':
                df = self.data[self.data.index >= timeutils.floor_dt(dt=timeutils.now(), on="minutes", to=30)]
                interest = int(df['1min'].sum())
        return interest

    def get_change(self):
        change = float(self.data['change'].iloc[self.last_index])
        if pd.isna(change):
            change = 0
        return change

    def is_event_detected(self, since=None, minutes=None, z_score_threshold=5):
        if minutes:
            last_index = len(self.data.index) - 1
            last_tweet_time = max(self.data.index[last_index], self.data.index[0])  # fix ordering issue
            from_time = last_tweet_time - timedelta(minutes=minutes)
        elif since:
            from_time = since
        else:
            print("WARNING: is_event_detected() illegal use of function.")
            return False

        df = self.data[self.data.index >= from_time]
        events = df.mask(df['zscore'] < z_score_threshold)
        events = events[events['zscore'].notnull()]
        if events.empty:
            return False
        return True  # TODO return NULL instead of false and the time of the event if detected.
