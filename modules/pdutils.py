import pandas as pd

def append(to, append_me):
    df = to.append(append_me, sort=False)
    df = df.sort_index(ascending=False)
    df = df[~df.index.duplicated(keep='first')]
    return df