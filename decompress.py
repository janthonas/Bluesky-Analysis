import zlib
import base64

import pandas as pd

current = pd.read_csv('output/current.csv')

def decompress(post):
    return zlib.decompress(base64.b64decode(post)).decode('utf-8')

current['posts'] = current['posts'].apply(decompress)

current.to_csv('output/decompress.csv', index=False)