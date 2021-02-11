import pandas as pd
from sec.sql import create_connection
import numpy as np
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
from sklearn.pipeline import Pipeline
from sklearn.manifold import Isomap, MDS


CONN = create_connection('sec.db')
top_stocks = pd.read_sql('select * from top_1000;', CONN)
bloomberg = pd.read_sql('select * from key_stats;', CONN)
yahoo = pd.read_sql('select * from yahoo_stats;', CONN)
CONN.close()

data = top_stocks.groupby(['report_pd', 'ticker']).sum().reset_index(level=0)
data = data.merge(bloomberg.set_index('Ticker').iloc[:, 1:], left_index=True, right_index=True)
data = yahoo.set_index('symbol').merge(data, left_index=True, right_index=True).sort_index()

num_data = data.iloc[:, 3:].reset_index(drop=True).drop('report_pd', axis=1)
data.index.names = ['ticker']
data = data[['report_pd', 'shortName', 'sector', 'industry', 'value']].reset_index()
data['market_cap'] = pd.cut(
    num_data['Market Cap (M)'],
    bins=[-np.inf, 300, 2000, 10000, 200000, np.inf],
    labels=['micro', 'small', 'mid', 'large', 'mega']
)

pl = Pipeline(
    steps=[
        ('scaler', StandardScaler()),
        # ('pca', PCA(n_components=2)),
        # ('encode', Isomap(n_neighbors=3, n_components=2)),
        ('mds', MDS())
    ]
)
components = pl.fit_transform(num_data)

data['pc1'] = components[:, 0]
data['pc2'] = components[:, 1]
data.to_csv('data.csv', index=False)
