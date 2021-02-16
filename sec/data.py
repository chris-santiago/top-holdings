import pandas as pd
from sec.sql import create_connection
import numpy as np
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
from sklearn.pipeline import Pipeline
from sklearn.manifold import Isomap, MDS


CONN = create_connection('sec.db')

holdings = pd.read_sql('select * from top_holdings;', CONN)
summary = pd.read_sql(
    """
    select ticker, dividendYield, beta, trailingPE, forwardPE, averageVolume, 
    marketCap
    from yahoo_summary;
    """,
    CONN
)
stats = pd.read_sql(
    """
    select ticker, profitMargins, shortPercentOfFloat, priceToBook, pegRatio,
    enterpriseToRevenue, enterpriseToEbitda, "52WeekChange"
    from yahoo_stats;
    """,
    CONN
)
profile = pd.read_sql(
    """
    select ticker, industry, sector
    from yahoo_profile
    where industry != 0 and industry != ""
    """,
    CONN
)
history = pd.read_sql('select * from yahoo_history;', CONN)
CONN.close()

data = holdings.set_index('ticker').merge(profile.set_index('ticker'), left_index=True, right_index=True)
data = data.merge(summary.set_index('ticker'), left_index=True, right_index=True)
data = data.merge(stats.set_index('ticker'), left_index=True, right_index=True)
data = data.merge(history.set_index('ticker'), left_index=True, right_index=True)
cap = pd.cut(
    data['marketCap'],
    bins=[-np.inf, 3e8, 2e9, 10e9, 2e11, np.inf],
    labels=['micro', 'small', 'mid', 'large', 'mega']
)
data.insert(4, 'market_cap', cap)
data = data.iloc[:, :-1]

for mcap in data['market_cap'].unique():
    mask = data['market_cap'] == mcap
    df = data[mask]
    df = df.sort_values('value', ascending=False)
    df_profile = df.iloc[:50, :5]
    df_num = df.iloc[:50, 5:].astype(float)
    df_num.replace(np.inf, 0, inplace=True)
    pl = Pipeline(
        steps=[
            ('scaler', StandardScaler()),
            # ('pca', PCA(n_components=2)),
            ('encode', Isomap(n_neighbors=5, n_components=2)),
            # ('mds', MDS(metric=False))
        ]
    )
    components = pl.fit_transform(df_num)
    fn = f'data-{mcap}.csv'
    out = df_profile.reset_index()
    out['pc1'] = components[:, 0]
    out['pc2'] = components[:, 1]
    out.to_csv(fn, index=False)
