import pandas as pd
from sec.sql import create_connection
from sec.tickers import Tickers
from sec.cluster import StringCluster


CONN = create_connection('sec.db')
TICKERS = Tickers()

data = pd.read_sql(
    sql="select * from holdings where report_pd == '12-31-2020';",
    con=CONN
)
data['company'] = data['company'].str.upper()
data = data.groupby('company').sum().reset_index().sort_values(by='value', ascending=False).iloc[:25000, :]
data = data[data['company'] != '']  # remove blanks
data.reset_index(drop=True, inplace=True)

cluster = StringCluster()
labels = cluster.fit_transform(data['company'], TICKERS.names)
data['company'] = labels.values
top_25k = data.groupby('company').sum().sort_values(by='value', ascending=False).reset_index()
top_25k['ticker'] = top_25k['company']
top_25k['ticker'].replace(TICKERS.tickers, inplace=True)
top_25k.to_sql('top_holdings', CONN, if_exists='replace', index=False)
CONN.close()

# test = pd.DataFrame({'in': data['company'], 'out': labels})
