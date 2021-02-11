import requests
import pandas as pd
from sec.sql import create_connection, execute_query
from sec.cluster import StringCluster
from tqdm import tqdm
import json


CONN = create_connection('sec.db')


class BloombergAPI:

    headers = {
        'x-rapidapi-key': "a9f14ee177mshde145bc2ef518aap11e8b7jsn33f1e888c4f1",
        'x-rapidapi-host': "bloomberg-market-and-financial-news.p.rapidapi.com"
    }

    def __init__(self):
        self.key = 'a9f14ee177mshde145bc2ef518aap11e8b7jsn33f1e888c4f1'
        self.base_url = 'https://bloomberg-market-and-financial-news.p.rapidapi.com'

    def get_ticker(self, company):
        payload = {'query': company}
        payload_2 = {'query': ' '.join(company.split(' ')[:2])}
        payload_3 = {'query': company.split(' ')[0]}

        url = f'{self.base_url}/market/auto-complete'
        try:
            response = requests.request("GET", url, headers=self.headers, params=payload)
            return response.json()['quote'][0]['symbol']
        except Exception as e:
            try:
                response = requests.request("GET", url, headers=self.headers, params=payload_2)
                return response.json()['quote'][0]['symbol']
            except Exception as e:
                try:
                    response = requests.request("GET", url, headers=self.headers, params=payload_3)
                    return response.json()['quote'][0]['symbol']
                except Exception as e:
                    return None

    def get_stats(self, ticker):
        payload = {'id': f'{ticker}:us'}
        url = f'{self.base_url}/stock/get-statistics'
        try:
            response = requests.request("GET", url, headers=self.headers, params=payload)
            return response.json()['result'][0]
        except Exception as e:
            return None


data = pd.read_sql(
    sql="select * from holdings where report_pd == '12-31-2020';",
    con=CONN
)
data['company'] = data['company'].str.upper()
data = data.groupby('company').sum().reset_index().sort_values(by='value', ascending=False).iloc[:10000, :]
data = data[data['company'] != '']  # remove blanks
data.reset_index(drop=True, inplace=True)
cluster = StringCluster()
labels = cluster.fit_transform(data['company'])
data['company'] = labels.values
top_1000 = data.groupby('company').sum().sort_values(by='value', ascending=False).iloc[:1000, :]

# out = top_1000.reset_index(drop=True)
# with open('tickers.json', 'r') as fp:
#     ticker_map = json.load(fp)
# out['ticker'] = out['company']
# out['ticker'] = out['ticker'].replace(ticker_map)

api = BloombergAPI()
tickers = []
for company in tqdm(top_1000.index):
    tickers.append(api.get_ticker(company))
ticker_map = dict(zip(top_1000.index, tickers))

with open('tickers2.json', 'w') as fp:
    json.dump(ticker_map, fp)

out = top_1000.reset_index()
out.insert(1, 'ticker', tickers)
out.insert(0, 'report_pd', '2020-12-31')
out.to_sql('top_1000', CONN, if_exists='replace', index=False)
CONN.close()

# query = """
#     create table top_1000 (
#     report_pd TEXT,
#     company TEXT,
#     ticker TEXT,
#     value REAL
#     );
#     """
# execute_query(CONN, query)

