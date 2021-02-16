import time
import aiohttp
import aiohttp.web
import asyncio
import pandas as pd
from sec.sql import create_connection


class YahooAPI:

    URL = 'https://yahoo-finance-low-latency.p.rapidapi.com'

    HEADERS = {
        'x-rapidapi-key': "a9f14ee177mshde145bc2ef518aap11e8b7jsn33f1e888c4f1",
        'x-rapidapi-host': "yahoo-finance-low-latency.p.rapidapi.com"
    }

    def __init__(self):
        self.data_ = None

    async def get(self, client, endpoint, payload):
        try:
            async with client.get(
                url=f'{self.URL}/{endpoint}',
                headers=self.HEADERS,
                params=payload
            ) as resp:
                response = await resp.json()
                return response
        except (aiohttp.web.HTTPNotFound, aiohttp.web.HTTPBadGateway):
            time.sleep(3)
            try:
                async with client.get(
                        url=f'{self.URL}/{endpoint}',
                        headers=self.HEADERS,
                        params=payload
                ) as resp:
                    response = await resp.json()
                    return response
            except (aiohttp.web.HTTPNotFound, aiohttp.web.HTTPBadGateway):
                return None

    async def fetch_all(self, tickers):
        raise NotImplementedError

    def run(self, tickers):
        self.data_ = asyncio.run(self.fetch_all(tickers))
        return self


class YahooStats(YahooAPI):
    def __init__(self):
        super().__init__()

    async def fetch(self, ticker, client):
        endpoint = f'v11/finance/quoteSummary/{ticker}'
        payload = {'modules': 'summaryDetail,defaultKeyStatistics,assetProfile'}
        return ticker, await self.get(client, endpoint, payload)

    async def fetch_all(self, tickers):
        connector = aiohttp.TCPConnector(limit=10)
        async with aiohttp.ClientSession(connector=connector) as client:
            tasks = [asyncio.create_task(self.fetch(ticker, client)) for ticker in tickers]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            return results

    def parse(self):
        parsed = {}
        for item in self.data_:
            ticker = item[0]
            try:
                details = item[1]['quoteSummary']['result'][0]
                parsed[ticker] = {
                    'summary': details['summaryDetail'],
                    'stats': details['defaultKeyStatistics'],
                    'profile': details['assetProfile']
                }
            except (TypeError, KeyError):
                parsed[ticker] = None
        return parsed


class YahooHistory(YahooAPI):
    def __init__(self):
        super().__init__()

    async def fetch(self, tickers, client):
        if isinstance(tickers, (list, pd.Series)):
            tickers = ','.join(tickers)
        endpoint = 'v8/finance/spark'
        payload = {'symbols': tickers, 'range': '1y', 'interval': '1wk'}
        return await self.get(client, endpoint, payload)

    async def fetch_all(self, tickers):
        i = 0
        batch_size = 10
        connector = aiohttp.TCPConnector(limit=20)
        async with aiohttp.ClientSession(connector=connector) as client:
            results = []
            while i < len(tickers):
                batch = tickers[i:i+batch_size]
                tasks = [asyncio.create_task(self.fetch(ticker, client)) for ticker in batch]
                results.append(await asyncio.gather(*tasks, return_exceptions=True))
                i += batch_size
            return results

    def parse(self):
        parsed = {}
        for item in self.data_[0]:
            ticker = list(item.keys())[0]
            try:
                parsed[ticker] = item[ticker].get('close')
            except (TypeError, KeyError):
                parsed[ticker] = None
        return parsed


def main():
    conn = create_connection('sec.db')
    top = pd.read_sql('select * from top_holdings;', conn)
    stats = YahooStats()
    stats.run(top['ticker'])
    stats_data = stats.parse()

    def parse_summary(data, query='summary'):
        out = {}
        for ticker in data:
            if data[ticker]:
                inside = {}
                for metric in data[ticker][query]:
                    if isinstance(data[ticker][query][metric], dict):
                        inside[metric] = data[ticker][query][metric].get('raw')
                    else:
                        inside[metric] = data[ticker][query][metric]
                out[ticker] = inside
            else:
                out[ticker] = None
        return out

    def to_dataframe(parsed):
        df = pd.DataFrame().from_dict(parsed).T
        df.dropna(axis=0, how='all', inplace=True)
        df.dropna(axis=1, how='all', inplace=True)
        df.fillna(0, inplace=True)
        df = df.iloc[:, 2:].reset_index()
        return df

    for table in ['summary', 'stats']:
        out = parse_summary(stats_data, table)
        out = to_dataframe(out)
        out.to_sql(f'yahoo_{table}', conn, if_exists='replace', index=False)

    history = YahooHistory()
    history.run(top['ticker'])

    out = {}
    for outer in history.data_:
        for item in outer:
            for ticker in item:
                try:
                    out[ticker] = item[ticker]['close']
                except KeyError:
                    out[ticker] = None

    out2 = {k: v for k,v in out.items() if v and len(v) == 54}
    df = pd.DataFrame().from_dict(out2).pct_change()
    df = df.iloc[1:, :].T.reset_index()
    df.to_sql(f'yahoo_history', conn, if_exists='replace', index=False)
    conn.close()
