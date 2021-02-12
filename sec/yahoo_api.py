import time
import aiohttp
import aiohttp.web
import asyncio
import pandas as pd
import json


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
        payload = {'modules': 'summaryDetail,defaultKeyStatistics'}
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
            details = item[1]['quoteSummary']['result'][0]
            parsed[ticker] = {
                'summary': details['summaryDetail'],
                'stats': details['defaultKeyStatistics']
            }
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
        connector = aiohttp.TCPConnector(limit=10)
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
            parsed[ticker] = item[ticker].get('close')
        return parsed
