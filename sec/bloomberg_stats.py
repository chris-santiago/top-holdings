import asyncio
import aiohttp
from sec.sql import create_connection
import pandas as pd
import tqdm
import time

CONN = create_connection('sec.db')

URL = 'https://bloomberg-market-and-financial-news.p.rapidapi.com/stock/get-statistics'
HEADERS = {
    'x-rapidapi-key': "a9f14ee177mshde145bc2ef518aap11e8b7jsn33f1e888c4f1",
    'x-rapidapi-host': "bloomberg-market-and-financial-news.p.rapidapi.com"
}


async def fetch(client, ticker):
    payload = {'id': f'{ticker}:us'}
    async with await client.get(
        url=URL,
        headers=HEADERS,
        params=payload
    ) as resp:
        return ticker, await resp.json()


async def fetch_all(tickers):
    connector = aiohttp.TCPConnector(limit=4)
    async with aiohttp.ClientSession(connector=connector) as client:
        tasks = [asyncio.create_task(fetch(client, ticker)) for ticker in tickers]
        responses = await asyncio.gather(*tasks)
        return responses


def clean_response(response):
    data = {}
    for item in response:
        ticker = item[0]
        try:
            table = item[1]['result'][0]['table']
            data[ticker] = {x['name']: x['value'] for x in table}
        except KeyError:
            data[ticker] = None
    return data


tickers = pd.read_sql('select ticker from top_1000', con=CONN)['ticker'].tolist()
CONN.close()

i = 0
batch = 5
data = {}
pbar = tqdm.tqdm(total=len(tickers)+1)
while i < len(tickers):
    response = asyncio.run(fetch_all(tickers[i:i+batch]))
    data.update(clean_response(response))
    time.sleep(.2)
    i += batch
    pbar.update(batch)
pbar.close()


failed = [key for key, value in data.items() if value is None]
data2 = {k: v for k, v in data.items() if v is not None}
i = 0
batch = 5
pbar = tqdm.tqdm(total=len(failed)+1)
while i < len(failed):
    response = asyncio.run(fetch_all(failed[i:i+batch]))
    data2.update(clean_response(response))
    time.sleep(.2)
    i += batch
    pbar.update(batch)
pbar.close()

data2.update(clean_response(asyncio.run(fetch_all(['PLTR']))))

import json
with open('key_stats.json', 'w') as fp:
    json.dump(data2, fp)

data3 = {k: v for k, v in data2.items() if v}
df = pd.DataFrame.from_dict(data3, orient='index')
df.fillna(0, inplace=True)
df.to_csv('key_stats.csv')


def clean_cell(x):
    if isinstance(x, str):
        x = x.replace(',', '')
        x = x.replace('%', '')
    return x


df = pd.read_csv('key_stats.csv')
df = df.iloc[:, :12]
df.fillna(0, inplace=True)
df = df.applymap(clean_cell)
df = pd.concat([df[['Ticker']], df.iloc[:, 1:].astype(float)], axis=1)
df.to_sql('key_stats', con=CONN, if_exists='replace')
CONN.close()