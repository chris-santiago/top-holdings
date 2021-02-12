import aiohttp
import asyncio
import pandas as pd
import json

URL = 'https://yahoo-finance-low-latency.p.rapidapi.com/v6/finance/autocomplete'
HEADERS = {
    'x-rapidapi-key': "a9f14ee177mshde145bc2ef518aap11e8b7jsn33f1e888c4f1",
    'x-rapidapi-host': "yahoo-finance-low-latency.p.rapidapi.com"
    }
SEARCH = pd.read_csv('search.csv').iloc[:, 0].apply(lambda x: x.strip()).tolist()


async def fetch(company, client):
    payload = {'query': company, 'lang': 'en'}
    payload2 = {'query': ' '.join(company.split(' ')[:2]), 'lang': 'en'}
    try:
        async with client.get(
            URL,
            headers=HEADERS,
            params=payload
        ) as resp:
            response = await resp.json()
            if len(response['ResultSet']['Result']) == 0:
                try:
                    async with client.get(
                            URL,
                            headers=HEADERS,
                            params=payload
                    ) as resp:
                        response = await resp.json()
                except Exception:
                    return None
            return response
    except Exception:
        try:
            async with client.get(
                    URL,
                    headers=HEADERS,
                    params=payload2
            ) as resp:
                return await resp.json()
        except Exception:
            return None


async def fetch_all(companies, loop):
    connector = aiohttp.TCPConnector(limit=10)
    async with aiohttp.ClientSession(connector=connector, loop=loop) as client:
        tasks = [asyncio.create_task(fetch(company, client)) for company in companies]
        return await asyncio.gather(*tasks, return_exceptions=True)


loop = asyncio.get_event_loop()
r = loop.run_until_complete(fetch_all(SEARCH, loop))
with open('yahoo_autocomplete.json', 'w') as fp:
    json.dump(r, fp)

tickers = []
for item in r:
    try:
        tickers.append(item['ResultSet']['Result'][0]['symbol'])
    except IndexError:
        tickers.append(None)
maps = pd.DataFrame({'company': top_25k.index, 'search': SEARCH, 'ticker': tickers})
maps['ticker3'] = maps['company'].apply(lambda x: ticker_map2.get(x))
maps = maps.dropna(how='all', thresh=3)
maps['check'] = maps.apply(lambda x: len(set([x['ticker'], x['ticker2'], x['ticker3']])) == 1, axis=1)

maps.to_csv('ticker_maps.csv', index=False)