import yfinance
import asyncio
import aiohttp
from sec.sql import create_connection
import pandas as pd
import tqdm
import time
from concurrent.futures import ThreadPoolExecutor

CONN = create_connection('sec.db')


def fetch(ticker, pbar=None):
    try:
        info = yfinance.Ticker(ticker).info
        if pbar:
            pbar.update(1)
        return ticker, info
    except KeyError:
        if pbar:
            pbar.update(1)
        return ticker, None


async def fetch_all(tickers, pbar):
    tasks = [asyncio.create_task(fetch(ticker, pbar)) for ticker in tickers]
    responses = await asyncio.gather(*tasks)
    return responses

tickers = pd.read_sql('select ticker from top_1000', con=CONN)['ticker'].tolist()
CONN.close()
tickers = list(set(tickers))

# data = {}
# for ticker in tqdm.tqdm(tickers):  # async keeps hitting limit
#     try:
#         data[ticker] = yfinance.Ticker(ticker).info
#     except Exception as e:
#         print(e)
#         data[ticker] = None


# pbar = tqdm.tqdm(total=len(tickers))
# response = asyncio.run(fetch_all(tickers, pbar))
# pbar.close()


# pbar = tqdm.tqdm(total=len(tickers))
# with ThreadPoolExecutor(max_workers=5) as exc:
#     futures = []
#     for ticker in tickers:
#         futures.append(exc.submit(fetch, ticker, pbar))
#     response = [x.result() for x in futures]
# pbar.close()


# i = 0
# batch = 10
# data = []
# pbar = tqdm.tqdm(total=len(tickers))
# while i < len(tickers):
#     with ThreadPoolExecutor() as exc:
#         futures = exc.map(fetch, tickers[i:i+batch])
#         data += [*futures]
#     i += batch
#     pbar.update(batch)
# pbar.close()