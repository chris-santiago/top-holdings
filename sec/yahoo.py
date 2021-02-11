import yfinance
from sec.sql import create_connection
import pandas as pd
import tqdm
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.exceptions import HTTPError
import json

CONN = create_connection('sec.db')
TICKERS = list(set(pd.read_sql('select ticker from top_1000', con=CONN)['ticker']))


def fetch(ticker, pbar=None):
    try:
        info = yfinance.Ticker(ticker).info
        if pbar:
            pbar.update(1)
        return ticker, info
    except HTTPError:
        time.sleep(10)
        try:
            info = yfinance.Ticker(ticker).info
            if pbar:
                pbar.update(1)
            return ticker, info
        except Exception:
            if pbar:
                pbar.update(1)
            return ticker, None
    except Exception:
        if pbar:
            pbar.update(1)
        return ticker, None



def fetch_all(tickers, batch_size=5):
    data = []
    i = 0
    pbar = tqdm.tqdm(total=len(tickers))
    while i < len(tickers):
        with ThreadPoolExecutor(max_workers=5) as exc:
            batch = tickers[i:i+batch_size]
            futures = [exc.submit(fetch, ticker) for ticker in batch]
            data += [x.result() for x in as_completed(futures)]
        i += batch_size
        time.sleep(5)
        pbar.update(batch_size)
    pbar.close()
    return data


ticker_info = fetch_all(TICKERS, batch_size=10)
d = {x[0]: x[1] for x in ticker_info if x[1]}
with open('yahoo_stats.json', 'w') as fp:
    json.dump(d, fp)

with open('yahoo_stats.json', 'r') as fp:
    d = json.load(fp)

out = pd.DataFrame(d).T
out = out[
    ['symbol', 'shortName', 'sector', 'industry', 'beta', 'averageVolume', 'pegRatio', 'shortRatio', 'heldPercentInsiders',
     'enterpriseToEbitda', 'profitMargins', 'priceToSalesTrailing12Months']
]
out.iloc[:, :4].fillna('None', inplace=True)
out.iloc[:, 4:].fillna(0, inplace=True)
out.to_sql('yahoo_stats', CONN, if_exists='replace', index=False)
CONN.close()
