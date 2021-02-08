import json

with open('company_tickers.json', 'r') as fp:
    tickers = json.load(fp)

TICKERS = {}
for value in tickers.values():
    TICKERS[value['title']] = value['ticker']
