import requests
from bs4 import BeautifulSoup
import re
from collections import defaultdict, namedtuple
from concurrent.futures import ThreadPoolExecutor
import pandas as pd



BASE_URL = 'https://www.sec.gov'

Purchase = namedtuple('Purchase', ('date', 'issuer', 'amount'))


url = 'https://www.sec.gov/cgi-bin/browse-edgar?company=&CIK=&type=13f&owner=include&count=10&action=getcurrent'
resp = requests.get(url)
soup = BeautifulSoup(resp.text, 'lxml')
links = [url.attrs['href'] for url in soup.find_all('a', {'href': re.compile(".*txt$")})]


url = f'{BASE_URL}{links[0]}'
resp = requests.get(url)
soup = BeautifulSoup(resp.text, 'lxml')
issuers = [x.text for x in soup.find_all('ns1:nameofissuer')]
values = [int(x.text) for x in soup.find_all('ns1:value')]

i = 0
links = []
while i < 500:
    url = f'{BASE_URL}/cgi-bin/browse-edgar?action=getcurrent&datea=&dateb=&company=&type=13f&SIC=&State=&Country=&CIK=&owner=include&accno=&start={i}&count={100}'
    resp = requests.get(url)
    soup = BeautifulSoup(resp.text, 'lxml')
    links += [url.attrs['href'] for url in soup.find_all('a', {'href': re.compile(".*txt$")})]
    i += 100


def fetct_url(url):
    url = BASE_URL + url
    resp = requests.get(url)
    return BeautifulSoup(resp.text, 'lxml')


def parse_soup(soup):
    period = soup.find('periodofreport').text
    data = defaultdict(int)
    issuers = [x.text for x in soup.find_all('ns1:nameofissuer')]
    cusips = [x.text for x in soup.find_all('ns1:cusip')]
    values = [int(x.text) for x in soup.find_all('ns1:value')]
    for issuer, value in zip(issuers, values):
        data[issuer] += value
    return period, data


with ThreadPoolExecutor(max_workers=30) as executor:
    futures = executor.map(fetct_url, links)
    results = [x for x in futures]

purchases = defaultdict(list)
for html in results:
    period, data = parse_soup(html)
    purchases[period].append(data)

data = defaultdict(int)
for p in purchases['12-31-2020']:
    for c, v in p.items():
        data[c] += v

series = pd.Series(data.values(), data.keys(), name='amount')
sorted = series.sort_values(ascending=False)[:20]

for stock in series.index[:20]:
    print(stock.replace('-', ' '))

cusip = '88579Y101'
url = 'https://quotes.fidelity.com/mmnet/SymLookup.phtml?reqforlookup=REQUESTFORLOOKUP&productid=mmnet&isLoggedIn=mmnet&rows=50&for=stock&by=cusip&criteria=88579Y101&submit=Search'
resp = requests.get(url)
soup = BeautifulSoup(resp.text, 'lxml')
ticker = soup.find(href=re.compile('.*QUOTE_TYPE=&SID_VALUE_ID=.*')).text

url = f'https://finviz.com/quote.ashx?t={ticker}'
resp = requests.get(url)
soup = BeautifulSoup(resp.text, 'lxml')

import pickle
with open('data.p', 'rb') as file:
    data = pickle.load(file)

url = 'https://www.sec.gov/edgar/search/#/category=custom&forms=13F-HR'
resp = requests.get(url)
soup = BeautifulSoup(resp.text, 'lxml')