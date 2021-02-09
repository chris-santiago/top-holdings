from bs4 import BeautifulSoup
import pandas as pd
from tqdm import tqdm

from sec.sql import create_connection
import pathlib
from concurrent.futures import ProcessPoolExecutor

FILES = list(pathlib.Path('filings').rglob('*.txt'))
CONN = create_connection('sec.db')


def open_file(file):
    with open(file, 'r') as fp:
        return BeautifulSoup(fp.read(), 'lxml')


def parse_soup(soup):
    period = soup.find(['periodofreport', 'periodOfReport']).text
    filer = soup.find('name').text if soup.find('name').text else 'None'
    issuers = [x.text for x in soup.find_all(['nameofissuer', 'ns1:nameofissuer'])]
    values = [float(x.text) for x in soup.find_all(['value', 'ns1:value'])]
    df = pd.DataFrame({'report_pd': period, 'filer': filer, 'company': issuers, 'value': values})
    df.to_sql('holdings', CONN, if_exists='append', index=False)


def process(file):
    return parse_soup(open_file(file))


def main(files, chunksize=100):
    with ProcessPoolExecutor(max_workers=3) as executor:
        results = list(tqdm(executor.map(process, files, chunksize=chunksize), total=len(files)))


if __name__ == '__main__':
    main(FILES, 1000)
    CONN.close()
