from bs4 import BeautifulSoup
import pandas as pd
from tqdm import tqdm

from sec.sql import create_connection
import pathlib
from concurrent.futures import ProcessPoolExecutor

FILES = list(pathlib.Path('filings').rglob('*.txt'))
CONN = create_connection('sec.db')


class EdgarParser:
    def __init__(self, files, chunk_size=100):
        self.files = files
        self.chunk = chunk_size
        self.conn = create_connection('sec.db')

    @staticmethod
    def open_file(file):
        with open(file, 'r') as fp:
            return BeautifulSoup(fp.read(), 'lxml')

    def parse_soup(self, soup):
        data = {
            'report_pd': soup.find(['periodofreport', 'periodOfReport']).text,
            'filer': soup.find('name').text if soup.find('name').text else 'None',
            'issuers': [x.text for x in soup.find_all(['nameofissuer', 'ns1:nameofissuer'])],
            'values': [float(x.text) for x in soup.find_all(['value', 'ns1:value'])]
        }
        pd.DataFrame(data).to_sql('holdings', con=self.conn, if_exists='append', index=False)

    def process(self, file):
        return self.parse_soup(self.open_file(file))

    def run(self):
        with ProcessPoolExecutor(max_workers=3) as executor:
            results = list(
                tqdm(
                    executor.map(self.process, self.files, chunksize=self.chunk),
                    total=len(self.files)
                )
            )


if __name__ == '__main__':
    parser = EdgarParser(FILES)
    parser.run()
    parser.conn.close()
