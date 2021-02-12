import pathlib

HERE = pathlib.Path(__file__).parent.absolute()
FILES = HERE.parent.glob('tickers/*.txt')


class Tickers:
    FILES = ['tickers/AMEX.txt', 'tickers/NASDAQ.txt', 'tickers/NYSE.txt', 'tickers/OTCBB.txt']

    def __init__(self, files=None):
        self.files = files if files else self.FILES

    @property
    def data(self):
        data = {}
        for file in self.files:
            with open(file, 'r') as fp:
                lines = [line.split('\t') for line in fp.readlines()[1:]]
                data.update({a.strip(): b.strip() for a, b in lines})
        return data

    @property
    def tickers(self):
        return {company: ticker for (ticker, company) in self.data.items()}

    @property
    def companies(self):
        return {ticker: company for (ticker, company) in self.data.items()}

    @property
    def names(self):
        return list(self.tickers.keys())

    def get_ticker(self, company):
        return self.tickers.get(company)

    def get_company(self, ticker):
        return self.companies.get(ticker)


if __name__ == '__main__':
    files = ['tickers/AMEX.txt', 'tickers/NASDAQ.txt', 'tickers/NYSE.txt', 'tickers/OTCBB.txt']
    tickers = Tickers(files)
