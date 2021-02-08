from bs4 import BeautifulSoup
import pandas as pd
from sec.sql import create_connection, execute_query

file = '0000007195-21-000001.txt'


def parse_filing(filing):
    with open(filing, 'r') as fp:
        soup = BeautifulSoup(fp, 'lxml')
    period = soup.find('periodofreport').text
    filer = soup.find('name').text
    issuers = [x.text for x in soup.find_all(['nameofissuer', 'ns1:nameofissuer'])]
    values = [int(x.text) for x in soup.find_all(['value', 'ns1:value'])]
    return pd.DataFrame(
        {'report_pd': period, 'filer': filer, 'company': issuers, 'value': values}
    )


df = parse_filing(file)
conn = create_connection('sec.db')
query = """
    create table fund_holdings (
    report_pd TEXT,
    filer TEXT,
    company TEXT,
    value INT
    );
    """
execute_query(conn, query)
df.to_sql('fund_holdings', conn, if_exists='append', index=False, method='multi')
