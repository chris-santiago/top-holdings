from secedgar.filings import MasterFilings
from secedgar.client import NetworkClient


def get_13f(filing_entry):
    return filing_entry.form_type.lower() == '13f-hr'


client = NetworkClient(
    batch_size=10,
    rate_limit=5
)

years = [2019, 2020]
quarters = list(range(1, 5))

for year in years:
    for quarter in quarters:
        print(f'Fetching {year}-{quarter}...')
        filing = MasterFilings(
            client=client,
            year=year,
            quarter=quarter,
            entry_filter=get_13f
        )
        filing.save('filings')
