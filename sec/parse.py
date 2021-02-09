from bs4 import BeautifulSoup
import pandas as pd
from sec.sql import create_connection
import tqdm
import pathlib
import asyncio
import aiofiles


async def open_file(file, queue):
    async with aiofiles.open(file, 'r') as fp:
        contents = await fp.read()
        await queue.put(BeautifulSoup(contents, 'lxml'))


async def parse_soup(queue, completed):
    while True:
        soup = await queue.get()
        period = soup.find(['periodofreport', 'periodOfReport']).text
        filer = soup.find('name').text if soup.find('name').text else 'None'
        issuers = [x.text for x in soup.find_all(['nameofissuer', 'ns1:nameofissuer'])]
        values = [float(x.text) for x in soup.find_all(['value', 'ns1:value'])]
        df = pd.DataFrame(
                {'report_pd': period, 'filer': filer, 'company': issuers, 'value': values}
            )
        completed.append(df)
        queue.task_done()


async def run_batch(files, conn):
    data = []
    n_tasks = range(len(files))
    queue = asyncio.Queue()
    # queue.put_nowait(files)

    open_tasks = [asyncio.create_task(open_file(file, queue)) for file in files]
    parse_tasks = [asyncio.create_task(parse_soup(queue, data)) for i in n_tasks]

    await asyncio.gather(*open_tasks)
    await queue.join()

    for task in parse_tasks:
        task.cancel()
    out = pd.concat(data).reset_index(drop=True)
    try:
        out.to_sql('holdings', conn, if_exists='append', index=False, chunksize=10000)
    except Exception as e:
        print(e)


def main():
    files = list(pathlib.Path('filings').rglob('*.txt'))
    conn = create_connection('sec.db')
    i = 0
    batch = 1000
    pbar = tqdm.tqdm(total=len(files)+1)
    while i < len(files):
        asyncio.run(run_batch(files[i:i+batch], conn))
        i += batch
        pbar.update(batch)
    pbar.close()
    conn.close()


main()


# def parse_filing(filing):
#     with open(filing, 'r') as fp:
#         soup = BeautifulSoup(fp, 'lxml')
#     period = soup.find('periodofreport').text
#     filer = soup.find('name').text
#     issuers = [x.text for x in soup.find_all(['nameofissuer', 'ns1:nameofissuer'])]
#     values = [float(x.text) for x in soup.find_all(['value', 'ns1:value'])]
#     return pd.DataFrame(
#         {'report_pd': period, 'filer': filer, 'company': issuers, 'value': values}
#     )
#
#
# def load_files(files):
#     for file in files:
#         yield parse_filing(file)
#
#
# files = list(pathlib.Path('filings').rglob('*.txt'))
# conn = create_connection('sec.db')
#
# i = 0
# batch = 100
# pbar = tqdm.tqdm(total=len(files)+1)
# while i < len(files):
#     try:
#         data = pd.concat(load_files(list(files)[i:i+batch]))
#         data.to_sql('holdings', conn, if_exists='append', index=False)
#     except Exception as e:
#         print(e)
#     finally:
#         i += batch
#         pbar.update(batch)
# pbar.close()
# conn.close()
#
#
# def main():
#     data = []
#     files = pathlib.Path('filings').rglob('*.txt')
#     with ThreadPoolExecutor(max_workers=64) as executor:
#         futures = executor.map(parse_filing, list(files))
#         data = [x for x in futures]
#     # for file in tqdm.tqdm(list(files)):
#     #     try:
#     #         data.append(parse_filing(file))
#     #     except Exception as e:
#     #         print(e)
#     #         continue
#     out = pd.concat(data)
#     # conn = create_connection('sec.db')
#     # out.to_sql('fund_holdings', conn, if_exists='append', index=False, method='multi')
#     # conn.close()
#
#
# if __name__ == '__main__':
#     main()
