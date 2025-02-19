import pyarrow.parquet as pq
from sqlalchemy import create_engine
from tqdm import tqdm

import argparse
import os

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.database
    table_name = params.table_name
    url = params.url
    downloaded_filename = 'downloaded_data.parquet'

    #download data file
    os.system(f"wget {url} -O {downloaded_filename}")

    #create Postgres engine
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    #insert into Postgres in chunks
    parquet_file = pq.ParquetFile(f'{downloaded_filename}')
    pq_iter = parquet_file.iter_batches(batch_size=1000)
    for chunk in tqdm(pq_iter):
        chunk.to_pandas().to_sql(name=table_name, con=engine, if_exists='append')


if __name__ == '__main__':
    
    parser = argparse.ArgumentParser(description='Ingest parquet data to Postgres')

    parser.add_argument('--user', help='username for Postgres')
    parser.add_argument('--password', help='password for Postgres')
    parser.add_argument('--host', help='hostname for Postgres')
    parser.add_argument('--port', help='port for Postgres')
    parser.add_argument('--database', help='database name for Postgres')
    parser.add_argument('--table_name', help='name of the table to write results to')
    parser.add_argument('--url', help='url of parquet file to ingest')

    main(parser.parse_args())