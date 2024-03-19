
import pyarrow.parquet as pq
import pandas as pd
from sqlalchemy import create_engine
import argparse

import os
from time import time
import argparse


def main(params):

    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url

    csv_name = 'output.parquet'

    # download the csv
    os.system(f"wget {url} -O {csv_name}")

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engine.connect()

    df = pd.read_parquet(csv_name)
    
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    parquet_file = pq.ParquetFile(csv_name)

    for batch in parquet_file.iter_batches(batch_size=100000):
        t_start = time()
        
        print("RecordBatch")
        batch_df = batch.to_pandas()
        batch_df.to_sql(name=table_name, con=engine, if_exists='append')
        
        t_end = time()
        print("inserted another batch, took %.3f second:"% (t_end - t_start))


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Ingest Parquet data to Postgres.')

    # user, password, host, port, database name, table name, url of csv
    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='name of table where we will write results')
    parser.add_argument('--url', help='url of the csv file')

    args = parser.parse_args()
    main(args)