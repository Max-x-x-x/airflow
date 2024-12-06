
import os

from time import time

import pandas as pd
from sqlalchemy import create_engine


def ingest_callable(user, password, host, port, db, table_name, file):

    print(user, password, host, port, db, table_name, file)
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engine.connect()

    print("connection established, inserting data...")

    t_start = time()

    if file.endswith(".parquet"):
        df = pd.read_parquet(file)
    else:
        df = pd.read_csv(file)
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    df.head(n=10).to_sql(name=table_name, con=engine, if_exists='append')

    t_end = time()

    print('inserted, took %.3f second' % (t_end - t_start))

    # t_start = time()
    # df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)

    # df = next(df_iter)

    # df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    # df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    # df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    # df.to_sql(name=table_name, con=engine, if_exists='append')

    # t_end = time()

    # print('inserted, took %.3f second' % (t_end - t_start))
    
    # while True: 

    #     try:
    #         t_start = time()
            
    #         df = next(df_iter)

    #         df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    #         df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    #         df.to_sql(name=table_name, con=engine, if_exists='append')

    #         t_end = time()

    #         print('inserted another chunk, took %.3f second' % (t_end - t_start))

    #     except StopIteration:
    #         print("Finished ingesting data into the postgres database")
    #         break
