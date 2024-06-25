import oracledb
import os
import pandas as pd

def _create_connection(secret):
    oracle_client = oracledb.connect(
        user=os.environ.get("DBT_ENV_SECRET_USER", secret.get("DB_USER", None)),
        password=os.environ.get("DBT_ENV_SECRET_PASS", secret.get("DB_PASSWORD", None)),
        dsn=secret.get("DB_DSN", None),
    )
    return oracle_client

def db_sql_run(sql_query, secret):
    oracle_client = _create_connection(secret)
    with oracle_client.cursor() as cursor:
        cursor.execute(sql_query)
        cursor.execute('commit')


def db_read_to_df(sql_query, secret, prefetch_rows = 1000):
    '''Function that returns the result of a sql query and the columns.
    '''
    oracle_client = _create_connection(secret)
    with oracle_client.cursor() as cursor:
        cursor.prefetchrows = prefetch_rows
        cursor.arraysize = prefetch_rows + 1
        cursor.execute(sql_query)
        rows = cursor.fetchall()
        cols = [col[0].lower() for col in cursor.description]
        return pd.DataFrame(rows, columns=cols)

def sql_df_to_db(sql_query, secret, val_dict):
    '''Insert data into database from a dataframe using sql query.
    '''
    oracle_client = _create_connection(secret)
    with oracle_client.cursor() as cursor:
        cursor.executemany(sql_query, val_dict, batcherrors=True, arraydmlrowcounts = False)
        print(f'cursor rowcount: {cursor.rowcount})')
        for error in cursor.getbatcherrors():
            print("Error", error.message, "at row offset", error.offset)
        cursor.execute('commit')

