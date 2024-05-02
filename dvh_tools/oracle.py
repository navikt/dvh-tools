import oracledb
import os

def _create_connection(secret):
    oracle_client = oracledb.connect(
        user=os.environ.get("USER", secret.get("DB_USER", None)),
        password=os.environ.get("PASSWORD", secret.get("DB_PASSWORD", None)),
        dsn=secret.get("DB_DSN", None),
    )
    return oracle_client

def db_sql_run(sql_query, secret):
    oracle_client = _create_connection(secret)
    with oracle_client.cursor() as cursor:
        cursor.execute(sql_query)
        cursor.execute('commit')

def db_read_to_df(sql_query, secret, prefetch_rows = 1000, print_info=True):
    '''Function that returns the result of a sql query and the columns.
    Example:
        rows, cols = db_read_to_df(...)
        df = pd.DataFrame(rows, columns=cols)
    Example 2, only the data:
        data, _ = db_read_to_df(...)'''
    oracle_client = _create_connection(secret)
    with oracle_client.cursor() as cursor:
        cursor.prefetchrows = prefetch_rows
        cursor.arraysize = prefetch_rows + 1
        cursor.execute(sql_query)
        if print_info:
            print(f'cursor rowcount: {cursor.rowcount})')
        rows = cursor.fetchall()
        cols = [col[0].lower() for col in cursor.description]
        return rows, cols

def sql_df_to_db(sql_query, secret, val_dict):
    '''Insert data into database from a dataframe using sql query.
    Example: sql_write(sql_query= sql_statement, val_dict= dataframe.to_dict(orient='records'))
    '''
    oracle_client = _create_connection(secret)
    with oracle_client.cursor() as cursor:
        cursor.executemany(sql_query, val_dict, batcherrors=True, arraydmlrowcounts = False)
        print(f'cursor rowcount: {cursor.rowcount})')
        for error in cursor.getbatcherrors():
            print("Error", error.message, "at row offset", error.offset)
        cursor.execute('commit')

#skal fases ut innen slutten av mai
class OracleUtils:
    def __init__(self, ORACLE_secrets):
        self.oracle_client = oracledb.connect(user=ORACLE_secrets.get('user', None),
                                                  password=ORACLE_secrets.get('password', None),
                                                  dsn=ORACLE_secrets.get('dsn', None))

    def sql_read(self, sql_query, prefetch_rows=1000, parameter='', variable_name='', print_info=True):
        '''
        Reads data from an sql query: Return results from a sql query. parameter is optional. prefetchrows in mandatory
        Example: sql_read(sql_query= "select * from table" , parameter= "2022")
        '''
        with self.oracle_client.cursor() as cursor:
            cursor.prefetchrows = prefetch_rows
            cursor.arraysize = prefetch_rows + 1
            cursor.execute(sql_query) if parameter else cursor.execute(sql_query.format(parameter))
            if print_info:
                print(f'({variable_name}:= Rows returned: {cursor.rowcount})')
            return cursor.fetchall()


    def sql_run(self, sql_query, variable_name=''):
        '''
        Runs a sql-query in the database, but does not return data. Can take a list of queries or a single instance.
        '''
        with self.oracle_client.cursor() as cursor:
            if isinstance(sql_query, list):
                for _ in sql_query:
                    cursor.execute(_)
                    print(f'({variable_name}:= Rows affected: {cursor.rowcount})')
                cursor.execute('commit')
            elif isinstance(sql_query, str):
                cursor.execute(sql_query)
                print(f'({variable_name}:= Rows affected: {cursor.rowcount})')
                cursor.execute('commit')

    def sql_write(self, sql_query, val_dict, variable_name=''):
        '''
        Read: Insert data into tables using sql query.
            'Commit' auto after single sql_query.
        Example: sql_write(sql_query= sql_statement, val_dict= dataframe.to_dict(orient='records'))
        '''
        with self.oracle_client.cursor() as cursor:
            cursor.executemany(sql_query, val_dict, batcherrors=True, arraydmlrowcounts = False)
            print(f'({variable_name}:= Rows inserted: {cursor.rowcount})')
            for error in cursor.getbatcherrors():
                print("Error", error.message, "at row offset", error.offset)
            cursor.execute('commit')

    def close_con(self):
        self.oracle_client.close()

