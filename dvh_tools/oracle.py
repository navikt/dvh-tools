import sys
import oracledb


class OracleUtils:
    def __init__(self, ORACLE_secrets):
        self.oracle_client = oracledb.create_pool(user=ORACLE_secrets.get('user', None),
                                                  password=ORACLE_secrets.get('password', None),
                                                  dsn=ORACLE_secrets.get('dsn', None), min=5, max=5, increment=1,
                                                  encoding="UTF-8")

    def oracle_connection_deactivate(self):
        '''
        Read: Closing oracle client
        '''
        try:
            self.oracle_client.close()
        except Exception:
            sys.exit()

    def sql_read(self, sql_query, prefetch_rows=1000, parameter='', variable_name=''):
        '''
        Read: Return results from a sql query. parameter is optional. prefetchrows in mandatory
        Example: sql_read(sql_query= "select * from table" , parameter= "2022")
        '''
        try:
            with self.oracle_client.acquire() as connection:
                with connection.cursor() as cursor:
                    cursor.prefetchrows = prefetch_rows;
                    cursor.arraysize = prefetch_rows + 1
                    cursor.execute(sql_query) if parameter else cursor.execute(sql_query.format(parameter))
                    print(f'({variable_name}:= Rows returned: {cursor.rowcount})')
                    return cursor.fetchall()
        except self.oracle_client as error:
            print(error)
            self.oracle_client.close()
            sys.exit()

    def sql_write(self, sql_query, val_dict, variable_name=''):
        '''
        Read: Insert data into tables using sql query.
            'Commit' auto after single sql_query.
        Example: sql_write(sql_query= sql_statement, val_dict= dataframe.to_dict(orient='records'))
        '''
        with self.oracle_client.acquire() as connection:
            with connection.cursor() as cursor:
                cursor.executemany(sql_query, val_dict, batcherrors=True, arraydmlrowcounts=False)
                # print(cursor.getarraydmlrowcounts())
                print(f'({variable_name}:= Rows inserted: {cursor.rowcount})')
                for error in cursor.getbatcherrors():
                    print("Error", error.message, "at row offset", error.offset)
                cursor.execute('commit')

    def sql_run(self, sql_query, variable_name=''):

        with self.oracle_client.acquire() as connection:
            with connection.cursor() as cursor:
                if isinstance(sql_query, list):
                    for _ in sql_query:
                        cursor.execute(_)
                        print(f'({variable_name}:= Rows affected: {cursor.rowcount})')
                    cursor.execute('commit')
                elif isinstance(sql_query, str):
                    cursor.execute(sql_query)
                    print(f'({variable_name}:= Rows affected: {cursor.rowcount})')
                    cursor.execute('commit')