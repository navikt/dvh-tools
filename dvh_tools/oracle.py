import oracledb

class OracleUtils:
    def __init__(self, ORACLE_secrets):
        self.oracle_client = oracledb.connect(user=ORACLE_secrets.get('user', None),
                                                  password=ORACLE_secrets.get('password', None),
                                                  dsn=ORACLE_secrets.get('dsn', None))

    def sql_read(self, sql_query, prefetch_rows=1000, parameter='', variable_name=''):
        '''
        Reads data from an sql query: Return results from a sql query. parameter is optional. prefetchrows in mandatory
        Example: sql_read(sql_query= "select * from table" , parameter= "2022")
        '''
        with self.oracle_client.cursor() as cursor:
            cursor.prefetchrows = prefetch_rows
            cursor.arraysize = prefetch_rows + 1
            cursor.execute(sql_query) if parameter else cursor.execute(sql_query.format(parameter))
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

