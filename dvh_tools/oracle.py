import oracledb

class OracleUtils:
    def __init__(self, ORACLE_secrets):
        self.oracle_client = oracledb.connect(user=ORACLE_secrets.get('user', None),
                                                  password=ORACLE_secrets.get('password', None),
                                                  dsn=ORACLE_secrets.get('dsn', None))

    def sql_read(self, sql_query, prefetch_rows=1000, parameter='', variable_name=''):
        '''
        Read: Return results from a sql query. parameter is optional. prefetchrows in mandatory
        Example: sql_read(sql_query= "select * from table" , parameter= "2022")
        '''

        with self.oracle_client.cursor() as cursor:
            cursor.prefetchrows = prefetch_rows
            cursor.arraysize = prefetch_rows + 1
            cursor.execute(sql_query) if parameter else cursor.execute(sql_query.format(parameter))
            print(f'({variable_name}:= Rows returned: {cursor.rowcount})')
            return cursor.fetchall()


    def sql_run(self, sql_query, variable_name=''):

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