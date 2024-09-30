import oracledb
import os
import pandas as pd


def _create_connection(secret = None):
    """Creates and returns an Oracle database connection.

    This function establishes a connection to an Oracle database using credentials and DSN 
    either provided through environment variables or a secret dictionary. If the `secret` 
    argument is provided, its values are used; otherwise, environment variables are used.

    Args:
        secret (dict, optional): A dictionary containing the database credentials and DSN.
            Expected keys are "DB_USER", "DB_PASSWORD", and "DB_DSN".

    Returns:
        oracle_client (oracledb.Connection): An Oracle database connection object.

    Examples:
        >>> secret = {"DB_USER": "username", "DB_PASSWORD": "password", "DB_DSN": "dsn"}
        >>> conn = _create_connection(secret)
    """
    if secret:
        oracle_client = oracledb.connect(
            user=os.environ.get("DBT_ENV_SECRET_USER", secret.get("DB_USER", None)),
            password=os.environ.get("DBT_ENV_SECRET_PASS", secret.get("DB_PASSWORD", None)),
            dsn=secret.get("DB_DSN", None),
        )
    else:
        oracle_client = oracledb.connect(
            user=os.environ.get("DB_USER", None),
            password=os.environ.get("DB_PASSWORD", None),
            dsn=os.environ.get("DB_DSN", None),
    )
    return oracle_client


def db_sql_run(sql_query, secret) -> None:
    """Executes an SQL query against the database.

    This function connects to the Oracle database using the provided secret dictionary, 
    executes the given SQL query, and commits the transaction.

    Args:
        sql_query (str): The SQL query to execute.
        secret (dict): A dictionary containing the database credentials and DSN.
            Expected keys are "DB_USER", "DB_PASSWORD", and "DB_DSN".

    Examples:
        >>> sql_query = "UPDATE my_table SET my_column = 'value' WHERE id = 1"
        >>> secret = {"DB_USER": "username", "DB_PASSWORD": "password", "DB_DSN": "dsn"}
        >>> db_sql_run(sql_query, secret)
    """
    oracle_client = _create_connection(secret)
    with oracle_client.cursor() as cursor:
        cursor.execute(sql_query)
        cursor.execute('commit')


def db_read_to_df(sql_query, secret = None, prefetch_rows = 1000):
    """Executes an SQL query and returns the result as a pandas DataFrame.

    This function connects to the Oracle database using the provided secret dictionary or 
    environment variables, executes the SQL query, and returns the results as a DataFrame.

    Args:
        sql_query (str): The SQL query to execute.
        secret (dict, optional): A dictionary containing the database credentials and DSN.
            Expected keys are "DB_USER", "DB_PASSWORD", and "DB_DSN".
        prefetch_rows (int, optional): Number of rows to prefetch for the cursor. Defaults to 1000.

    Returns:
        df (pd.DataFrame): A DataFrame containing the query results, with columns named after 
        the SQL query's result columns.

    Examples:
        >>> sql_query = "SELECT * FROM my_table"
        >>> secret = {"DB_USER": "username", "DB_PASSWORD": "password", "DB_DSN": "dsn"}
        >>> df = db_read_to_df(sql_query, secret)
        >>> print(df.head())
    """
    oracle_client = _create_connection(secret)
    with oracle_client.cursor() as cursor:
        cursor.prefetchrows = prefetch_rows
        cursor.arraysize = prefetch_rows + 1
        cursor.execute(sql_query)
        rows = cursor.fetchall()
        cols = [col[0].lower() for col in cursor.description]
        df = pd.DataFrame(rows, columns=cols)
        return df


def sql_df_to_db(sql_query, secret, val_dict) -> None:
    """Inserts data into the database from a list of tuples using an SQL query.

    This function connects to the Oracle database using the provided secret dictionary,
    executes the SQL query with the given data, and commits the transaction. The data is
    inserted in batches, and errors during batch processing are reported.

    Args:
        sql_query (str): The SQL query for inserting data.
        secret (dict): A dictionary containing the database credentials and DSN.
            Expected keys are "DB_USER", "DB_PASSWORD", and "DB_DSN".
        val_dict (list[tuple]): A list of tuples containing the data to insert.

    Examples:
        >>> sql_query = "INSERT INTO my_table (column1, column2) VALUES (:1, :2)"
        >>> secret = {"DB_USER": "username", "DB_PASSWORD": "password", "DB_DSN": "dsn"}
        >>> val_dict = [("value1", "value2"), ("value3", "value4")]
        >>> sql_df_to_db(sql_query, secret, val_dict)
    """
    oracle_client = _create_connection(secret)
    with oracle_client.cursor() as cursor:
        cursor.executemany(sql_query, val_dict, batcherrors=True, arraydmlrowcounts = False)
        print(f'cursor rowcount: {cursor.rowcount}')
        for error in cursor.getbatcherrors():
            print("Error", error.message, "at row offset", error.offset)
        cursor.execute('commit')
