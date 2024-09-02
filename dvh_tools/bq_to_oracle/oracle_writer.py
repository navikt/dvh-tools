import json
from oracledb import connect


class OracleWriter:
    """Handles writing data batches to an Oracle database.

    This class manages the connection to an Oracle database and provides methods to write data
    batches to a specified table. It includes functionality for batch insertion, handling list and
    dictionary conversions to JSON, and managing database transactions.

    Attributes:
        con: Database connection object.
        target_table: The table in the Oracle database where data will be inserted.
        insert_string: SQL insert statement template.
        total_rows_inserted: Total number of rows inserted so far.
        execution_time: Timestamp of the current execution.

    Args:
        config (dict): Configuration dictionary with keys for database user, password, and DSN.
        target_table (Optional[str], optional): The target table in Oracle where data will be written.
            Defaults to the value provided in `config` under `"target-table"`.

    Methods:
        write_batch(batch: list, convert_lists: bool = False, datatypes: dict = {}):
            Inserts a batch of data into the target table, with optional list conversion and datatype settings.
        cleanup(is_healthy: bool = True):
            Commits or rolls back the current transaction and closes the database connection.
        prepare_table():
            Truncates the target table to prepare it for new data.
        get_oracle_sysdate():
            Retrieves the current system date from the Oracle database.
        create_insert_string(batch: list):
            Creates the SQL insert statement template based on the batch's column names.
        convert_lists_and_dicts_in_batch_to_json(batch: list):
            Converts lists and dictionaries within the batch to JSON strings.
        add_execution_time_to_batch(time, batch: list):
            Adds a timestamp of the current execution to each item in the batch.

    Examples:
        >>> config = {
        ...     "DB_USER": "user",
        ...     "DB_PASSWORD": "password",
        ...     "DB_DSN": "dsn",
        ...     "target-table": "my_table"
        ... }
        >>> writer = OracleWriter(config)
        >>> batch = [
        ...     {"pk": 1, "data": ["value1"], "metadata": {"key": "value"}},
        ...     {"pk": 2, "data": ["value2"], "metadata": {"key": "value2"}}
        ... ]
        >>> writer.write_batch(batch, convert_lists=True)
        >>> writer.cleanup()
    """
    def __init__(self, config, target_table=None):
        self.__config = config
        self.con = connect(
            user=self.__config["DB_USER"],
            password=self.__config["DB_PASSWORD"],
            dsn=self.__config["DB_DSN"],
        )
        self.target_table = target_table or self.__config["target-table"]
        self.insert_string = None
        self.total_rows_inserted = 0
        self.execution_time = self.get_oracle_sysdate()


    def write_batch(self, batch, convert_lists=False, datatypes={}) -> None:
        """Inserts a batch of data into the target table.

        If `convert_lists` is True, it converts lists and dictionaries in the batch to JSON strings.
        Adds an execution timestamp to each record in the batch before insertion.

        Args:
            batch (list): List of dictionaries representing the batch of data to insert.
            convert_lists (bool, optional): Whether to convert lists and dictionaries to JSON strings. Defaults to False.
            datatypes (dict, optional): Optional dictionary mapping column names to data types. Defaults to {}.

        Examples:
            >>> batch = [
            ...     {"pk": 1, "data": ["value1"], "metadata": {"key": "value"}},
            ...     {"pk": 2, "data": ["value2"], "metadata": {"key": "value2"}}
            ... ]
            >>> writer = OracleWriter(config)
            >>> writer.write_batch(batch, convert_lists=True)
        """
        if self.total_rows_inserted == 0:
            # self.prepare_table()
            pass
        if convert_lists:
            self.convert_lists_and_dicts_in_batch_to_json(batch)

        # Add execution timestamp lastet tid
        self.add_execution_time_to_batch(self.execution_time, batch)

        if not self.insert_string:
            self.create_insert_string(batch)
        with self.con.cursor() as cursor:
            try:
                if datatypes:
                    cursor.setinputsizes(**datatypes)
                cursor.executemany(self.insert_string, batch)
                self.total_rows_inserted += cursor.rowcount
            except Exception as e:
                self.cleanup(is_healthy=False)
                print(e)
                raise RuntimeError(e)
        self.con.commit()


    def cleanup(self, is_healthy=True) -> None:
        """Commits or rolls back the current transaction and closes the connection.

        Args:
            is_healthy (bool, optional): If True, commits the transaction; otherwise, rolls it back. Defaults to True.

        """
        if is_healthy:
            self.con.commit()
        else:
            self.con.rollback()
        self.con.close()


    def prepare_table(self):
        """Truncates the target table to prepare it for new data.

        Returns:
            bool: True if the table was successfully truncated.
        """
        with self.con.cursor() as cursor:
            cursor.execute(f"truncate table {self.target_table}")
        return True


    def get_oracle_sysdate(self):
        """Retrieves the current system date from the Oracle database.

        Returns:
            datetime.datetime: The current system date.
        """
        with self.con.cursor() as cursor:
            cursor.execute(f"select sysdate from dual")
            row = cursor.fetchone()
        return row[0]


    def create_insert_string(self, batch):
        """Creates the SQL insert statement template based on the batch's column names.

        Args:
            batch (list): List of dictionaries representing the batch of data.

        Returns:
            str: The SQL insert statement template.
        """
        column_names = batch[0].keys()
        self.insert_string = f"""
        insert into {self.target_table}
        ({', '.join(column_names)}) 
        values({', '.join([f':{col}' for col in column_names])})
        """
        return self.insert_string


    @staticmethod
    def convert_lists_and_dicts_in_batch_to_json(batch: list) -> None:
        """Converts lists and dictionaries within the batch to JSON strings.

        Args:
            batch (list): List of dictionaries representing the batch of data.

        Examples:
            >>> batch = [
            ...     {"pk": 1, "data": ["value1"], "metadata": {"key": "value"}},
            ...     {"pk": 2, "data": ["value2"], "metadata": {"key": "value2"}}
            ... ]
            >>> OracleWriter.convert_lists_and_dicts_in_batch_to_json(batch)
            >>> print(batch)
            [{'pk': 1, 'data': '["value1"]', 'metadata': '{"key": "value"}'},
             {'pk': 2, 'data': '["value2"]', 'metadata': '{"key": "value2"}'}]
        """
        for i, ele in enumerate(batch):
            for key in ele:
                if isinstance(ele[key], list):
                    ele[key] = json.dumps(ele[key])
                if isinstance(ele[key], dict):
                    ele[key] = json.dumps(ele[key])
            batch[i] = ele


    @staticmethod
    def add_execution_time_to_batch(time, batch: list) -> None:
        """Adds an execution timestamp to each item in the batch.

        Args:
            time (datetime.datetime): The timestamp of the current execution.
            batch (list): List of dictionaries representing the batch of data.

        Examples:
            >>> batch = [{"pk": 1}, {"pk": 2}]
            >>> OracleWriter.add_execution_time_to_batch(datetime.datetime.now(), batch)
            >>> print(batch)
            [{'pk': 1, 'lastet_tid': datetime.datetime(2024, 8, 30, 12, 0, 0)},
             {'pk': 2, 'lastet_tid': datetime.datetime(2024, 8, 30, 12, 0, 0)}]
        """
        for i, ele in enumerate(batch):
            ele["lastet_tid"] = time
            batch[i] = ele
