from typing import Optional
from google.cloud import bigquery
from google.oauth2 import service_account


class BQReader:
    """A class for reading data from BigQuery in batches.

    This class initializes a BigQuery client, executes a query, and allows for iteration
    over the query results in batches. The `__iter__` and `__next__` methods are implemented
    to support iteration over the query results, yielding rows in batches.

    Attributes:
        client (bigquery.Client): The BigQuery client used to execute the query.
        total_rows_read (int): The total number of rows read from the query results.

    Args:
        config (dict): Configuration dictionary containing the service account credentials
            for authenticating with BigQuery. Must include the necessary fields for
            `service_account.Credentials.from_service_account_info`.
        source_query (str): The SQL query to execute in BigQuery.
        query_job_config (Optional[bigquery.QueryJobConfig], optional): Optional configuration
            for the query job, such as setting query priority or specifying timeouts. Defaults to None.

    Methods:
        __iter__(): Returns the iterator object (self).
        __next__(): Fetches the next batch of rows from the query results.
        __batch_generator(): A generator that yields pages of query results.

    Examples:
        >>> config = {"type": "service_account", "project_id": "my-project", ...}
        >>> source_query = "SELECT * FROM my_dataset.my_table"
        >>> reader = BQReader(config, source_query)
        >>> for batch in reader:
        >>>     print(batch)
        >>>     # Process each batch of rows here
        >>> print(f"Total rows read: {reader.total_rows_read}")
    """
    def __init__(self, config, source_query, query_job_config: Optional[bigquery.QueryJobConfig] = None):
        self.__config = config
        self.client = bigquery.Client(
            credentials=service_account.Credentials.from_service_account_info(self.__config)
        )
        self._query_job = self.client.query(source_query, job_config=query_job_config)

        self._generator = self.__batch_generator()
        self.total_rows_read = 0


    def __iter__(self):
        """Returns the iterator object (self)."""
        return self


    def __next__(self):
        """Fetches the next batch of rows from the query results.

        Returns:
            list[dict]: A list of rows, where each row is represented as a dictionary.

        Raises:
            StopIteration: When there are no more rows to fetch.
        """
        rows = [dict(row) for row in next(self._generator)]
        if len(rows) > 0:
            self.total_rows_read += len(rows)
            return rows
        else:
            raise StopIteration


    def __batch_generator(self):
        """A generator that yields pages of query results.

        Yields:
            page (bigquery.table.RowIterator): A page of query results.
        """
        for page in self._query_job.result(page_size=500000).pages:
            yield page
