from google.cloud import secretmanager
from google.cloud.bigquery import QueryJobConfig
from typing import Optional
from .bq_reader import BQReader
from .oracle_writer import OracleWriter
import logging
import json


def get_secret_env(resource_name):
    """Retrieves and decodes a secret from Google Secret Manager.

    Args:
        resource_name (str): The resource name of the secret in the format 
                             `projects/{project_id}/secrets/{secret_id}/versions/latest`.

    Returns:
        dict: The decoded secret as a dictionary.

    Examples:
        >>> secret = get_secret_env("projects/my-project/secrets/my-secret/versions/latest")
        >>> print(secret)
        {'key': 'value', ...}
    """
    secrets = secretmanager.SecretManagerServiceClient()
    secret = secrets.access_secret_version(name=resource_name)
    secret_str = secret.payload.data.decode("UTF-8")
    return json.loads(secret_str)


class DataTransfer:
    """Handles the transfer of data from BigQuery to an Oracle database.

    This class reads data from BigQuery using the `BQReader` class and writes it to an Oracle
    database using the `OracleWriter` class. It supports optional batch processing and data 
    conversion.

    Attributes:
        oracle_writer (OracleWriter): The writer instance for inserting data into Oracle.
        bq_reader (BQReader): The reader instance for fetching data from BigQuery.

    Args:
        config (dict): Configuration dictionary containing credentials and settings for both BigQuery
            and Oracle. The dictionary must contain keys `"gcp"` and `"oracle"` with appropriate
            configuration details.
        source_query (str): The SQL query to execute in BigQuery.
        target_table (Optional[str], optional): The target table in Oracle where data will be written.
            Defaults to None.
        query_job_config (Optional[QueryJobConfig], optional): Optional configuration for the BigQuery
            query job. Defaults to None.

    Methods:
        run(batch_limit: Optional[int] = None, datatypes: Optional[dict] = None, convert_lists: bool = False):
            Executes the data transfer process, reading from BigQuery and writing to Oracle.
    """
    def __init__(
        self,
        config,
        source_query,
        target_table=None,
        query_job_config: Optional[QueryJobConfig] = None,
    ):
        self.__config = config
        self.oracle_writer = OracleWriter(self.__config["oracle"], target_table=target_table)
        self.bq_reader = BQReader(
            self.__config["gcp"], source_query=source_query, query_job_config=query_job_config
        )

    def run(self, batch_limit=None, datatypes=None, convert_lists=False) -> None:
        """Reads data from BigQuery and writes it to an Oracle database.

        This method iterates over the batches of data fetched from BigQuery, writes each batch to 
        the Oracle database, and performs optional data conversion. The process stops after the
        specified batch limit if provided.

        Args:
            batch_limit (Optional[int], optional): Maximum number of batches to process. If None,
                processes all available batches. Defaults to None.
            datatypes (Optional[dict], optional): Optional dictionary mapping column names to data types
                for conversion in Oracle. Defaults to None.
            convert_lists (bool, optional): Whether to convert lists in the data to a specific format
                for Oracle. Defaults to False.

        Examples:
            >>> config = {"gcp": {"type": "service_account", ...}, "oracle": {"dsn": "oracle_dsn", ...}}
            >>> source_query = "SELECT * FROM my_dataset.my_table"
            >>> data_transfer = DataTransfer(config, source_query, target_table="my_oracle_table")
            >>> data_transfer.run(batch_limit=10, datatypes={"column1": "VARCHAR2", "column2": "NUMBER"}, convert_lists=True)
            >>> # Data will be transferred in batches from BigQuery to Oracle
        """
        for i, batch in enumerate(self.bq_reader):
            self.oracle_writer.write_batch(batch, datatypes=datatypes, convert_lists=convert_lists)
            if batch_limit:
                if i > batch_limit:
                    self.oracle_writer.cleanup()
                    break
            logging.info(f"total rows read: {self.bq_reader.total_rows_read}")
        self.oracle_writer.cleanup()
