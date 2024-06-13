from google.cloud import secretmanager
from google.cloud.bigquery import QueryJobConfig
import json
from typing import Optional

from .bq_reader import BQReader
from .oracle_writer import OracleWriter

import logging


def get_secret_env(resource_name) -> dict:
    secrets = secretmanager.SecretManagerServiceClient()
    secret = secrets.access_secret_version(name=resource_name)
    secret_str = secret.payload.data.decode("UTF-8")
    return json.loads(secret_str)


class DataTransfer:
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

    def run(self, batch_limit=None, datatypes=None, convert_lists=False):
        for i, batch in enumerate(self.bq_reader):
            self.oracle_writer.write_batch(batch, datatypes=datatypes, convert_lists=convert_lists)
            if batch_limit:
                if i > batch_limit:
                    self.oracle_writer.cleanup()
                    break
            logging.info(f"total rows read: {self.bq_reader.total_rows_read}")
        self.oracle_writer.cleanup()
