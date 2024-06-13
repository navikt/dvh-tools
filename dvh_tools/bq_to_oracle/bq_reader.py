from typing import Optional
from google.cloud import bigquery
from google.oauth2 import service_account


class BQReader:
    def __init__(
        self, config, source_query, query_job_config: Optional[bigquery.QueryJobConfig] = None
    ):
        self.__config = config
        self.client = bigquery.Client(
            credentials=service_account.Credentials.from_service_account_info(self.__config)
        )
        self._query_job = self.client.query(source_query, job_config=query_job_config)

        self._generator = self.__batch_generator()
        self.total_rows_read = 0

    def __iter__(self):
        return self

    def __next__(self):
        rows = [dict(row) for row in next(self._generator)]
        if len(rows) > 0:
            self.total_rows_read += len(rows)
            return rows
        else:
            raise StopIteration

    def __batch_generator(self):
        for page in self._query_job.result(page_size=500000).pages:
            yield page
