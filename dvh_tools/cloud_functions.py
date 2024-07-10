import json
from google.cloud import secretmanager
from google.oauth2 import service_account
from google.cloud import bigquery
import pandas as pd
from google.cloud.exceptions import NotFound
from google.cloud.bigquery.schema import SchemaField
from google.cloud.bigquery import Client, LoadJobConfig
import logging


def get_gsm_secret(project_id, secret_name):
    """Returnerer secret-verdien"""
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_name}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    secret = json.loads(response.payload.data.decode("UTF-8"))
    return secret


def create_bigquery_client(project_id: str, secret_name_bigquery: str):
    """Lager en BigQuery client som kan hente data som dataframe.

    Parameters
    ----------
    project_id : str
        GSM project id for hemmeligheter
    secret_name_bigquery : str
        Hemmelighetens navn i GSM. Må være en BQ-sørvisbruker

    Examples
    --------
    >>> bq_client = create_bigquery_client(<id>, <secret_name>)
    >>> df = bq_client.query("select * from `{project_id_bq}.{datasett}.{kilde_tabell}`").to_dataframe()

    Returns
    -------
    google.cloud.bigquery.client.Client
        bigquery client som kan hente data som dataframe
    """
    bq_secret = get_gsm_secret(project_id, secret_name_bigquery)
    creds = service_account.Credentials.from_service_account_info(bq_secret)
    bigquery_client = bigquery.Client(credentials=creds, project=creds.project_id)
    return bigquery_client


def trunc_and_load_to_bq(
    *,
    df: pd.DataFrame,
    bq_client: Client,
    bq_target: str,
    bq_table_description: str = "",
    bq_columns_schema: list[SchemaField],
) -> None:
    """Truncate a BigQuery table if it exists, then load a DataFrame to the table.
    If the BigQuery table does not exist, it will be created.

    Args:
        df (pd.DataFrame): DataFrame to load to BigQuery
        bq_client (Client): BigQuery client
        bq_target (str): target table in BigQuery, dataset.table
        bq_table_description (str): table description for BigQuery
        bq_columns_schema (list[SchemaField]): list of SchemaField objects
    """
    logging.info(f"creating the table in BigQuery with the data from the given df")
    job_config = LoadJobConfig(
        schema=bq_columns_schema,
        write_disposition="WRITE_TRUNCATE",  # truncates the table before loading
        create_disposition="CREATE_IF_NEEDED",
    )
    insert_job = bq_client.load_table_from_dataframe(
        df, bq_target, job_config=job_config
    )
    insert_job.result()
    logging.info(f"Loaded {insert_job.output_rows} rows into {bq_target}")

    # Update table description, if it has changed
    table = bq_client.get_table(bq_target)
    if table.description != bq_table_description:
        table.description = bq_table_description
        bq_client.update_table(table, ["description"])
        logging.info(f"Updated table description for {bq_target}")
