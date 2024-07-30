import json
from io import BytesIO
from google.cloud import secretmanager
from google.oauth2 import service_account
from google.cloud import storage
from google.cloud import bigquery
import pandas as pd
from google.cloud.exceptions import NotFound
from google.cloud.bigquery.schema import SchemaField
from google.cloud.bigquery import Client, LoadJobConfig
import logging


def get_gsm_secret(project_id, secret_name):
    """Gets the value of the secret into a python object

    Parameters
    ----------
    project_id : str
        Name of Google project
    secret_name : str
        name of secret in Google Secret Manager

    Returns
    -------
    object
        The value in the json in the secret
    """
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


def create_storage_client(project_id: str, secret_name_bucket: str):
    """Creates a Google storage client used for interacting with buckets

    Parameters
    ----------
    project_id : str
        Name of Google project
    secret_name_bucket : str
        Name of GSM secret where the key to the service user is stored

    Returns
    -------
    google.cloud.storage.client.Client
        storage client that can be used to interacting with buckets
    """
    bucket_secret = get_gsm_secret(project_id, secret_name_bucket)
    creds = service_account.Credentials.from_service_account_info(bucket_secret)
    storage_client = storage.Client(credentials=creds, project=creds.project_id)
    return storage_client


def from_gcs_to_df(bucket_name, client, filename, sep):
    """Reads a csv-file from a bucket into a pandas dataframe

    Parameters
    ----------
    bucket_name : str
        name of bucket
    client : google.cloud.storage.client.Client
        storage client
    filename :
        name of csv-file
    sep : str
        separator used in the csv

    Returns
    -------
    pandas.dataframe
        A pandas dataframe of the data in the csv-file
    """
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(filename)
    byte_stream = BytesIO()
    blob.download_to_file(byte_stream)
    byte_stream.seek(0)
    if filename.lower().endswith('.csv'):
        df = pd.read_csv(byte_stream, sep=sep)
    del blob, byte_stream
    return df