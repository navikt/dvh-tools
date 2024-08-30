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
    """Retrieves the latest version of a secret from Google Secret Manager.

    This function accesses the latest version of the specified secret from Google
    Secret Manager (GSM) and returns it as a dictionary.

    Args:
        project_id (str): The ID of the Google Cloud project containing the secret.
        secret_name (str): The name of the secret in GSM to retrieve.

    Returns:
        secret (dict): The secret data as a dictionary, parsed from JSON format.

    Examples:
        >>> project_id = 'my-gcp-project'
        >>> secret_name = 'my-secret'
        >>> secret_data = get_gsm_secret(project_id, secret_name)
        >>> print(secret_data)
    """
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_name}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    secret = json.loads(response.payload.data.decode("UTF-8"))
    return secret


def create_bigquery_client(project_id: str, secret_name_bigquery: str):
    """Creates a BigQuery client using service account credentials from a secret manager.

    This function retrieves service account credentials from a secret manager, 
    creates a BigQuery client with those credentials, and returns the client.

    Args:
        project_id (str): The ID of the Google Cloud project where the secret is stored.
        secret_name_bigquery (str): The name of the secret containing the service account key.

    Returns:
        bigquery_client (google.cloud.bigquery.Client): A BigQuery client instance configured with the retrieved credentials.

    Examples:
        >>> from google.cloud import bigquery
        >>> # Replace with your actual project ID and secret name
        >>> project_id = 'my-gcp-project'
        >>> secret_name = 'my-bigquery-secret'
        >>> bq_client = create_bigquery_client(project_id, secret_name)
        >>> df = bq_client.query("SELECT * FROM `{project_id_bq}.{dataset}.{source_table}`").to_dataframe()
        >>> print(df)
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
    """Truncates a BigQuery table and loads data from a DataFrame into it.

    This function truncates the specified BigQuery table and loads the data from
    the provided DataFrame into the table. If the table does not exist, it will
    be created. Optionally, the table description is updated if provided.

    Args:
        df (pd.DataFrame): The DataFrame containing the data to load into BigQuery.
        bq_client (Client): The BigQuery client used to interact with BigQuery.
        bq_target (str): The target table in BigQuery, specified as `dataset.table`.
        bq_table_description (str, optional): Description to set for the table. Defaults to an empty string.
        bq_columns_schema (list[SchemaField]): List of SchemaField objects defining the schema of the table.

    Examples:
        >>> from google.cloud import bigquery
        >>> import pandas as pd
        >>> from google.cloud.bigquery import SchemaField
        >>> # Sample DataFrame
        >>> df = pd.DataFrame({'name': ['Alice', 'Bob'], 'age': [25, 30]})
        >>> # Create BigQuery client
        >>> bq_client = bigquery.Client()
        >>> # Define table schema
        >>> schema = [
        >>>     SchemaField('name', 'STRING'),
        >>>     SchemaField('age', 'INTEGER')
        >>> ]
        >>> # Call the function to truncate and load data
        >>> trunc_and_load_to_bq(
        >>>     df=df,
        >>>     bq_client=bq_client,
        >>>     bq_target='my_dataset.my_table',
        >>>     bq_table_description='Sample table with user data',
        >>>     bq_columns_schema=schema
        >>> )
    """
    logging.info(f"creating the table in BigQuery with the data from the given df")
    job_config = LoadJobConfig(
        schema=bq_columns_schema,
        write_disposition="WRITE_TRUNCATE",  # Truncates the table before loading
        create_disposition="CREATE_IF_NEEDED",
    )
    insert_job = bq_client.load_table_from_dataframe(
        df, bq_target, job_config=job_config
    )
    insert_job.result()
    logging.info(f"Loaded {insert_job.output_rows} rows into {bq_target}")

    # Update table description if it has changed
    table = bq_client.get_table(bq_target)
    if table.description != bq_table_description:
        table.description = bq_table_description
        bq_client.update_table(table, ["description"])
        logging.info(f"Updated table description for {bq_target}")


def create_storage_client(project_id: str, secret_name_bucket: str):
    """Creates a Google Cloud Storage client using service account credentials.

    This function retrieves service account credentials from a secret manager,
    creates a Google Cloud Storage client, and returns it.

    Args:
        project_id (str): The ID of the Google Cloud project.
        secret_name_bucket (str): The name of the secret in the secret manager containing the service account key.

    Returns:
        storage_client (google.cloud.storage.Client): A Google Cloud Storage client instance configured with the provided credentials.

    Examples:
        >>> project_id = 'my-gcp-project'
        >>> secret_name = 'my-bucket-secret'
        >>> storage_client = create_storage_client(project_id, secret_name)
        >>> print(storage_client)
    """
    bucket_secret = get_gsm_secret(project_id, secret_name_bucket)
    creds = service_account.Credentials.from_service_account_info(bucket_secret)
    storage_client = storage.Client(credentials=creds, project=creds.project_id)
    return storage_client


def from_gcs_to_df(bucket_name, client, filename, sep):
    """Reads a CSV file from a Google Cloud Storage bucket into a pandas DataFrame.

    This function downloads a CSV file from the specified Google Cloud Storage bucket
    and loads it into a pandas DataFrame.

    Args:
        bucket_name (str): The name of the Google Cloud Storage bucket.
        client (google.cloud.storage.client.Client): The storage client used to interact with Google Cloud Storage.
        filename (str): The name of the CSV file to read from the bucket.
        sep (str): The separator used in the CSV file.

    Returns:
        df (pd.Dataframe): A pandas DataFrame containing the data from the CSV file.

    Examples:
        >>> from google.cloud import storage
        >>> client = storage.Client()
        >>> df = from_gcs_to_df('my_bucket', client, 'data.csv', ',')
        >>> print(df.head())
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