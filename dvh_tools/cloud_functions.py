import json
from google.cloud import secretmanager
from google.oauth2 import service_account
from google.cloud import bigquery

def get_gsm_secret(project_id, secret_name):
    '''Returnerer secret-verdien
    '''
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_name}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    secret = json.loads(response.payload.data.decode('UTF-8'))
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
