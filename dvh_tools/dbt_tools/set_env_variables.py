import os
from google.cloud import secretmanager
import time
import json

def set_dbt_env_variables(secret_name: str):
    """
    OBS! Vi har jo ikke dvh-tools i ariflow-imaget, så denne funker bare lokalt.


    Setter environment variabler for kjøring av dbt, enten i Airflow eller lokalt.

    For å kjøre lokalt må terminalen som kjører dbt ha satt environment variabler for
    "LOKAL_USER" og "LOKAL_PASSWORD", hvor LOKAL_USER må være med proxy på skjemaet.

    For øyeblikket er denne tilpasset å hente Spenn secret fra Secret Manager, enten
    for P eller U. Environment variablen som må være satt fra før er "env" (P/U).

    Henter følgende fra GSM:
    - DB_USER, eventuelt LOKAL_USER for lokal kjøring
    - DB_PASSWORD, eventuelt LOKAL_PASSWORD for lokal kjøring
    - DB_SCHEMA

    Setter følgende environment variabler, som blir brukt i profiles.yml:
    - DBT_DB_TARGET
    - DBT_DB_SCHEMA
    - DBT_ENV_SECRET_USER
    - DBT_ENV_SECRET_PASS
    - ORA_PYTHON_DRIVER_TYPE = "thin"

    Args:
        secret_name (str): navnet på secret i Secret Manager, feks "dvh_aap"
    """    
    # target-miljø, altså U = utvikling, P = produksjon
    environment = os.getenv("env", "U")  

    # Setter timezone til Oslo
    os.environ["TZ"] = "Europe/Oslo"
    time.tzset()

    # Koble til secret manager med riktig prosjekt og secret
    project_id = "spenn-prod-23e0" if environment == "P" else "spenn-dev-5a1e"
    with secretmanager.SecretManagerServiceClient() as client:
        name = f"projects/{project_id}/secrets/{secret_name}/versions/latest"
        response = client.access_secret_version(request={"name": name})
        secret_dict = json.loads(response.payload.data.decode("UTF-8"))
        # leggg til .get(environment) hvis vi skal ha U og R i samme secret

    # legger til mulighet for lokal dbt-kjøring
    # husk at user må være <ident>[SKJEMA]
    user = os.environ.get("LOKAL_USER", secret_dict["DB_USER"])
    password = os.environ.get("LOKAL_PASSWORD", secret_dict["DB_PASSWORD"])
    schema = secret_dict["DB_SCHEMA"]

    # setter environment variabler for kjøring, som puttes inn i profiles.yml
    os.environ["DBT_DB_TARGET"] = environment
    os.environ["DBT_DB_SCHEMA"] = schema
    os.environ["DBT_ENV_SECRET_USER"] = user
    os.environ["DBT_ENV_SECRET_PASS"] = password
    os.environ["ORA_PYTHON_DRIVER_TYPE"] = "thin"
