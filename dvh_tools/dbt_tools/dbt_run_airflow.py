import subprocess
import os
import time
import json
from google.cloud import secretmanager

from dvh_tools.dbt_tools.dbt_docs_publish import publish_docs


def run_dbt(*, secret_name: str):
    """
    Kjøring av `dbt build` og eventuelt `dbt docs generate` i Airflow.
    Stegene er:
        1. Hente environment variabel "env" fra Airflow, enten U eller P
        2. Hente secrets fra Google Secret Manager
        3. Sette environment variabler basert på secret values
        4a. Kjøre `dbt build`
        4b. Eventuelt kjøre `dbt docs generate` og `publish_docs()`

    Args:
        secret_name (str): Secret name fra GSM, feks "dvh_dagpenger"

    Raises:
        ValueError: Hvis secret_name mangler, eller environment variabel er feil
        Exception: Hvis `dbt build` eller `dbt docs generate` feiler
    """

    if not secret_name:
        raise ValueError("Missing secret name")

    dbt_base_command = ["dbt", "--no-use-colors", "--log-format", "json"]

    # henter miljøvariabel "env", som blir satt i DAGen via `ENVIRONMENT`
    environment = os.getenv("env", "U")  # U = utvikling, P = produksjon
    if environment not in ["P", "U"]:
        raise ValueError(f"Invalid environment: {environment}. Must be 'P' or 'U'.")
    # setter prosjekt-id for å hente riktig secret
    project_id = "spenn-prod-23e0" if environment == "P" else "spenn-dev-5a1e"

    # miljøvariabel for å generere docs eller ikke
    make_docs = os.getenv("make_docs", "no")

    # tidssone Europe/Oslo
    os.environ["TZ"] = "Europe/Oslo"
    time.tzset()

    # koble til secret manager
    with secretmanager.SecretManagerServiceClient() as client:
        name = f"projects/{project_id}/secrets/{secret_name}/versions/latest"
        response = client.access_secret_version(request={"name": name})
        payload = response.payload.data.decode("UTF-8")

    # setter environment variabler for dbt
    dbt_secrets = json.loads(payload).get("DBT").get(environment)
    os.environ["DBT_DB_TARGET"] = environment
    os.environ["DBT_DB_SCHEMA"] = dbt_secrets.get("schema")
    os.environ["DBT_ENV_SECRET_USER"] = str(dbt_secrets.get("user"))
    os.environ["DBT_ENV_SECRET_PASS"] = dbt_secrets.get("password")
    os.environ["ORA_PYTHON_DRIVER_TYPE"] = "thin"

    # kjører dbt build
    try:
        if make_docs == "no":
            completed_process = subprocess.run(
                dbt_base_command + ["build"],
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            print(completed_process.stdout)

        # kjører dbt docs generate istedenfor dbt build
        if make_docs == "yes":
            completed_process = subprocess.run(
                dbt_base_command + ["docs", "generate"],
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            print(completed_process.stdout)
    except subprocess.CalledProcessError as err:
        raise Exception(err.stdout)
    if make_docs == "yes":
        publish_docs()


if __name__ == "__main__":
    run_dbt()
