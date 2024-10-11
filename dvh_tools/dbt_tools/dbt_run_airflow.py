import os
import time
import logging
from dbt.cli.main import dbtRunner, dbtRunnerResult

from dvh_tools.dbt_tools import publish_docs
from dvh_tools.cloud_functions import get_gsm_secret

DBT_BASE_COMMAND = ["--no-use-colors", "--log-format-file", "json"]
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

def dbt_run_airflow(
    secret_name: str,
    project_id_prod: str,
    project_id_dev: str,
    dbt_docs_navn="",
    run_dbt_deps=False,
    ) -> None:
    """Funksjon for å kjøre dbt i en Airflow DAG.
    dbt-kommandoen blir angitt i miljøvariabelen 'dbt_command', satt i DAGen.
    Target-miljøet blir angitt i miljøvariabelen 'env', satt i DAGen.

    For fremtiden: Deler av eller hele dbt-loggen kan skrives til xcom.

    Args:
        secret_name (str): Secretnavn i GSM med dbt-credentials. Hentes med get_gsm_secret
        project_id_prod (str): GCP-prosjekt for produksjon, for å hente riktig secret
        project_id_dev (str): GCP-prosjekt for utvikling, for å hente riktig secret
        dbt_docs_navn (str, optional): Prosjekt-delen av url til dbt docs. Defaults to "".
        run_dbt_deps (bool, optional): Boolean for om 'dbt deps' skal kjøres før den angitte dbt-kommandoen. Defaults to False.
    """    
    
    # henter dbt-kommando fra DAGen. Default er 'build'
    # eks på dbt_command i DAG er: 'build --select tag:daglig'
    dbt_command = os.environ.get("dbt_command", 'build')
    logging.info(f"Kjører dbt med kommando 'dbt {dbt_command}'. Først litt oppsett...")
    dbt_command = dbt_command.split(" ")

    # setter opp miljøvariabler
    os.environ["TZ"] = "Europe/Oslo"
    time.tzset()  # OBS! Denne linja funker ikke på windows
    environment = os.environ["env"]
    if environment not in ["P", "U"]:
        raise ValueError(f"Ugyldig miljø: {environment}. Må være 'P' eller 'U'.")
    project_id = project_id_prod if environment == "P" else project_id_dev
    dbt_secret = get_gsm_secret(project_id, secret_name)
    os.environ["DBT_DB_TARGET"] = environment
    os.environ["DBT_DB_SCHEMA"] = dbt_secret.get("DB_SCHEMA")
    os.environ["DBT_ENV_SECRET_USER"] = dbt_secret.get("DB_USER")
    os.environ["DBT_ENV_SECRET_PASS"] = dbt_secret.get("DB_PASSWORD")
    os.environ["ORA_PYTHON_DRIVER_TYPE"] = "thin"
    logging.info(f"Kjører mot miljø {environment} med brukeren {os.environ['DBT_ENV_SECRET_USER']}")


    # lager en dbtRunner, som er istedenfor subprocess.run
    dbt = dbtRunner()

    # eventuelt: kjøre 'dbt deps' først
    if run_dbt_deps:
        logging.info(f"Kjører dbt deps")
        dbt_deps = dbt.invoke(DBT_BASE_COMMAND + ["deps"])
    
    # kjører den gitte dbt-kommandoen, som også gir live logging
    output: dbtRunnerResult = dbt.invoke(DBT_BASE_COMMAND + dbt_command)

    # etter kjørt dbt-kommando håndterer vi eventuell feil
    # exit code 2, feil utenfor DBT
    if output.exception:
        raise output.exception
    # exit code 1, feil i dbt (test eller under kjøring)
    if not output.success:
        raise Exception(output.result)

    # eventuelt: publiserer dbt docs til dbt.ansatt.nav.no
    if "docs" in dbt_command:
        if dbt_docs_navn == "":
            raise ValueError("Mangler dbt_docs_navn for å publisere dbt docs")
        else:
            logging.info("publiserer dbt docs")
            publish_docs(docs_url_project=dbt_docs_navn)

    # fremtidig: legg til logikk for å skrive loggfila eller annet til xcom
