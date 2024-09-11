from dvh_tools.cloud_functions import get_gsm_secret
from dbt_docs_publish import publish_docs
import subprocess
import os
import time

# Constants for default values
DEFAULT_TIMEZONE = "Europe/Oslo"
DEFAULT_ENVIRONMENT = "U"
DEFAULT_DBT_BUILD_COMMAND = ["build"]

def run_subprocess(command):
    """Run a subprocess command and return the output.

    Args:
        command (list): A list containing the command and its arguments.

    Returns:
        str: The standard output of the command if it succeeds.

    Raises:
        Exception: If the subprocess fails and returns a non-zero exit code.
        FileNotFoundError: If the command is not found.
        Exception: For any unexpected error that occurs.

    Examples:
        >>> run_subprocess(["ls", "-l"])
        # Returns the output of the 'ls -l' command.
        
        >>> run_subprocess(["echo", "Hello World"])
        'Hello World\n'
        
        >>> run_subprocess(["invalid_command"])
        # Raises an Exception due to the invalid command.
    """
    try:
        completed_process = subprocess.run(
            command,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        return completed_process.stdout
    except subprocess.CalledProcessError as err:
        raise Exception(f"Kommando '{' '.join(command)}' feilet med error: {err.stderr}")
    except FileNotFoundError:
        raise Exception(f"Kommando '{command[0]}' ikke funnet.")
    except Exception as e:
        raise Exception(f"Uforventet error oppstod: {e}")

def configure_environment(secret_name, environment, project_id_prod, project_id_dev) -> None:
    """Configure environment variables based on the provided secret and environment type.

    Args:
        secret_name (str): The name of the secret in Google Cloud Secret Manager.
        environment (str): The environment type ('P' for production, 'U' for development).
        project_id_prod (str): The project ID for production.
        project_id_dev (str): The project ID for development.

    Raises:
        Exception: If the secret cannot be found in the specified project.

    Examples:
        >>> configure_environment("dbt_secret", "P", "prod_project_id", "dev_project_id")
        # Configures environment variables for production.

        >>> configure_environment("dbt_secret", "U", "prod_project_id", "dev_project_id")
        # Configures environment variables for development.
    """
    project_id = project_id_prod if environment == "P" else project_id_dev
    dbt_secret = get_gsm_secret(project_id, secret_name)

    if not dbt_secret:
        raise Exception(f"Hemmelighet '{secret_name}' er ikke funnet i prosjektet '{project_id}'")

    os.environ["DBT_DB_TARGET"] = environment
    os.environ["DBT_DB_SCHEMA"] = dbt_secret.get("DB_SCHEMA")
    os.environ["DBT_ENV_SECRET_USER"] = str(dbt_secret.get("DB_USER"))
    os.environ["DBT_ENV_SECRET_PASS"] = dbt_secret.get("DB_PASSWORD")
    os.environ["ORA_PYTHON_DRIVER_TYPE"] = "thin"

def run_dbt_command(
        secret_name,
        project_id_prod,
        project_id_dev,
        publish_docs_param=None,
        timezone=DEFAULT_TIMEZONE,
        environment=DEFAULT_ENVIRONMENT,
        dbt_build_command=DEFAULT_DBT_BUILD_COMMAND
    ) -> None:
    """Run a dbt command using environment variables and secrets.

    Args:
        secret_name (str): The name of the secret in Google Cloud Secret Manager.
        project_id_prod (str): The Google Cloud project ID for production.
        project_id_dev (str): The Google Cloud project ID for development.
        publish_docs_param (str, optional): Optional parameter for the `publish_docs` function.
        timezone (str, optional): The timezone to use for the process (default is 'Europe/Oslo').
        environment (str, optional): The environment to run in, either 'P' for production or 'U' for development (default is 'U').
        dbt_build_command (list, optional): The dbt command to run (default is `["build"]`).

    Raises:
        ValueError: If the `environment` is not 'P' or 'U'.
        ValueError: If the `make_docs` environment variable is not 'yes' or 'no'.
        Exception: If the timezone setting is not supported on the system.
        Exception: If the dbt command fails or cannot be found.
        Exception: If the secret for the environment cannot be found in Google Cloud Secret Manager.

    Examples:
        >>> run_dbt_command("dbt_secret", "prod_project_id", "dev_project_id")
        # Runs the default dbt build command for the development environment.

        >>> run_dbt_command(
        ...     secret_name="dbt_secret",
        ...     project_id_prod="prod_project_id",
        ...     project_id_dev="dev_project_id",
        ...     publish_docs_param="publish_param",
        ...     timezone="America/New_York",
        ...     environment="P"
        ... )
        # Runs the dbt command in production with a custom timezone and docs publishing.
    """
    # Set timezone
    os.environ["TZ"] = timezone
    try:
        time.tzset()
    except AttributeError:
        raise Exception("Innstilling av tidssone støttes ikke på dette systemet.")

    # Retrieve and validate environment
    env = os.getenv("env", environment)
    if env not in ["P", "U"]:
        raise ValueError(f"Ugyldig miljø: {env}. Må være 'P' eller 'U'.")

    # Configure environment variables
    configure_environment(secret_name, env, project_id_prod, project_id_dev)
    print(f"Kjører mot {os.environ['DBT_DB_TARGET']} med {os.environ['DBT_ENV_SECRET_USER']}")

    # Determine dbt command
    dbt_command = os.environ.get("dbt_command", None)
    dbt_command = dbt_command.split(",") if dbt_command else dbt_build_command

    # Build dbt command base
    dbt_base_command = ["dbt", "--no-use-colors", "--log-format", "json"]

    # Run dbt deps first
    print(f"Kjører dbt deps")
    output = run_subprocess(dbt_base_command + ["deps"])
    print(output)

    # Run dbt command or docs generation
    make_docs = os.getenv("make_docs", "no")
    if make_docs == "no":
        print(f"Kjører dbt-kommando: {dbt_command}")
        output = run_subprocess(dbt_base_command + dbt_command)
        print(output)
    elif make_docs == "yes":
        print("Genererer dbt docs")
        output = run_subprocess(dbt_base_command + ["docs", "generate"])
        print(output)
        if publish_docs_param:
            publish_docs(publish_docs_param)
    else:
        raise ValueError(f"Ugyldig 'make_docs'-verdi: {make_docs}. Må være 'yes' eller 'no'.")
