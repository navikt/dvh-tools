import os
from getpass import getpass

# Dictionary mapping numeric keys to user username IDs
user_dict = {
    "1": "M167094",
    "2": "W158886",
    "3": "A170141",
}

# Dictionary mapping numeric keys to schema names
schema_dict = {
    "1": "DVH_AAP",
    "2": "DVH_DAGPENGER",
    "3": "DVH_TILTAKSPENGER",
    "4": "DVH_TILLEGGSSTONADER",
}

def get_environ_input():
    """Prompts the user for credentials and schema information.

    This function displays options for user IDs and schemas, then prompts the user
    to select or enter these values manually. It also asks for a password securely.
    The selected or entered values are returned as a tuple containing the user and password.

    Returns:
        tuple[str, str]: A tuple where the first element is the user information
        (formatted as "user[schema]" if a schema is provided) and the second element is the password.

    Examples:
        >>> user, password = get_environ_input()
        >>> print(user)
        >>> print(password)
    """
    print("Hurtigvalg for brukere:")
    for key, value in user_dict.items():
        print(f"    {key}: {value}")
    user = input("Velg bruker med nummer eller skriv inn manuelt: ")
    if user in user_dict:
        user = user_dict[user]
    print("\nHurtigvalg for skjema (trykk enter for å hoppe over):")
    for key, value in schema_dict.items():
        print(f"    {key}: {value}")
    skjema = input("Velg skjema med nummer eller skriv inn manuelt:")
    password = getpass("\nPassord: ")
    if skjema == "":
        return user, password
    if skjema in schema_dict:
        skjema = schema_dict[skjema]
    return f"{user}[{skjema}]", password


def set_environ() -> None:
    """Sets environment variables for user and password if they are not already set.

    This function checks if the environment variables `DBT_ENV_SECRET_USER` and
    `DBT_ENV_SECRET_PASS` are already set. If not, it prompts the user for credentials
    and schema information, then sets these environment variables for the current session.
    Note that these environment variables are only set for the current session and will
    not persist after the script ends. For persistent environment variables, use the
    `environment_local_user.sh` script.

    Examples:
        >>> set_environ()
        Miljøvariablene DBT_ENV_SECRET_USER og DBT_ENV_SECRET_PASS satt for: M167094
    """
    if (
        os.environ.get("DBT_ENV_SECRET_USER") is not None
        and os.environ.get("DBT_ENV_SECRET_PASS") is not None
    ):
        print(
            "Miljøvariabler er allerede satt for bruker: ",
            os.environ.get("DBT_ENV_SECRET_USER"),
        )
        return
    user, password = get_environ_input()
    os.environ["DBT_ENV_SECRET_USER"] = user
    os.environ["DBT_ENV_SECRET_PASS"] = password
    print(
        "Miljøvariablene DBT_ENV_SECRET_USER og DBT_ENV_SECRET_PASS satt for: ",
        os.environ.get("DBT_ENV_SECRET_USER"),
    )


if __name__ == "__main__":
    set_environ()
