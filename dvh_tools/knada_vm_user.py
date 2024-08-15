# setter bruker og passord for lokal kjøring i knada VM
# setter miljøvariablene DBT_ENV_SECRET_USER og DBT_ENV_SECRET_PASS, som altså blir lagret i minnet

import os
from getpass import getpass

# Brukernavn kan settes manuelt eller fra denne listen, bare å legge til flere
user_dict = {
    "1": "M167094",
    "2": "W158886",
    "3": "A170141",
}

schema_dict = {
    "1": "DVH_AAP",
    "2": "DVH_DAGPENGER",
    "3": "DVH_TILTAKSPENGER",
    "4": "DVH_TILLEGGSSTONADER",
}

def get_environ_input():
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


def set_environ():
    """Bruker input og getpass for å sette miljøvariablene for bruker og passord,
    dersom de ikke finnes fra før.

    OBS! Dette funker i gjeldende sesjon (og da feks i en jupyter notebook), men 
    miljøvariablene som eventuelt settes blir ikke lagret i terminalen når scriptet
    slutter. 
    Bruk da heller bash-scriptet environment_local_user.sh for å sette miljøvariablene.
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
