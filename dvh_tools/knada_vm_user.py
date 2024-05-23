# setter bruker og passord for lokal kjøring i knada VM
# setter miljøvariablene USER og PASSWORD, som altså blir lagret i minnet

# _create_connection i oracle.py, samt set_dbt_env_variables i set_env_variables.py,
# bruker disse miljøvariablene for å koble til databasen.

import os
from getpass import getpass

# Brukernavn kan settes manuelt eller fra denne listen, bare å legge til flere
user_dict = {
    '1': 'M167094',
    '2': 'W158886',
    '3': 'M167094[DVH_AAP]',
    '4': 'W158886[DVH_AAP]',
    '5': 'M167094[DVH_TILLEGGSSTONADER]',
}

def get_environ_input():
    print("Hurtigvalg for brukere:")
    for key, value in user_dict.items():
        print(f"    {key}: {value}")
    user = input("Velg bruker med nummer eller skriv inn manuelt: ")
    if user in user_dict:
        user = user_dict[user]
    password = getpass("Passord: ")
    return user, password


def set_environ():
    """Bruker input og getpass for å sette miljøvariablene USER og PASSWORD.
    Todo: vurdere å endre til LOCAL_USER, fordi USER finnes fra før som variabel."""    
    user, password = get_environ_input()
    os.environ['USER'] = user
    os.environ['PASSWORD'] = password
    print("Miljøvariablene USER og PASSWORD satt, med USER=", os.environ.get("USER"))


if __name__ == "__main__":
    set_environ()
