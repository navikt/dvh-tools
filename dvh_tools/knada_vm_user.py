# setter bruker og passord for lokal kjøring i knada VM
# setter miljøvariablene DBT_ENV_SECRET_USER og DBT_ENV_SECRET_PASS, som altså blir lagret i minnet

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
    
    OBS! Dette funker i en jupyter notebook, men blir ikke lagret i minnet når python scriptet avsluttes.
    Bruk da heller bash-scriptet environment_local_user.sh for å sette miljøvariablene."""    
    user, password = get_environ_input()
    os.environ['DBT_ENV_SECRET_USER'] = user
    os.environ['DBT_ENV_SECRET_PASS'] = password
    print("Miljøvariablene DBT_ENV_SECRET_USER og DBT_ENV_SECRET_PASS satt for: ", os.environ.get("DBT_ENV_SECRET_USER"))


if __name__ == "__main__":
    set_environ()
