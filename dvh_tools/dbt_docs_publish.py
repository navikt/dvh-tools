import os
import requests


def publish_docs(*, docs_url_project: str):
    """
    Funksjon for å publisere dbt-dokumentasjon til dbt-intern.nav.no

    Krever at 'dbt docs generate' er kjørt først, som lager de tre filene som blir
    lastet opp til dbt.intern.nav.no. Nettsiden er speilet på dbt.ansatt.nav.no

    Se dokumentasjon fra Nada på https://github.com/navikt/dbt-docs#publisering

    OBS! Ikke bruk underscore i docs_url_project, bruk heller bindestrek.

    Args:
        docs_url_project (str): siste ledd i url-en, feks 'dvh-aap'
    """    

    if docs_url_project is None:
        ...

        
    docs_url_complete = "https://dbt.intern.nav.no/docs/spenn/" + docs_url_project

    files = ["target/manifest.json", "target/catalog.json", "target/index.html"]
    multipart_form_data = {}
    for file_path in files:
        file_name = os.path.basename(file_path)
        with open(file_path, "rb") as file:
            file_contents = file.read()
            print(f"Gathering {file_path} ({len(file_contents)/1024:.0f} kB)")
            multipart_form_data[file_name] = (file_name, file_contents)

    res = requests.put(docs_url_complete, files=multipart_form_data)
    res.raise_for_status()

    print("HTTP PUT status: ", res.status_code, res.text)
    print()


if __name__ == "__main__":
    publish_docs()
