from pathlib import Path
from yaml import safe_load
from dvh_tools.oracle import db_read_to_df
from dvh_tools.knada_vm_user import set_environ
from dvh_tools.cloud_functions import get_gsm_secret


def get_comments_from_oracle(
        *,
        project_id=None,
        secret_name=None,
        sources_yml_path="dbt/sources.yml"
        ):
    """
    Leser kildetabeller i sources.yml, kobler seg til Oracle, henter alle kommentarer
    og lager en 'comments_source.yml' som kan brukes i autogenerering til modeller.

    Oppdaterer/lager 'comments_source.yml' med tabell- og kolonnekommentarer.

    Antar at py-fila blir kjørt fra en mappe dbt-prosjektet, feks fra dbt/docs/
    Det er her ^ comments_source.yml (output) blir lagret.

    Args:
        project_id (str): GCP-prosjekt-ID. Defaults to None.
        secret_name (str): Hemmelighetsnavn i GSM. Defaults to None.
        sources_yml_path (str): Path til soruces.yml. Defaults to "dbt/sources.yml".
    
    Returns:
        None
    """
    # henter hemmeligheter fra Google Secret Manager. trenger DSN
    print("setter hemmeligheter for Oracle tilkobling")
    if project_id is None or secret_name is None:
        print("Mangler prosjekt-ID og/eller hemmelighetsnavn")
        exit(1)
    secret_dict = get_gsm_secret(project_id, secret_name)
    set_environ()

    # find the sources.yml file
    def find_project_root(current_path):
        """Recursively find the project's root directory by looking for a specific marker (e.g., '.git' folder)."""
        if (current_path / '.git').exists():
            return current_path
        else:
            return find_project_root(current_path.parent)

    def find_all_sources_from_yml(sources_yml_path=sources_yml_path):
        """Finner alle kilder fra sources.yml."""
        print("Finner sources.yml fra:", sources_yml_path)
        project_root = find_project_root(Path(__file__).resolve())
        source_file = project_root / sources_yml_path  # Adjust this line if sources_yml_path should not be relative to project_root
        try:
            with open(source_file, "r") as file:
                content = file.read()
        except FileNotFoundError:
            print(f"Finner ikke yaml-filen hvor sources er spesifisert")
            print(f"Prøvde å lese fra: {source_file}")
            print(f"Endre argumentet 'sources_yml_path' til riktig path, som nå er:\n {sources_yml_path}")
            exit(1)
        yml_raw = safe_load(content)
        schema_list = yml_raw["sources"]
        schema_table_dict = {}  # schema som key, liste av tabellnavn som value
        for schema in schema_list:
            if schema["name"] != schema["schema"]:
                print("Obs! verdiene for name og schema er ulike! Se:", schema)
            schema_name = schema["name"]
            tables_name_list = []
            for table in schema["tables"]:
                tables_name_list.append(table["name"])
            schema_table_dict[schema_name] = tables_name_list
        return schema_table_dict


    def get_table_comments_from_oracle(schema_name: str, table_name: str) -> str:
        """Henter tabellkommentar fra Oracle-databasen.
        Args:
            schema_name (str): skjemanavn
            table_name (str): tabellnavn
        Returns:
            str: tabellkommentaren"""
        sql = f"""select comments from all_tab_comments
            where owner = upper('{schema_name}') and table_name = upper('{table_name}')"""
        sql_result = db_read_to_df(sql, secret_dict)
        if sql_result.empty or sql_result.iloc[0, 0] is None:
            return ""
        else:
            # fjerner fnutter, fordi det skaper problemer senere
            return sql_result.iloc[0, 0].replace("'", "").replace('"', "")


    def get_column_comments_from_oracle(schema_name: str, table_name: str) -> dict:
        """Henter alle kolonnekommentarer til en tabell i databasen.
        Args:
            schema_name (str): skjemanavn
            table_name (str): tabellnavn
        Returns:
            pd.dataframe: df med 'column_name' og 'comments'"""
        sql = f"""select column_name, comments from dba_col_comments
            where owner = upper('{schema_name}') and table_name = upper('{table_name}')"""
        df_col_comments = db_read_to_df(sql, secret_dict)
        df_col_comments["column_name"] = df_col_comments["column_name"].str.lower()
        df_col_comments["comments"] = df_col_comments["comments"].str.replace("'", "").str.replace('"', "")
        df_col_comments["comments"] = df_col_comments["comments"].fillna("")
        return df_col_comments


    print("Henter tabellbeskrivelser fra Oracle")
    schema_table_dict = find_all_sources_from_yml()
    src_table_descriptions = {}  # kommentar til source-folder
    stg_table_descriptions = {}  # kommentar til staging-modeller
    for schema, table_list in schema_table_dict.items():
        for table in table_list:
            source_description = get_table_comments_from_oracle(schema, table).replace("\n", " | ")
            if source_description is None:
                source_description = "(ingen modellbeskrivelse i Oracle)"
            stg_table_descriptions[f"stg_{table}"] = f"Staging av {schema}.{table}, med original beskrivelse: {source_description}."
            src_table_descriptions[table] = source_description


    # makes the file dbt/models/sources_with_comments.yml
    # and fills in the dict with unique column comments
    print("Henter kolonnekommentarer fra Oracle")
    print("Lager 'sources_with_comments.yml'")
    column_comments_dict = {}
    yml = "# IKKE ENDRE DENNE FILA!\n"
    yml += "# Den er autogenerert av dvh_tools.dbt.tools.get_comments_from_oracle\n"
    yml += "# Fjern/legg til kilder i dbt/sources.yml\n\n"
    yml += """version: 2\n\nsources:\n"""
    for schema, table_list in schema_table_dict.items():
        yml += f"  - name: {schema}\n"
        yml += f"    schema: {schema}\n"
        yml += f"    tables:\n"
        for table in table_list:
            yml += f"      - name: {table}\n"
            yml += f"        description: '{src_table_descriptions[table]}'\n"
            yml += f"        columns:\n"
            df_table_columns_comments = get_column_comments_from_oracle(schema, table)
            for _, row in df_table_columns_comments.iterrows():
                yml += f"          - name: {row['column_name']}\n"
                comments_replace = row['comments'].replace('\n',' | ')
                yml += f"            description: '{comments_replace}'\n"
                # get unique column comments
                column = row["column_name"]
                comment = row["comments"]
                if column not in column_comments_dict:
                    column_comments_dict[column] = comment
    column_comments_dict = dict(sorted(column_comments_dict.items()))

    # lage source_comments.yml, med kolonnekommentarer og staging-kommentarer
    print("Lager 'comments_source.yml'")
    alle_kommentarer = "{\n    source_column_comments: {\n"
    for column, comment in column_comments_dict.items():
        comment_replace = comment.replace('\n', " | ")
        alle_kommentarer += f"""        {column}: "{comment_replace}",\n"""
    alle_kommentarer += "    },\n\n    source_table_descriptions: {\n"
    for table, description in stg_table_descriptions.items():
        alle_kommentarer += f"""        {table}: "{description}",\n"""
    alle_kommentarer += "    }\n}\n"

    project_root = find_project_root(Path(__file__).resolve())
    with open(project_root / "dbt/models/sources_with_comments.yml", "w") as file:
        file.write(yml)
    with open(project_root / "dbt/docs/comments_source.yml", "w") as file:
        file.write(alle_kommentarer)
    print("Ferdig!")
