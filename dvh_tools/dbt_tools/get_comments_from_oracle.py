# %%
from pathlib import Path
from yaml import safe_load
from dvh_tools.oracle import db_read_to_df
from dvh_tools.cloud_functions import get_gsm_secret
from dvh_tools.knada_vm_user import set_environ


def get_comments_from_oracle(
        *,
        project_id=None,
        secret_name=None,
        sources_yml_path="dbt/models/staging/sources.yml"
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
        sources_yml_path (str): Path til soruces.yml. Defaults to "../models/staging/sources.yml".
    
    Returns:
        None
    """
    # %%
    # henter hemmeligheter fra Google Secret Manager. trenger DSN
    print("setter hemmeligheter for Oracle tilkobling")
    if project_id is None or secret_name is None:
        print("Mangler prosjekt-ID og/eller hemmelighetsnavn")
        exit(1)
    secret_dict = get_gsm_secret(project_id, secret_name)
    set_environ()

    # %%
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
            print(f"Finner ikke yaml-filen hvor sources er spesifisert i models/staging")
            print(f"Prøvde å lese fra: {source_file}")
            print(f"Endre argumentet 'sources_yml_path' til riktig path, som nå er: {sources_yml_path}")
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
    schema_table_dict = find_all_sources_from_yml()

    # %%
    # sql-er mot Oracle for tabell- og kolonnekommentarer
    def sql_table_comment(schema_name: str, table_name: str) -> str:
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
            return " "
        else:
            # fjerner fnutter, fordi det skaper problemer senere
            return sql_result.iloc[0, 0].replace("'", "").replace('"', "")

    def sql_columns_comments(schema_name: str, table_name: str) -> dict:
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
        df_col_comments["comments"] = df_col_comments["comments"].fillna(" ")
        return df_col_comments

    # %%
    # get table descriptions
    print("Henter tabellbeskrivelser fra Oracle")
    source_table_descriptions = {}  # antar at det ikke finnes tabeller med samme navn
    for schema, table_list in schema_table_dict.items():
        for table in table_list:
            source_description = sql_table_comment(schema, table)
            if source_description is None:
                source_description = "(ingen modellbeskrivelse i Oracle)"
            table_description = f"""Staging av {schema}.{table}, med original beskrivelse: {source_description}"""
            source_table_descriptions[f"stg_{table}"] = table_description

    # %%
    # get all column comments
    # uses the first comment if there are multiple comments for the same column
    print("Henter kolonnekommentarer fra Oracle")
    column_comments_dict = {}
    for schema, table_list in schema_table_dict.items():
        for table in table_list:
            df_table_columns_comments = sql_columns_comments(schema, table)
            for index, row in df_table_columns_comments.iterrows():
                column = row["column_name"]
                comment = row["comments"]
                if column not in column_comments_dict:
                    column_comments_dict[column] = comment
    column_comments_dict = dict(sorted(column_comments_dict.items()))

    # %%
    # lage source_comments.yml
    print("Lager 'comments_source.yml'")
    alle_kommentarer = "{\n    source_column_comments: {\n"
    for column, comment in column_comments_dict.items():
        alle_kommentarer += f"""        {column}: "{comment.replace('\n', " | ")}",\n"""
    alle_kommentarer += "    },\n\n    source_table_descriptions: {\n"
    for table, description in source_table_descriptions.items():
        alle_kommentarer += f"""        {table}: "{description.replace('\n', " | ")}",\n"""
    alle_kommentarer += "    }\n}\n"
    with open("comments_source.yml", "w") as file:
        file.write(alle_kommentarer)
    print("Ferdig! 'comments_source.yml' er lagret i samme mappe som denne filen.")
