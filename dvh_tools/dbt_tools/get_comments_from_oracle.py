# %%
from pathlib import Path
from yaml import safe_load
from dvh_tools.oracle import db_read_to_df
from dvh_tools.cloud_functions import get_gsm_secret


def get_comments_from_oracle(
        project_id=None,
        secret_name=None,
        sources_yml_path="../models/staging/sources.yml"
        ):
    """
    Leser alle kilder fra sources.yml, kobler seg til Oracle og henter alle kommentarer.

    Oppdaterer/lager 'comments_source.yml' med tabell- og kolonnekommentarer.

    Args:
        project_id (str, optional): GCP-prosjekt-ID. Defaults to None.
        secret_name (str, optional): Hemmelighetsnavn i GSM. Defaults to None.
        sources_yml_path (str, optional): Path til soruces.yml. Defaults to "../models/staging/sources.yml".
    """

    # %%
    # find the sources.yml file
    def find_all_sources(sources_yml_path=sources_yml_path):
        """Finner alle kilder fra sources.yml."""
        sources_yml_file = sources_yml_path
        source_file = str(Path(__file__).parent / sources_yml_file)

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
            # 'name' og 'schema' er samme. legge inn sjekk?
            schema_table_dict[schema_name] = tables_name_list
        return schema_table_dict

    schema_table_dict = find_all_sources()

    # %%
    # henter hemmeligheter fra Google Secret Manager
    if project_id is None or secret_name is None:
        print("Mangler prosjekt-ID og/eller hemmelighetsnavn")
        exit(1)
    secret_dict = get_gsm_secret(project_id, secret_name)

    # %%
    # sql-er mot Oracle for tabell- og kolonnekommentarer
    def sql_table_comment(schema: str, table: str) -> str:
        """Henter tabellkommentar fra Oracle-databasen.

        Args:
            schema (str): skjemanavn
            table (str): tabellnavn

        Returns:
            str: tabellkommentaren
        """
        sql = f"""
            select
                comments
            from all_tab_comments
            where owner = upper('{schema}') and table_name = upper('{table}')"""
        sql_result = db_read_to_df(sql, secret_dict)
        if sql_result[0][0] == None:
            return " "  # mangler kommenter
        else:
            return sql_result[0][0].replace("'", "").replace('"', "")

    def sql_columns_comments(schema: str, table: str) -> dict:
        """Henter alle kolonnekommentarer til en tabell i databasen.

        Args:
            schema (str): skjemanavn
            table (str): tabellnavn

        Returns:
            dict: kolonnenavn som key, kommentar som value
        """
        sql = f"""
        select
            column_name,
            comments
        from dba_col_comments
        where owner = upper('{schema}') and table_name = upper('{table}')"""
        # sql_result = oracle_connection.sql_read(sql, print_info=False)
        sql_result = db_read_to_df(sql, secret_dict)
        commemnts_dict = {}
        for column in sql_result:
            name = column[0]
            if column[1] == None:
                description = " "  # beskrivelse mangler
            else:
                description = column[1].replace("'", "").replace('"', '').replace('\n', '    ')
            commemnts_dict[name.lower()] = description
        return commemnts_dict


    # %%
    # get table descriptions
    source_table_descriptions = {}  # antar at det ikke finnes tabeller med samme navn
    for schema, table_list in schema_table_dict.items():
        for table in table_list:
            source_description = sql_table_comment(schema, table)
            table_description = f""""Staging av {schema}.{table}, med original beskrivelse: {source_description}"""
            source_table_descriptions[f"stg_{table}"] = table_description

    # %%
    # get all column comments
    source_column_comments = {}

    for schema, table_list in schema_table_dict.items():
        for table in table_list:
            source_columns_dict = sql_columns_comments(schema, table)

            for column, comment in source_columns_dict.items():
                if column not in source_column_comments:
                    source_column_comments[column] = comment

    source_column_comments = dict(sorted(source_column_comments.items()))

    # %%
    # lage source_comments.yml
    alle_kommentarer = "{\n    source_column_comments: {\n"
    for column, comment in source_column_comments.items():
        alle_kommentarer += f"""        {column}: "{comment}",\n"""
    alle_kommentarer += "    },\n    source_table_descriptions: {"
    for table, description in source_table_descriptions.items():
        alle_kommentarer += f"""        {table}: "{comment}",\n"""
    alle_kommentarer += "    }\n}\n"

    with open("comments_source.yml", "w") as file:
        file.write(alle_kommentarer)

