from pathlib import Path
from yaml import safe_load
from dvh_tools.oracle import db_read_to_df
from dvh_tools.cloud_functions import get_gsm_secret


def get_comments_from_oracle(
        *,
        project_id=None,
        secret_name=None,
        sources_yml_path="dbt/models/sources.yml"
        ) -> None:
    """
    Reads source tables from `sources.yml`, connects to Oracle, retrieves comments, 
    and generates a `comments_source.yml` file for model auto-generation.

    This function performs the following tasks:
    1. Retrieves Oracle connection secrets from Google Secret Manager (GSM).
    2. Reads source table definitions from `sources.yml`.
    3. Fetches table and column comments from Oracle database.
    4. Creates a `comments_source.yml` file with table and column comments.

    Assumes the script is run from within the dbt project folder, e.g., `dbt/docs/`,
    where the `comments_source.yml` (output) will be saved.

    Args:
        project_id (Optional[str]): GCP project ID for accessing Google Secret Manager. Defaults to None.
        secret_name (Optional[str]): Secret name in Google Secret Manager for Oracle connection. Defaults to None.
        sources_yml_path (str): Path to the `sources.yml` file. Defaults to "dbt/sources.yml".

    Raises:
        ValueError: If `project_id` or `secret_name` is None.
        FileNotFoundError: If `sources_yml_path` is invalid or the file does not exist.
        RuntimeError: For errors during database operations.

    Examples:
        >>> get_comments_from_oracle(
        ...     project_id="my-gcp-project",
        ...     secret_name="oracle-db-secret",
        ...     sources_yml_path="dbt/sources.yml"
        ... )
        Setter hemmeligheter for Oracle tilkobling
        Finner sources.yml fra: dbt/sources.yml
        Henter tabellbeskrivelser fra Oracle
        Henter kolonnekommentarer fra Oracle
        Lager 'comments_source.yml'
        Ferdig!
    """
    # Retrieve secrets from GSM for Oracle connection
    print("Setter hemmeligheter for Oracle tilkobling")
    if project_id is None or secret_name is None:
        raise ValueError("Mangler prosjekt-ID og/eller hemmelighetsnavn.")
    secret_dict = get_gsm_secret(project_id, secret_name)

    def find_project_root(current_path):
        """Recursively finds the root directory of a project by searching for a specific marker directory.

        This function starts from the given `current_path` and moves up the directory tree, checking for the
        presence of a specific marker directory (e.g., `.git`) to identify the root of the project.

        Args:
            current_path (Path): The starting directory path from which to begin the search.

        Returns:
            Path: The path to the project root directory if the marker directory is found.

        Raises:
            RecursionError: If the root directory is not found and the search exceeds system recursion limits.

        Examples:
            Assuming that the `.git` directory is located at `/home/user/my_project`,
            to find the project root where a `.git` directory is located:
            >>> root_path = find_project_root(Path('/home/user/my_project/subdir'))
            >>> print(root_path)
            /home/user/my_project
        """
        if (current_path / '.git').exists():
            return current_path
        else:
            return find_project_root(current_path.parent)

    def find_all_sources_from_yml(sources_yml_path=sources_yml_path):
        """Finds all source tables listed in the specified `sources.yml` file.

        This function locates the `sources.yml` file from the project's root directory, reads its content,
        and extracts information about source tables. It returns a dictionary mapping schema names to lists
        of table names.

        Args:
            sources_yml_path (str): The path to the `sources.yml` file relative to the project root directory.

        Returns:
            schema_table_dict (dict): A dictionary where keys are schema names and values are lists of table names within those schemas.

        Raises:
            FileNotFoundError: If the `sources.yml` file cannot be found at the specified path.

        Examples:
            Suppose you have a `sources.yml` file located at `dbt/sources.yml` with the following content:

            ```yaml
            version: 2
            sources:
            - name: schema1
                schema: schema1
                tables:
                - name: table1
                - name: table2
            - name: schema2
                schema: schema2
                tables:
                - name: table3
            ```

            You can call the function as follows:

            >>> find_all_sources_from_yml('dbt/sources.yml')
            {'schema1': ['table1', 'table2'], 'schema2': ['table3']}

            This will return a dictionary where `schema1` maps to the list of tables `['table1', 'table2']`
            and `schema2` maps to `['table3']`.
        """
        print("Finner sources.yml fra:", sources_yml_path)
        project_root = find_project_root(Path(__file__).resolve())
        source_file = project_root / sources_yml_path  # Adjust this line if sources_yml_path should not be relative to project_root
        try:
            with open(source_file, "r") as file:
                content = file.read()
        except FileNotFoundError:
            raise FileNotFoundError(f"Cannot find YAML file at: {source_file}")
        
        yml_raw = safe_load(content)
        schema_list = yml_raw["sources"]
        schema_table_dict = {} # schema as key, list of table names as value
        for schema in schema_list:
            if schema["name"] != schema["schema"]:
                print("Obs! Verdiene for name og schema er ulike! Se:", schema)
            schema_name = schema["name"]
            tables_name_list = []
            for table in schema["tables"]:
                tables_name_list.append(table["name"])
            schema_table_dict[schema_name] = tables_name_list
        return schema_table_dict


    def get_table_comments_from_oracle(schema_name: str, table_name: str):
        """Retrieves the table comment from an Oracle database for a specified schema and table.

        This function queries the Oracle database to fetch the comment associated with a particular table
        within a given schema. It removes any single or double quotes from the comment to avoid potential
        issues with text formatting.

        Args:
            schema_name (str): The name of the schema where the table is located.
            table_name (str): The name of the table for which the comment is to be retrieved.

        Returns:
            str: The comment associated with the specified table. Returns an empty string if no comment is found.

        Examples:
            Suppose you have a schema named 'HR' and a table named 'EMPLOYEES'. If the comment for the table 
            is 'Employee records with personal details.', the function call would be:

            >>> get_table_comments_from_oracle('HR', 'EMPLOYEES')
            'Employee records with personal details.'

            If there is no comment for the specified table, the function would return an empty string:

            >>> get_table_comments_from_oracle('HR', 'UNKNOWN_TABLE')
            ''
        """
        sql = f"""select comments from all_tab_comments
            where owner = upper('{schema_name}') and table_name = upper('{table_name}')"""
        sql_result = db_read_to_df(sql, secret_dict)
        if sql_result.empty or sql_result.iloc[0, 0] is None:
            return ""
        else:
            # Removing quotes as they cause problems later
            return sql_result.iloc[0, 0].replace("'", "").replace('"', "")


    def get_column_comments_from_oracle(schema_name: str, table_name: str):
        """Retrieves all column comments for a specified table in an Oracle database schema.

        This function queries the Oracle database to fetch comments for each column in the specified table.
        It processes the comments to remove any single or double quotes and ensures that any missing comments 
        are represented as empty strings.

        Args:
            schema_name (str): The name of the schema where the table is located.
            table_name (str): The name of the table for which column comments are to be retrieved.

        Returns:
            df_col_comments (dict): A dictionary containing two columns:
                - 'column_name': The name of each column in lowercase.
                - 'comments': The comment associated with each column, with quotes removed and missing comments replaced with empty strings.

        Examples:
            Suppose you have a schema named 'HR' and a table named 'EMPLOYEES'. If the table has comments for the columns 'ID' and 'NAME', the function call would return a DataFrame like:

            >>> get_column_comments_from_oracle('HR', 'EMPLOYEES')
            column_name                     comments
            0         id  'Employee ID, used as primary key'
            1       name           'Name of the employee'

            If the table has no comments or the columns are not documented, the DataFrame would have empty strings for comments:

            >>> get_column_comments_from_oracle('HR', 'UNKNOWN_TABLE')
            column_name comments
            0         id        
            1       name        
        """
        sql = f"""select column_name, comments from dba_col_comments
            where owner = upper('{schema_name}') and table_name = upper('{table_name}')"""
        df_col_comments = db_read_to_df(sql, secret_dict)
        df_col_comments["column_name"] = df_col_comments["column_name"].str.lower()
        df_col_comments["comments"] = df_col_comments["comments"].str.replace("'", "").str.replace('"', "")
        df_col_comments["comments"] = df_col_comments["comments"].fillna("")
        return df_col_comments


    print("Henter tabellbeskrivelser fra Oracle")
    schema_table_dict = find_all_sources_from_yml()
    stg_table_descriptions = {}  # Comments for staging models 
    for schema, table_list in schema_table_dict.items():
        for table in table_list:
            source_description = get_table_comments_from_oracle(schema, table).replace("\n", " | ")
            if source_description is None:
                source_description = "(Ingen modellbeskrivelse i Oracle)"
            stg_table_descriptions[f"stg_{table}"] = f"Staging av {schema}.{table}, med original beskrivelse: {source_description}."


    # Fill in the dictionary with unique column comments
    print("Henter kolonnekommentarer fra Oracle")
    column_comments_dict = {}
    for schema, table_list in schema_table_dict.items():
        for table in table_list:
            df_table_columns_comments = get_column_comments_from_oracle(schema, table)
            for _, row in df_table_columns_comments.iterrows():
                # Get unique column comments
                column = row["column_name"]
                comment = row["comments"]
                if column not in column_comments_dict:
                    column_comments_dict[column] = comment.replace('\n', " | ")
    column_comments_dict = dict(sorted(column_comments_dict.items()))
    
    # Create `comments_source.yml` containing staging model and column comments
    print("Lager 'comments_source.yml'")
    alle_kommentarer = "{\n    source_column_comments: {\n"
    for column, comment in column_comments_dict.items():
        alle_kommentarer += f"""        {column}: "{comment}",\n"""
    alle_kommentarer += "    },\n\n    source_table_descriptions: {\n"
    for table, description in stg_table_descriptions.items():
        alle_kommentarer += f"""        {table}: "{description}",\n"""
    alle_kommentarer += "    }\n}\n"

    project_root = find_project_root(Path(__file__).resolve())
    with open(project_root / "dbt/docs/comments_source.yml", "w", encoding="utf-8") as file:
        file.write(alle_kommentarer)
    print("Ferdig!")
