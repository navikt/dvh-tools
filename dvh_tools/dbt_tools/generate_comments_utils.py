import os
import glob
from yaml import safe_load

def make_yml_string(yml: dict):
    """Converts a YAML dictionary into a YAML-formatted string suitable for writing to a file.

    This function takes a dictionary representing YAML data and formats it into a YAML string.
    The dictionary must contain a 'version' and 'models' key. The function handles various types 
    of data within the dictionary, including strings, lists, and nested dictionaries.

    Args:
        yml (dict): A dictionary representing the YAML data. Must include:
            - 'version': The version number of the YAML format (default is "2" if not provided).
            - 'models': A list of model dictionaries, each with:
                - 'name': The name of the model.
                - Other optional keys such as 'description', 'columns', etc.
                - 'columns': A list of column dictionaries, each with:
                    - 'name': The name of the column.
                    - Other optional keys such as 'description', etc.

    Returns:
        yml_string (str): A string formatted in YAML syntax, ready to be written to a file.

    Examples:
        >>> yml_data = {
        ...     "version": "2",
        ...     "models": [
        ...         {
        ...             "name": "model_1",
        ...             "description": "This is a description.",
        ...             "columns": [
        ...                 {"name": "column_1", "description": "Column 1 description."},
        ...                 {"name": "column_2"}
        ...             ]
        ...         }
        ...     ]
        ... }
        >>> yml_string = make_yml_string(yml_data)
        >>> print(yml_string)
        version: 2

        models:
          - name: model_1
            description: >
              This is a description.
            columns:
              - name: column_1
                description: 'Column 1 description.'
              - name: column_2
    """
    fallback_v = "2"
    try:
        yml_version = yml["version"]
    except KeyError:
        print(f"Ingen 'version' i yml-filen. Bruker fallback-versjon: {fallback_v}")
        yml_version = fallback_v
    yml_string = f"version: {yml_version}\n\nmodels:\n"

    # Loop over tables
    for tab in yml["models"]:
        tab_keys = tab.keys()
        yml_string += f"  - name: {tab['name']}\n" # Need a name-section
        indent_4 = "    " # For tables
        indent_6 = "      " # For config of tables and columns
        indent_8 = "        " # For config of columns
        indent_10 = "          " # For config of columns and lists/dictionaries
        for key in tab_keys:
            if key == "name" or key == "columns":
                continue
            elif key == "description":
                yml_string += f"{indent_4}{key}: >\n{indent_6}{tab[key].strip()}\n"
            elif type(tab[key]) == str:
                yml_string += f"{indent_4}{key}: {tab[key].strip()}\n"
            elif type(tab[key]) == list:
                yml_string += f"{indent_4}{key}:\n"
                for list_item in tab[key]:
                    yml_string += f"{indent_6}- {list_item}\n"
            elif type(tab[key]) == dict:
                yml_string += f"{indent_4}{key}:\n"
                for ik, iv in tab[key].items():
                    yml_string += f"{indent_6}{ik}: {iv}\n"
            else:
                print(f"Ukjent type for {key} i {tab['name']}. Type: {type(tab[key])}")

        # Loop over columns
        yml_string += indent_4 + "columns:\n"
        for col in tab["columns"]:
            yml_string += f"{indent_6}- name: {col['name']}\n"
            for ckey in col.keys():
                if ckey == "name":
                    continue
                elif ckey == "description":
                    clean_desk = col["description"].strip().replace('"', '').replace("'", "")
                    yml_string += f"{indent_8}description: '{clean_desk}'\n"
                elif type(col[ckey]) == str:
                    yml_string += f"{indent_8}{ckey}: {col[ckey].strip()}\n"
                elif type(col[ckey]) == list:
                    yml_string += f"{indent_8}{ckey}:\n"
                    for col_list_item in col[ckey]:
                        yml_string += f"{indent_10}- {col_list_item}\n"
                elif type(col[ckey]) == dict:
                    yml_string += f"{indent_8}{ckey}:\n"
                    for ik, iv in col[ckey].items():
                        yml_string += f"{indent_10}{ik}: {iv}\n"
                else:
                    print(f"Ukjent type for {col} i {tab['name']}. Type: {type(col[ckey])}")
        yml_string += "\n"
    return yml_string


def find_sql_columns(file):
    """Extracts column names from a SQL file.

    This function reads a SQL file and identifies column names based on the SQL query structure.
    It handles two types of SQL statements: those ending with a `final as` clause and flat select statements.
    The function returns a list of column names found in the SQL file.

    Args:
        file (str or Path): The path to the SQL file from which to extract column names.

    Returns:
        list: A list of column names extracted from the SQL file.

    Raises:
        ValueError: If the SQL file does not follow the expected format or cannot be read.

    Examples:
        >>> find_sql_columns("path/to/your_sql_file.sql")
        ['column1', 'column2', 'column3']

    TODO:
        - Add support for handling leading commas in `with` clause SQL statements:
            funker ikke å splitte på "." hvis det er en kommentar på linja
                column = column.lower()
                if "." in column:  # search for ".", if the column is aliased
                    column = column.split(".")[1]
        - Improve handling of column comments to ensure they are properly ignored or parsed.
        - Refactor to handle SQL files with complex structures or multiple `select` statements.
    """
    with open(file, "r") as file:
        content = file.readlines()

    # Two alternatives:
    # 1. the with clause, finding "final as(\n"  # todo: add support leading comma
    # 2. flat select statements, finding "select\n"
    model_columns = []
    try:
        # 1. with-clause
        if "select * from final\n" in content:
            # Allocate the lines between "    select" and "    from ..."
            select_line = content.index("final as (\n")
            read_from_index = select_line + 2
        else: # Flat select
            select_line = content.index("select\n")
            read_from_index = select_line + 1

        for column in content[read_from_index:]:
            if column.strip().startswith("from"):
                break # Stop when reaching "from" in the .sql-file
            elif column.strip().startswith("--"):
                continue # Skip commented lines
            elif column.strip().startswith("*"):
                print(f"\nError reading {file.name}")
                print("Do not end with 'select *' statements")
                print("Finish with explicit 'final as(' statement or a flat select")
                print("The final version requires the line: 'select * from final\\n'")
                exit()
            if column.count("--") > 0:
                # If the column has a comment, split on the first "--"
                column = column.split("--")[0].strip().replace(",", "")
            try: # When aliasing
                column.split(" as ")[1]
                column_name = column.split(" as ")[1].strip().replace(",", "")
                model_columns.append(column_name)
            except IndexError:  # all normal columns
                column_name = column.strip().replace(",", "")
                model_columns.append(column_name)
    except ValueError as e:
        print(f"\nError reading {file.name}")
        print("Make sure to follow the standard structure of the sql-files,")
        print("i.e. use the with clause and 'final as(', or flat select statements")
        print(e)
        exit()
    return model_columns


def empty_model_dict(model_name: str):
    """Creates an empty model dictionary with a specified model name.

    This function initializes a dictionary for a model with a given name. The dictionary includes
    default values for the model's description and columns, making it useful for initializing model
    configurations in data processing workflows.

    Args:
        model_name (str): The name of the model to be included in the dictionary.

    Returns:
        dict: A dictionary with the model's name, an empty description, and an empty list of columns.

    Examples:
        >>> empty_model_dict("example_model")
        {'name': 'example_model', 'description': '', 'columns': []}
    """
    return {"name": model_name, "description": "", "columns": []}


def update_yml_dict(*, yml_dict: dict, sql_dict: dict, yml_file: str) -> None:
    """Updates the YAML dictionary by adding or removing models and columns based on the SQL dictionary.

    This function synchronizes a YAML dictionary with a SQL dictionary by comparing model and column names. 
    It adds new models or columns that exist in the SQL dictionary but not in the YAML dictionary, 
    and removes models or columns that exist in the YAML dictionary but are not present in the SQL dictionary.

    Args:
        yml_dict (dict): A dictionary representing the current state of the YAML file. It should contain a 
                         "models" key with a list of model dictionaries, where each model has a "name" and "columns".
        sql_dict (dict): A dictionary representing the state of the SQL files. It maps model names to a list of column names.
        yml_file (str): The filename of the YAML file being updated. This is used for logging purposes.

    Examples:
        >>> yml_dict = {
        ...     "models": [
        ...         {"name": "model1", "columns": [{"name": "col1", "description": ""}]},
        ...         {"name": "model2", "columns": [{"name": "col2", "description": ""}]}
        ...     ]
        ... }
        >>> sql_dict = {
        ...     "model1": ["col1", "col3"],
        ...     "model3": ["col4"]
        ... }
        >>> update_yml_dict(yml_dict=yml_dict, sql_dict=sql_dict, yml_file="models.yml")
        Appending model3 to models.yml
        Popping model2 from models.yml
        Appending col3 to model1
        Appending col4 to model3
    """
    yml_mod_names = [model["name"] for model in yml_dict["models"]]
    for sql_model in sql_dict:
        if sql_model in yml_mod_names:
            continue
        else:
            print(f"Appending {sql_model} to {yml_file}")
            yml_dict["models"].append(empty_model_dict(sql_model))
    # Model in yml, but not in sql
    for i, yml_model_n in enumerate(yml_mod_names):
        if yml_model_n not in sql_dict:
            print(f"Popping model {yml_model_n} from {yml_file}")
            yml_dict["models"].pop(i)
            break  # Add break statement to exit the loop after popping the model
    # Updating the columns
    for model in yml_dict["models"]:
        model_name = model["name"]
        model_cols = model["columns"]
        model_col_names = [col["name"] for col in model_cols]
        # Create a new list of columns to keep
        new_model_cols = [col for col in model_cols if col["name"] in sql_dict[model_name]]
        # Print columns that are being removed
        for col in model_cols:
            if col["name"] not in sql_dict[model_name]:
                print(f"Popping {col['name']} from {model_name} in {yml_file}")
        # Replace the original list with the new list
        model["columns"] = new_model_cols
        # Append new columns from sql_dict
        for sql_col in sql_dict[model_name]:
            if sql_col not in model_col_names:
                print(f"Appending {sql_col} to {model_name}")
                model["columns"].append({"name": sql_col, "description": ""})


def update_yamls_from_sqls_in_dir(files_and_dirs: list, dir_path: str = None) -> None:
    """
    Updates YAML files based on SQL files in the specified directory.

    This function processes SQL and YAML files found in the given directory. 
    For each SQL file, it extracts model and column information and updates the corresponding YAML file. 
    If no YAML file is present, it creates a new dummy YAML file with the information extracted from SQL files.

    Args:
        files_and_dirs (list): A list of filenames and directories to be processed. The list should contain both SQL and YAML files.
        dir_path (str, optional): The path to the directory containing the files. If not specified, the function assumes the files are in the current working directory.

    Examples:
        Updating existing YAML file based on SQL files.
        >>> update_yamls_from_sqls_in_dir(
        ...     files_and_dirs=["model1.sql", "model2.sql", "existing_model.yml"],
        ...     dir_path="path/to/dir"
        ... )
        # Updates 'existing_model.yml' with columns and models found in 'model1.sql' and 'model2.sql'.
    """
    sql_files = [f for f in files_and_dirs if f.endswith(".sql")]
    yml_file = [f for f in files_and_dirs if f.endswith(".yml")]
    skip_yml = ['sources.yml', 'sources_with_comments.yml']
    yml_file = [f for f in yml_file if f not in skip_yml]

    sql_dict = {} # Models as keys, columns as values
    if len(sql_files) > 0:
        for file in sql_files:
            file_name = file[: -len(".sql")]
            with open(dir_path + "/" + file, "r") as f:
                model_columns = find_sql_columns(dir_path + "/" + file)
                sql_dict[file_name] = model_columns

    if len(yml_file) > 0:
        with open(dir_path + "/" + yml_file[0], "r") as f:
            yml_dict = safe_load(f)
        try:
            yml_models_dict = yml_dict["models"]
        except KeyError:
            print(f"No 'models' in {yml_file[0]}")
            yml_models_dict = None

        if yml_models_dict:
            update_yml_dict(yml_dict=yml_dict, sql_dict=sql_dict, yml_file=yml_file[0])
            yml_string = make_yml_string(yml_dict)
            with open(dir_path + "/" + yml_file[0], "w") as f:
                f.write(yml_string)

    # If no YAML file is present but SQL files are found
    if len(yml_file) == 0 and len(sql_files) > 0:
        print(f"Creating the missing yaml file in {dir_path}")
        # First, make a dummy variable yml_dict
        yml_dict = {"version": "2", "models": [{"name": "dummy", "columns": [{"name": "aarmnd"}]}]}
        update_yml_dict(yml_dict=yml_dict, sql_dict=sql_dict, yml_file="dummy.yml")
        yml_string = make_yml_string(yml_dict)
        new_file_name = "/_" + dir_path.split("/")[-1] + "_models.yml"
        with open(dir_path + new_file_name, "w") as f:
            f.write(yml_string)


def run_yml_update_in_dir(*, models_path: str):
    """Searches for directories within the specified models directory and updates YAML files based on SQL files.

    This function scans through all directories under the provided `models_path` directory. 
    For each directory found, it identifies SQL and YAML files, and then uses `update_yamls_from_sqls_in_dir`
    to update the YAML files with column information extracted from the SQL files.

    Args:
        models_path (str): The path to the root directory containing model subdirectories with SQL and YAML files.

    Examples:
        Update YAML files in the specified directory and its subdirectories.
        >>> run_yml_update_in_dir(models_path="path/to/models")
        # This will process all subdirectories of 'path/to/models', updating YAML files with data from SQL files.
    """
    all_model_dirs = glob.glob(models_path + "/**/", recursive=True)
    for dir_path in all_model_dirs:
        files_and_dirs = os.listdir(dir_path)
        update_yamls_from_sqls_in_dir(files_and_dirs, dir_path)
