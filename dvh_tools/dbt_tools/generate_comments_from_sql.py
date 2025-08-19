import io
from pathlib import Path
from yaml import safe_load, dump
import os
from loguru import logger
import sys


def make_yml_string(data):
    # Start with the version
    yml_string = f"version: {data.get('version', 2)}\n\nmodels:\n"

    # Iterate over models
    for model in data['models']:
        # Add model name and description
        yml_string += f"  - name: {model['name']}\n"
        yml_string += f"    description: '{model['description']}'\n"

        # Columns section
        yml_string += "    columns:\n"

        for column in model['columns']:
            # Add column name and description
            yml_string += f"      - name: {column['name']}\n"
            yml_string += f"        description: '{column['description']}'\n"

            # Check and add data_tests
            if 'data_tests' in column:
                yml_string += "        data_tests:\n"
                for test in column['data_tests']:
                    if isinstance(test, str):
                        yml_string += f"          - {test}\n"
                    elif isinstance(test, dict):
                        for key, value in test.items():
                            yml_string += f"          - {key}:\n"
                            if isinstance(value, dict):
                                for sub_key, sub_value in value.items():
                                    yml_string += f"              {sub_key}: '{sub_value}'\n"
                            else:
                                yml_string += f"              {value}\n"

    return yml_string


def find_sql_columns(file):
    with open(file, "r", encoding="utf-8") as file:
        content = [line.lower() for line in file.readlines()]

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
        else:  # Flat select
            select_line = content.index("select\n")
            read_from_index = select_line + 1

        for column in content[read_from_index:]:
            if column.strip().startswith("from"):
                break  # Stop when reaching "from" in the .sql-file
            elif column.strip().startswith("--"):
                continue  # Skip commented lines
            elif column.strip().startswith("*"):
                logger.info(f"\nError reading {file.name}")
                logger.info("Do not end with 'select *' statements")
                logger.info("Finish with explicit 'final as(' statement or a flat select")
                logger.info("The final version requires the line: 'select * from final\\n'")
                exit()
            if column.count("--") > 0:
                # If the column has a comment, split on the first "--"
                column = column.split("--")[0].strip().replace(",", "")
            try:  # When aliasing
                column.split(" as ")[1]
                column_name = column.split(" as ")[1].strip().replace(",", "")
                model_columns.append(column_name)
            except IndexError:  # all normal columns
                column_name = column.strip().replace(",", "")
                model_columns.append(column_name)
    except ValueError as e:
        logger.info(f"\nError reading {file.name}")
        logger.info("Make sure to follow the standard structure of the sql-files,")
        logger.info("i.e. use the with clause and 'final as(', or flat select statements")
        logger.info(e)
        exit()
    return model_columns


def empty_model_dict(model_name: str):
    return {"name": model_name, "description": "", "columns": []}


def update_yml_dict(*, yml_dict: dict, sql_dict: dict, yml_file: str) -> None:
    yml_mod_names = [model["name"] for model in yml_dict["models"]]
    for sql_model in sql_dict:
        if sql_model in yml_mod_names:
            continue
        else:
            logger.info(f"Appending {sql_model} to {yml_file}")
            yml_dict["models"].append(empty_model_dict(sql_model))
    # Model in yml, but not in sql
    for i, yml_model_n in enumerate(yml_mod_names):
        if yml_model_n not in sql_dict:
            logger.info(f"Popping model {yml_model_n} from {yml_file}")
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
                logger.info(f"Popping {col['name']} from {model_name} in {yml_file}")
        # Replace the original list with the new list
        model["columns"] = new_model_cols
        # Append new columns from sql_dict
        for sql_col in sql_dict[model_name]:
            if sql_col not in model_col_names:
                logger.info(f"Appending {sql_col} to {model_name}")
                model["columns"].append({"name": sql_col, "description": ""})


def update_yamls_from_sqls_in_dir(files_and_dirs: list, dir_path: str = None) -> None:
    sql_files = [f for f in files_and_dirs if f.endswith(".sql")]
    yml_file = [f for f in files_and_dirs if f.endswith(".yml")]
    skip_yml = ['sources.yml', 'sources_with_comments.yml']
    yml_file = [f for f in yml_file if f not in skip_yml]

    sql_dict = {}  # Models as keys, columns as values
    if len(sql_files) > 0:
        for file in sql_files:
            logger.info(f" Reading column names from sql file: {file}")
            file_name = file[:-4]  # Remove the .sql extension
            sql_path = dir_path / file
            with open(sql_path, "r", encoding="utf-8") as f:
                model_columns = find_sql_columns(sql_path)
                sql_dict[file_name] = model_columns

    if yml_file:
        # Build the full path to the YAML file
        yml_path = dir_path / yml_file[0]
        # Load the YAML file
        try:
            with open(yml_path, "r", encoding='utf-8') as f:
                yml_dict = safe_load(f)
        except FileNotFoundError:
            logger.info(f'File not found: {yml_file[0]}')
            raise
        except Exception as e:
            logger.info(f'Error reading YAML file {yml_file[0]: {e}}')
            raise

        # Handle the case of an empty or invalid YAML file
        if yml_dict:
            # Check if the 'models' section exists
            if "models" not in yml_dict or not yml_dict["models"]:
                logger.info(f"No 'models' found in {yml_file[0]}. Please delete the file and rerun this program.")
                return
            else:
                logger.info(f'models section found in {yml_file[0]} file, updating column names with .sql files')
                update_yml_dict(yml_dict=yml_dict, sql_dict=sql_dict, yml_file=yml_file[0])
                yml_string = make_yml_string(yml_dict)
                with open(yml_path, "w", encoding="utf-8") as f:
                    f.write(yml_string)
        else:
            logger.info(f"The YAML file {yml_file[0]} is empty or invalid. Deleting the file.")
            # Delete the empty or invalid YAML file
            try:
                yml_path.unlink()  # Deletes the file
                logger.info(f"Deleted empty or invalid YAML file: {yml_file[0]}")

                # Remove the file from the yml_file list
                yml_file = [f for f in yml_file if f != yml_file[0]]
            except Exception as e:
                logger.error(f"Error deleting YAML file {yml_file[0]}: {e}")

    # If no YAML file is present but SQL files are found

    # logger.info(f'{len(yml_file)}')
    if len(yml_file) == 0 and len(sql_files) > 0:
        logger.info(f"Creating the missing yaml file in {dir_path}")

        # Create a dummy YAML structure
        yml_dict = {"version": "2", "models": [{"name": "dummy", "columns": [{"name": "aarmnd"}]}]}

        # Update the dummy YAML structure with the provided SQL data
        update_yml_dict(yml_dict=yml_dict, sql_dict=sql_dict, yml_file="dummy.yml")

        # Convert the dictionary to a YAML-formatted string
        yml_string = make_yml_string(yml_dict)

        # Construct the new file name
        new_file_name = f"_{Path(dir_path).name}_models.yml"
        # new_file_name = "/_" + dir_path.split("/")[-1] + "_models.yml"

        # Create the full path for the new YAML file (Path handles platform-specific separators)
        new_file_path = Path(dir_path) / new_file_name

        try:
            # Write the string to the new YAML file
            with open(new_file_path, "w", encoding="utf-8") as file:
                file.write(yml_string)
            logger.info(f"YAML file successfully created: {new_file_path}")
        except Exception as e:
            logger.error(f"Error writing YAML file {new_file_path}: {e}")


def run_yml_update_in_dir(*, models_path: str):
    all_model_dirs = [d for d in models_path.rglob('*') if d.is_dir()]
    for dir_path in all_model_dirs:
        files_and_dirs = os.listdir(dir_path)
        update_yamls_from_sqls_in_dir(files_and_dirs, dir_path)


def generate_comments_from_sql(*, models_path="dbt/models", docs_path="dbt/docs") -> None:
    def find_project_root(current_path):
        while current_path != current_path.parent:  # Continue until reaching the root of the filesystem
            if (current_path / '.git').exists():
                logger.info(f"Project root found at: {current_path}")
                return current_path
            current_path = current_path.parent
        raise ValueError("Could not find project root")

    # step 1: Finding project_root
    project_root = find_project_root(Path(__file__).resolve())

    # Step 2: Creating Path for models folder.
    models_path = (project_root / models_path).resolve()
    logger.info(f"Project model path is: {models_path}")

    # Step 3: Recursive search for .yml files in models folder
    yaml_files = list(models_path.rglob("*.yml"))

    # Updates YAML-files (only column names -not description) according to SQL-files (i.e. adds/removes columns/models based on the SQL-filestructure)
    run_yml_update_in_dir(models_path=models_path)

    overskriv_yml_med_custom = True  # Overwrite the YAML files with custom_comments

    # True: all empty comments will be modified with custom_comments.
    # False: all comments should be modified, not just empty ones with custom_comments.
    endre_bare_tomme_kommentarer = False

    column_descriptions = {}
    table_descriptions = {}
    # Read comments_custom.yml file (if not exist create it)
    # Define the default structure for the YAML file if not exists
    default_content = {
        "custom_column_comments": {},
        "custom_table_descriptions": {}
    }

    """
    Reading comments_custom.yml
    """
    file_path = project_root / docs_path / "comments_custom.yml"

    if file_path.exists():
        # If the file exists, read it
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                custom_comments = safe_load(f)
                custom_column_comments = custom_comments["custom_column_comments"]
                custom_table_descriptions = custom_comments["custom_table_descriptions"]
        except Exception as e:
            logger.info(e)
            logger.info(f"Kunne ikke lese '{file_path}'.")
    else:
        # If the file doesn't exist, create it with default content
        try:
            with open(file_path, "w", encoding="utf-8") as f:
                dump(default_content, f, default_flow_style=True)
            custom_column_comments = default_content["custom_column_comments"]
            custom_table_descriptions = default_content["custom_table_descriptions"]
            logger.info(f"File '{file_path}' created with default content. Remember to fill relevant information in this file and rerun the current program.")
        except Exception as e:
            logger.info(e)
            logger.info(f"Kunne ikke opprette '{file_path}'.")

    """
    Reading comments_source.yml
    """
    file_path = project_root / docs_path / "comments_source.yml"
    # If the file exists, read it
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            source_comments = safe_load(f)
            source_column_comments = source_comments["source_column_comments"]
            source_table_descriptions = source_comments["source_table_descriptions"]
            table_descriptions.update(source_table_descriptions)
    except Exception as e:
        logger.info(e)
        logger.info("Fant ikke 'comments_source.yml' fil som inneholder kommentarer fra source")
        # Define the file path to create file 'get_comments_source.py'
        file_path = project_root / docs_path / "get_comments_source.py"
        # Content for the new file
        content = (
            'from dvh_tools.dbt_tools import get_comments_from_oracle\n'
            '"""Husk å fylle secret_name. Sjekk project_id også by default den er PROD"""\n'
            'get_comments_from_oracle(\n'
            '    project_id="spenn-prod-23e0",\n'
            '    secret_name="", # skriv python servicebruker fra din komponent og kjør den fil\n'
            '    sources_yml_path="dbt/models/sources.yml",\n'
            ')'
        )
        try:
            if file_path.exists():
                logger.info(f"File '{file_path}' already exists. Skipping creation.")
                logger.info('Please execute "get_comments_source.py" file first, and then re-run the current program.')
            else:
                # Create the file and write the content
                with open(file_path, "w", encoding="utf-8") as f:
                    f.write(content)
                logger.info(f"The file '{file_path}' has been created with the necessary instructions.")
                logger.info('Please execute this file first, and then re-run the current program.')
        except Exception as file_error:
            logger.info(f"Kunne ikke opprette filen '{file_path}': {file_error}")

        sys.exit(1)  # Exit after handling the error

    # Collect all column names and descriptions
    kolonner_navn = []
    kolonner_kommentar = []
    logger.info(f'reading {len(yaml_files)} .yml files')
    for file in yaml_files:
        logger.info(f'reading:{Path(file).name}')
        # Skip "sources.yml"
        if os.path.basename(file) in ["sources.yml", "sources_with_comments.yml"]:
            logger.info(f'skip reading {Path(file).name}')
        # if "/sources.yml" in file or "\\sources.yml" in file or "/sources_with_comments.yml" in file or "\\sources_with_comments.yml" in file:
            continue
        # Reading .yml files
        with io.open(file, "r", encoding="utf-8") as f:
            yml = safe_load(f)

            try:
                tabeller = yml["models"]
            except KeyError:
                logger.info(f"KeyError on 'models' in {file}")
                continue
            for t in tabeller:
                t_name = t["name"]
                logger.info(f'       model: {t_name}')
                t_columns = t["columns"]
                if "description" in t:
                    table_descriptions[t_name] = t["description"]
                for c in t_columns:
                    c_name = c["name"]
                    try:
                        c_description = c["description"]
                    except KeyError:
                        logger.info(f"{c_name} har ikke felt for beskrivelse i {t_name}")
                        continue
                    if c_description is None or c_description == "":
                        continue
                    if c_name in kolonner_navn:
                        continue  # Only get unique column names and first description
                    else:
                        kolonner_navn.append(c_name)
                        kolonner_kommentar.append(c_description)
    yml_column_comments = dict(zip(kolonner_navn, kolonner_kommentar))

    # custom > yml > source
    # Overwrites source_column_comments with yml_column_comments
    for col, desc in source_column_comments.items():
        column_descriptions[col] = desc
    # Overwrite database descriptions with YAML
    column_descriptions.update(yml_column_comments)
    # Optionally update with custom_column_comments
    if overskriv_yml_med_custom:
        column_descriptions.update(custom_column_comments)
    # Add new column comments
    for col, desc in custom_column_comments.items():
        column_descriptions[col] = desc
    table_descriptions.update(custom_table_descriptions)

    manglende_kommentarer = []
    # Parse the files and update comments
    for f in yaml_files:
        # Skip "sources.yml"
        if os.path.basename(f) in ["sources.yml", "sources_with_comments.yml"]:
            # if "/sources.yml" in f or "\\sources.yml" in f or "/sources_with_comments.yml" in f or "\\sources_with_comments.yml" in f:
            continue
        with open(f, "r", encoding="utf-8") as file:
            yml = dict(safe_load(file))
            yml_models = False
            try:
                yml["models"].sort(key=lambda x: x["name"])
                tabeller = yml["models"]
                yml_models = True
            except KeyError:
                logger.info(f"Ingen 'models' i .yml {f}")
                continue
            if yml_models:
                # Loop over DBT models in the YAML file
                for i in range(len(tabeller)):
                    t_name = tabeller[i]["name"]
                    t_columns = tabeller[i]["columns"]
                    if "description" in tabeller[i]:
                        t_desc = tabeller[i]["description"]
                        if t_desc.strip() != table_descriptions[t_name].strip():
                            logger.info(f"Endrer beskrivelse for modell {t_name}")
                            yml["models"][i]["description"] = table_descriptions[t_name]
                    # Loop over columns in a model
                    for c in range(len(t_columns)):
                        c_name = t_columns[c]["name"]
                        overskriv_beskrivelse = False
                        if not endre_bare_tomme_kommentarer:
                            overskriv_beskrivelse = True
                        try:
                            c_desc = t_columns[c]["description"]
                        except KeyError:  # No description for the column
                            overskriv_beskrivelse = True
                            c_desc = None
                        if c_name not in column_descriptions:
                            overskriv_beskrivelse = False  # Cannot overwrite
                            if c_name not in manglende_kommentarer:
                                manglende_kommentarer.append(c_name)
                        if overskriv_beskrivelse and c_desc != column_descriptions[c_name]:
                            logger.info(f"Endrer beskrivelse for {c_name} i {t_name}")
                            oppdatert_desc = column_descriptions[c_name]
                            yml["models"][i]["columns"][c]["description"] = oppdatert_desc

        # Write each YAML-file
        with open(f, "w", encoding="utf-8") as file:
            logger.info(f'writing:{Path(f).name}')
            file.write(make_yml_string(yml))

    if len(manglende_kommentarer) > 0:
        logger.info("Mangler følgende kolonner i comments_custom.yml:")
        for c_name in manglende_kommentarer:
            logger.info("   ", c_name)


if __name__ != "__main__":
    generate_comments_from_sql()
