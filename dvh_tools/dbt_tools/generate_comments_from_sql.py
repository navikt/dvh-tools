import logging
import os
import sys
from pathlib import Path

from yaml import dump, safe_dump, safe_load

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


def missing_model_comments(data, yml_file, *, require_descriptions=True):
    issues = []
    if not isinstance(data, dict) or not any(key in data for key in ("models", "snapshots")):
        return [f"{yml_file}: missing or empty 'models' list"]
    for resource_key in ("models", "snapshots"):
        if resource_key not in data:
            continue
        resources = data[resource_key]
        if not isinstance(resources, list) or not resources:
            issues.append(f"{yml_file}: missing or empty '{resource_key}' list")
            continue
        resource_type = "snapshot" if resource_key == "snapshots" else "model"
        for resource_index, resource in enumerate(resources, start=1):
            if not isinstance(resource, dict):
                issues.append(f"{yml_file}: {resource_type} entry #{resource_index} must be a mapping")
                continue
            resource_name = resource.get("name") or f"<{resource_type} #{resource_index}>"
            location = f"{yml_file}: {resource_type} '{resource_name}'"
            if not isinstance(resource.get("name"), str) or not resource["name"].strip():
                issues.append(f"{yml_file}: {resource_type} entry #{resource_index} is missing 'name'")
            description = resource.get("description")
            if require_descriptions and (not isinstance(description, str) or not description.strip()):
                issues.append(f"{location} is missing a non-empty description")
            if not isinstance(resource.get("columns"), list):
                issues.append(f"{location} is missing a 'columns' list")
                continue
            for column_index, column in enumerate(resource["columns"], start=1):
                if not isinstance(column, dict):
                    issues.append(f"{location}, column entry #{column_index} must be a mapping")
                    continue
                column_name = column.get("name") or f"<column #{column_index}>"
                if not isinstance(column.get("name"), str) or not column["name"].strip():
                    issues.append(f"{location}, column entry #{column_index} is missing 'name'")
                description = column.get("description")
                if require_descriptions and (not isinstance(description, str) or not description.strip()):
                    issues.append(f"{location}, column '{column_name}' is missing a non-empty description")
    return issues


def make_yml_string(data, yml_file="<model YAML>"):
    issues = missing_model_comments(data, yml_file)
    if issues:
        for issue in issues:
            logger.error(issue)
        raise ValueError("Missing model documentation. Fix the reported fields and rerun; this YAML was not written.")
    return safe_dump({"version": data.get("version", 2), **data}, sort_keys=False, allow_unicode=True)


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


def update_yamls_from_sqls_in_dir(files_and_dirs: list, dir_path=None, *, yaml_updates=None) -> None:
    if dir_path is None:
        raise ValueError("A model directory path is required.")
    dir_path = Path(dir_path)
    sql_files = [f for f in files_and_dirs if f.endswith(".sql")]
    yml_file = [f for f in files_and_dirs if f.endswith(".yml")]
    skip_yml = ['sources.yml', 'sources_with_comments.yml']
    yml_file = [f for f in yml_file if f not in skip_yml]

    if yml_file:
        yml_path = dir_path / yml_file[0]
        with open(yml_path, "r", encoding="utf-8") as file:
            yml_dict = safe_load(file)
        if missing_model_comments(yml_dict, yml_path, require_descriptions=False):
            if yaml_updates is not None:
                yaml_updates[yml_path] = yml_dict
                return
            make_yml_string(yml_dict, str(yml_path))
    elif sql_files:
        yml_path = Path(dir_path) / f"_{Path(dir_path).name}_models.yml"
        yml_dict = {"version": 2, "models": []}
    else:
        return

    if sql_files and "models" in yml_dict:
        snapshot_names = {snapshot["name"] for snapshot in yml_dict.get("snapshots", [])}
        sql_dict = {}
        for file in sql_files:
            if file[:-4] in snapshot_names:
                continue
            logger.info(f"Reading column names from SQL file: {dir_path / file}")
            sql_dict[file[:-4]] = find_sql_columns(dir_path / file)
        update_yml_dict(yml_dict=yml_dict, sql_dict=sql_dict, yml_file=str(yml_path))

    if yaml_updates is not None:
        yaml_updates[yml_path] = yml_dict
        return
    yml_string = make_yml_string(yml_dict, str(yml_path))
    with open(yml_path, "w", encoding="utf-8") as file:
        file.write(yml_string)


def run_yml_update_in_dir(*, models_path, yaml_updates=None):
    all_model_dirs = [models_path] + sorted(d for d in models_path.rglob('*') if d.is_dir())
    for dir_path in all_model_dirs:
        files_and_dirs = os.listdir(dir_path)
        update_yamls_from_sqls_in_dir(files_and_dirs, dir_path, yaml_updates=yaml_updates)


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
    if not models_path.is_dir():
        logger.error(f"Models directory not found: {models_path}. Check models_path; no model YAML files were written.")
        sys.exit(1)

    yaml_updates = {}
    run_yml_update_in_dir(models_path=models_path, yaml_updates=yaml_updates)
    for yaml_file in models_path.rglob("*.yml"):
        if yaml_file.name in ["sources.yml", "sources_with_comments.yml"] or yaml_file in yaml_updates:
            continue
        with open(yaml_file, "r", encoding="utf-8") as file:
            yaml_updates[yaml_file] = safe_load(file)
    yaml_files = sorted(yaml_updates)
    structure_issues = []
    for yaml_file, data in yaml_updates.items():
        structure_issues.extend(missing_model_comments(data, yaml_file, require_descriptions=False))
    if structure_issues:
        for issue in structure_issues:
            logger.error(issue)
        logger.error("Validation failed. No model YAML files were written. Fix the reported model structure and rerun.")
        sys.exit(1)

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
            sys.exit(1)
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
            sys.exit(1)

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
        yml = yaml_updates[file]
        for model in yml.get("models", []) + yml.get("snapshots", []):
            model_name = model["name"]
            description = model.get("description")
            if isinstance(description, str) and description.strip():
                table_descriptions[model_name] = description
            for column in model["columns"]:
                column_name = column["name"]
                description = column.get("description")
                if isinstance(description, str) and description.strip() and column_name not in kolonner_navn:
                    kolonner_navn.append(column_name)
                    kolonner_kommentar.append(description)
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

    issues = []
    for yaml_file, data in yaml_updates.items():
        for resource_key in ("models", "snapshots"):
            if resource_key in data:
                data[resource_key].sort(key=lambda resource: resource["name"])
        for model in data.get("models", []) + data.get("snapshots", []):
            model_name = model["name"]
            if model_name in table_descriptions:
                model["description"] = table_descriptions[model_name]
            for column in model["columns"]:
                column_name = column["name"]
                if column_name in column_descriptions and (
                    not endre_bare_tomme_kommentarer or not column.get("description")
                ):
                    column["description"] = column_descriptions[column_name]
        issues.extend(missing_model_comments(data, yaml_file))
    if issues:
        for issue in issues:
            logger.error(issue)
        logger.error(
            "Validation failed. No model YAML files were written. "
            "Add descriptions in the reported YAML files or in comments_custom.yml "
            "(custom_table_descriptions / custom_column_comments), then rerun."
        )
        sys.exit(1)

    formatted_yamls = {path: make_yml_string(data, str(path)) for path, data in yaml_updates.items()}
    for yaml_file, content in formatted_yamls.items():
        with open(yaml_file, "w", encoding="utf-8") as file:
            logger.info(f"writing:{yaml_file}")
            file.write(content)


if __name__ != "__main__":
    generate_comments_from_sql()
