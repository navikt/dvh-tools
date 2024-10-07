import glob
from pathlib import Path
from yaml import safe_load
from dvh_tools.dbt_tools.generate_comments_utils import run_yml_update_in_dir, make_yml_string

def generate_comments_from_sql(*, models_path="dbt/models", docs_path="dbt/docs") -> None:
    """Updates YAML files with comments from SQL files.

    This function performs the following steps:
    1. Updates YAML files according to SQL files by adding or removing columns/models based on the SQL file structure.
    2. Reads custom comments from `comments_custom.yml` and source comments from `comments_source.yml`.
    3. Extracts column and table descriptions from the YAML files and updates them with custom and source comments.
    4. Writes the updated comments back to the YAML files.

    The function assumes it is run from the project directory, where it will look for YAML files and update them.

    Args:
        models_path (str): Path to the directory containing model YAML files. Defaults to "dbt/models".
        docs_path (str): Path to the directory containing documentation YAML files. Defaults to "dbt/docs".

    Raises:
        FileNotFoundError: If the `comments_custom.yml` or `comments_source.yml` files are not found.

    Examples:
        To use the function, you can call it from your main script or entry point:
        
        ```
        generate_comments_from_sql(models_path="path/to/models", docs_path="path/to/docs")
        ```

        This will update YAML files in the specified directories with comments based on the SQL files and custom configurations.
    """
    def find_project_root(current_path):
        """Recursively find the project's root directory by looking for a specific marker (e.g., '.git' folder)."""
        if (current_path / '.git').exists():
            return current_path
        else:
            return find_project_root(current_path.parent)
        
    project_root = find_project_root(Path(__file__).resolve())
    models_path = str(project_root / models_path) + "/"
    yaml_files = glob.glob(models_path + "**/*.yml", recursive=True)

    # Updates YAML-files according to SQL-files (i.e. adds/removes columns/models based on the SQL-filestructure)
    run_yml_update_in_dir(models_path=models_path)

    overskriv_yml_med_custom = True # Overwrite the YAML files with custom_comments
    endre_bare_tomme_kommentarer = False  # Only modify empty comments, or all

    column_descriptions = {}
    table_descriptions = {}

    try: # Read custom column comments
        with open(str(project_root / docs_path / "comments_custom.yml")) as f:
            custom_comments = safe_load(f)
            custom_column_comments = custom_comments["custom_column_comments"]
            custom_table_descriptions = custom_comments["custom_table_descriptions"]
    except Exception as e:
        print(e)
        print("Ha en fil med kommentarer i 'comments_custom.yml'")

    try: # Read source column comments
        with open(str(project_root / docs_path / "comments_source.yml")) as f:
            source_comments = safe_load(f)
            source_column_comments = source_comments["source_column_comments"]
            source_table_descriptions = source_comments["source_table_descriptions"]
            table_descriptions.update(source_table_descriptions)
    except Exception as e:
        print(e)
        print("Fant ikke 'comments_source.yml' som inneholder kommentarer fra source")

    # Collect all column names and descriptions
    kolonner_navn = []
    kolonner_kommentar = []
    for file in yaml_files:
        # Skip "sources.yml"
        if "/sources.yml" in file or "\\sources.yml" in file or "/sources_with_comments.yml" in file or "\\sources_with_comments.yml" in file:
            continue
        with open(file, "r") as f:
            yml = safe_load(f)
            try:
                tabeller = yml["models"]
            except KeyError:
                print(f"KeyError on 'models' in {file}")
                continue
            for t in tabeller:
                t_name = t["name"]
                t_columns = t["columns"]
                if "description" in t:
                    table_descriptions[t_name] = t["description"]
                for c in t_columns:
                    c_name = c["name"]
                    try:
                        c_description = c["description"]
                    except KeyError:
                        print(f"{c_name} har ikke felt for beskrivelse i {t_name}")
                        continue
                    if c_description is None or c_description == "":
                        continue
                    if c_name in kolonner_navn:
                        continue # Only get unique column names and first description
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
        if "/sources.yml" in f or "\\sources.yml" in f or "/sources_with_comments.yml" in f or "\\sources_with_comments.yml" in f:
            continue
        with open(f, "r") as file:
            yml = dict(safe_load(file))
            yml_models = False
            try:
                yml["models"].sort(key=lambda x: x["name"])
                tabeller = yml["models"]
                yml_models = True
            except KeyError:
                print(f"Ingen 'models' i .yml {f}")
                continue
            if yml_models:
                # Loop over DBT models in the YAML file
                for i in range(len(tabeller)):
                    t_name = tabeller[i]["name"]
                    t_columns = tabeller[i]["columns"]
                    if "description" in tabeller[i]:
                        t_desc = tabeller[i]["description"]
                        if t_desc.strip() != table_descriptions[t_name].strip():
                            print(f"Endrer beskrivelse for modell {t_name}")
                            yml["models"][i]["description"] = table_descriptions[t_name]
                    # Loop over columns in a model
                    for c in range(len(t_columns)):
                        c_name = t_columns[c]["name"]
                        overskriv_beskrivelse = False
                        if not endre_bare_tomme_kommentarer:
                            overskriv_beskrivelse = True
                        try:
                            c_desc = t_columns[c]["description"]
                        except KeyError: # No description for the column
                            overskriv_beskrivelse = True
                            c_desc = None
                        if c_name not in column_descriptions:
                            overskriv_beskrivelse = False # Cannot overwrite
                            if c_name not in manglende_kommentarer:
                                manglende_kommentarer.append(c_name)
                        if overskriv_beskrivelse and c_desc != column_descriptions[c_name]:
                            print(f"Endrer beskrivelse for {c_name} i {t_name}")
                            oppdatert_desc = column_descriptions[c_name]
                            yml["models"][i]["columns"][c]["description"] = oppdatert_desc

        # Write each YAML-file
        with open(f, "w") as file:
            file.write(make_yml_string(yml))

    if len(manglende_kommentarer) > 0:
        print("Mangler følgende kolonner i comments_custom.yml:")
        for c_name in manglende_kommentarer:
            print("   ", c_name)
