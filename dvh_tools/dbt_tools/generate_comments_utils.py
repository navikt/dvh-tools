import os
from yaml import safe_load

def make_yml_string(yml: dict) -> str:
    """Using a yml dict to make a string to write to a yml file.

    Args:
        yml (dict): A yml dict, must have 'version' and 'models'

    Returns:
        str: A string to write to a yml file
    """
    # antar at yml er en dict med 'version' og 'models'
    # lager output string til 1 yml-fil
    fallback_v = "2"
    try:
        yml_version = yml["version"]
    except KeyError:
        print(f"Ingen 'version' i yml-filen. Bruker fallback-versjon: {fallback_v}")
        yml_version = fallback_v
    yml_string = f"version: {yml_version}\n\nmodels:\n"

    # loop over tabeller
    for tab in yml["models"]:
        tab_keys = tab.keys()
        yml_string += f"  - name: {tab['name']}\n"  # må ha name
        indent_4 = "    "  # for tabell
        indent_6 = "      "  # for konfig til tabell og kolonner
        indent_8 = "        "  # for konfig kolonner
        indent_10 = "          "  # for konfig kolonner med lister/dict
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

        # loop over kolonner
        yml_string += indent_4 + "columns:\n"
        for col in tab["columns"]:
            yml_string += f"{indent_6}- name: {col['name']}\n"
            for ckey in col.keys():
                if ckey == "name":
                    continue
                elif ckey == "description":
                    yml_string += f"{indent_8}description: '{col['description']}'\n"
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


def find_sql_columns(file) -> list:
    # returns a list of the columns in the sql-file
    with open(file, "r") as file:
        content = file.readlines()

    model_columns = []
    # two alternatives:
    ### 1. the with clause, finding "final as(\n"  # todo: add support leading comma
    ### 2. flat select statements, finding "select\n"
    try:
        # 1. with clause
        if content[-1].strip() == "select * from final":
            # find the lines between "    select" and "    from ..."
            select_line = content.index("final as (\n")
            read_from_index = select_line + 2
        else:  # flat select
            select_line = content.index("select\n")
            read_from_index = select_line + 1

        # todo: funker ikke å splitte på "." hvis det er en kommentar på linja
        #     column = column.lower()
        #     if "." in column:  # search for ".", if the column is aliased
        #         column = column.split(".")[1]

        for column in content[read_from_index:]:
            if column.strip().startswith("from"):
                break  # stop when reaching "from" in the sql-file
            elif column.strip().startswith("--"):
                continue  # skip commented lines
            elif column.strip().startswith("*"):
                print(f"\nError reading {file.name}")
                print("Do not end with 'select *' statements")
                print("Finish with explicit 'final as(' statement or a flat select")
                exit()
            elif column.count("--") > 0:
                # if the column has a comment, split on the first "--"
                column_name = column.split("--")[0].strip().replace(",", "")
                model_columns.append(column_name)
            else:
                try:  # when aliasing
                    column.split(" as ")[1]
                    column_name = column.split(" as ")[1].strip().replace(",", "")
                    model_columns.append(column_name)
                except IndexError:  # all normal columns
                    column_name = column.strip().replace(",", "")
                    model_columns.append(column_name)
    except ValueError:
        print(f"\nError reading {file.name}")
        print("Make sure to follow the standard structure of the sql-files,")
        print("i.e. use the with clause and 'final as(', or flat select statements")
        exit()
    return model_columns


def remove_column(yml: dict, model_name: str, column_name: str):
    # remove column from yml
    for i, mod in enumerate(yml["models"]):
        if mod["name"] == model_name:
            for j, col in enumerate(mod["columns"]):
                if col["name"] == column_name:
                    yml["models"][i]["columns"].pop(j)
                    break
            break


def add_column_empty_description(yml: dict, model_name: str, column_name: str):
    # add column to yml
    for i, mod in enumerate(yml["models"]):
        if mod["name"] == model_name:
            yml["models"][i]["columns"].append({"name": column_name, "description": ""})
            break


def empty_model_dict(model_name: str):
    return {"name": model_name, "description": "", "columns": []}


def update_yml_dict(*, yml_dict: dict, sql_dict: dict, yml_file: str) -> None:
    """
    Updates the yml dict by adding or removing models and columns.

    Args:
        yml_dict (dict): dict from the yml file
        sql_dict (dict): dict from the sql files, with models as keys and columns as values
        yml_file (str): file name of the yml file
    """
    yml_mod_names = [model["name"] for model in yml_dict["models"]]

    # new sql model
    for sql_model in sql_dict:
        if sql_model in yml_mod_names:
            continue
        else:
            print(f"Appending {sql_model} to {yml_file} with empty desc")
            yml_dict["models"].append(empty_model_dict(sql_model))

    # model in yml but not in sql
    for i, yml_model_n in enumerate(yml_mod_names):
        if yml_model_n not in sql_dict:
            print(f"Popping model {yml_model_n} from {yml_file}")
            yml_dict["models"].pop(i)

    # updating the columns
    for model in yml_dict["models"]:
        model_name = model["name"]
        model_cols = model["columns"]
        model_col_names = [col["name"] for col in model_cols]
        for col in model_cols:
            if col["name"] in sql_dict[model_name]:
                continue
            # column not in sql
            else:
                print(f"Popping {col['name']} from {model_name} in {yml_file}")
                remove_column(yml_dict, model_name, col["name"])
        # column in sql but not in yml
        for sql_col in sql_dict[model_name]:
            if sql_col not in model_col_names:
                print(f"Appending {sql_col} to {model_name}")
                add_column_empty_description(yml_dict, model_name, sql_col)


def update_yml_in_dir(files_and_dirs: list, model_dir: str, models_path: str = None) -> None:
    sql_files = [f for f in files_and_dirs if f.endswith(".sql")]
    yml_file = [f for f in files_and_dirs if f.endswith(".yml")]
    if "sources.yml" in yml_file:
        yml_file.remove("sources.yml")

    sql_dict = {}  # models as keys, columns as values
    if len(sql_files) > 0:
        for file in sql_files:
            file_name = file[: -len(".sql")]
            with open(models_path + model_dir + "/" + file, "r") as f:
                model_columns = find_sql_columns(models_path + model_dir + "/" + file)
                sql_dict[file_name] = model_columns

    if len(yml_file) > 0:
        with open(models_path + model_dir + "/" + yml_file[0], "r") as f:
            yml_dict = safe_load(f)
        try:
            yml_models_dict = yml_dict["models"]
        except KeyError:
            print(f"No 'models' in {yml_file[0]}")
            yml_models_dict = None

        if yml_models_dict:
            update_yml_dict(yml_dict=yml_dict, sql_dict=sql_dict, yml_file=yml_file[0])
            yml_string = make_yml_string(yml_dict)
            with open(models_path + model_dir + "/" + yml_file[0], "w") as f:
                f.write(yml_string)

    # hvis det ikke er noen yaml, men det er sql-filer
    if len(yml_file) == 0 and len(sql_files) > 0:
        print(f"No yml file in {model_dir}, but found {sql_files}. Making the yml file.")
        # first make a dummy yml dict
        yml_dict = {"version": "2", "models": [{"name": "dummy", "columns": [{"name": "aarmnd"}]}]}
        update_yml_dict(yml_dict=yml_dict, sql_dict=sql_dict, yml_file="dummy.yml")
        yml_string = make_yml_string(yml_dict)
        new_file_name = "/_" + model_dir.split("/")[-1] + "_models.yml"
        with open(models_path + model_dir + new_file_name, "w") as f:
            f.write(yml_string)


def update_yml_from_sql(*, models_path: str):
    """
    Oppdaterer .yml-filene i dbt-prosjektet med kolonner fra .sql-filene.
    De får tomme kommentarer med denne funksjonen, men det er midlertidig.
    Kommentarene blir fylt ut i neste steg, men må være i .ym-filene for å kunne fylles ut.
    """

    # looping over the dbt models dir and subdirs
    for model_dir in ["staging", "marts", "intermediate"]:
        # first look for .sql files in the directory
        files_and_dirs = os.listdir(models_path + model_dir)
        update_yml_in_dir(files_and_dirs, model_dir, models_path)
        # then look for subdirs
        subdirs = [d for d in files_and_dirs if os.path.isdir(models_path + model_dir + "/" + d)]
        for subdir in subdirs:
            files_and_dirs = os.listdir(models_path + model_dir + "/" + subdir)
            update_yml_in_dir(files_and_dirs, model_dir + "/" + subdir, models_path)
