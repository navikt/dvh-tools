from pathlib import Path
import runpy

import pytest
from yaml import safe_dump, safe_load


@pytest.fixture(scope="module")
def comments_module():
    script_path = Path(__file__).parent / "dbt_tools" / "generate_comments_from_sql.py"
    return runpy.run_path(str(script_path), run_name="__main__")


@pytest.mark.parametrize("description", [None, "", "  "])
def test_missing_descriptions_report_file_model_and_column(
    comments_module, caplog, description
):
    data = {
        "models": [
            {
                "name": "customers",
                "description": description,
                "columns": [{"name": "customer_id", "description": description}],
            }
        ]
    }
    with pytest.raises(ValueError, match="Missing model documentation"):
        comments_module["make_yml_string"](data, "models/customers.yml")
    assert (
        "models/customers.yml: model 'customers' is missing a non-empty description"
        in caplog.text
    )
    assert (
        "model 'customers', column 'customer_id' is missing a non-empty description"
        in caplog.text
    )


def test_absent_description_keys_are_reported(comments_module, caplog):
    data = {"models": [{"name": "customers", "columns": [{"name": "customer_id"}]}]}
    with pytest.raises(ValueError, match="Missing model documentation"):
        comments_module["make_yml_string"](data, "models/customers.yml")
    assert len(caplog.records) == 2


def test_complete_descriptions_are_preserved(comments_module):
    data = {
        "version": 2,
        "models": [
            {
                "name": "customers",
                "description": "Customer model",
                "columns": [
                    {
                        "name": "customer_id",
                        "description": "Customer identifier",
                        "data_tests": ["not_null"],
                    }
                ],
            }
        ],
    }
    assert safe_load(comments_module["make_yml_string"](data)) == data


def test_directory_validation_does_not_overwrite_yaml(
    comments_module, tmp_path, caplog
):
    sql_path = tmp_path / "customers.sql"
    sql_path.write_text(
        "select\n    customer_id\nfrom source_customers\n", encoding="utf-8"
    )
    yaml_path = tmp_path / "_models.yml"
    original = "version: 2\nmodels:\n  - name: customers\n    columns:\n      - name: customer_id\n"
    yaml_path.write_text(original, encoding="utf-8")
    with pytest.raises(ValueError, match="Missing model documentation"):
        comments_module["update_yamls_from_sqls_in_dir"](
            [sql_path.name, yaml_path.name], tmp_path
        )
    assert str(yaml_path) in caplog.text
    assert yaml_path.read_text(encoding="utf-8") == original


@pytest.mark.parametrize(
    "data, message",
    [
        ({}, "missing or empty 'models' list"),
        ({"models": [{"description": "Model", "columns": []}]}, "missing 'name'"),
        (
            {"models": [{"name": "customers", "description": "Model"}]},
            "missing a 'columns' list",
        ),
    ],
)
def test_missing_model_structure_is_reported(comments_module, data, message):
    assert message in "\n".join(
        comments_module["missing_model_comments"](data, "models/customers.yml")
    )


@pytest.fixture
def model_project(comments_module, tmp_path, monkeypatch):
    (tmp_path / ".git").mkdir()
    (tmp_path / "models").mkdir()
    docs_path = tmp_path / "docs"
    docs_path.mkdir()
    (docs_path / "comments_custom.yml").write_text(
        "custom_column_comments: {}\ncustom_table_descriptions: {}\n", encoding="utf-8"
    )
    (docs_path / "comments_source.yml").write_text(
        "source_column_comments: {}\nsource_table_descriptions: {}\n", encoding="utf-8"
    )
    generate = comments_module["generate_comments_from_sql"]
    monkeypatch.setitem(
        generate.__globals__, "__file__", str(tmp_path / "package" / "module.py")
    )
    return tmp_path, generate


@pytest.fixture
def write_model(model_project):
    project_root, _ = model_project

    def write(
        name, model_description=None, column_description=None, column_name="customer_id"
    ):
        folder = project_root / "models" / name
        folder.mkdir()
        (folder / f"{name}.sql").write_text(
            f"select\n    {column_name}\nfrom source_customers\n", encoding="utf-8"
        )
        column = {"name": column_name}
        if column_description is not None:
            column["description"] = column_description
        model = {"name": name, "columns": [column]}
        if model_description is not None:
            model["description"] = model_description
        yaml_path = folder / "_models.yml"
        yaml_path.write_text(
            safe_dump({"version": 2, "models": [model]}, sort_keys=False),
            encoding="utf-8",
        )
        return yaml_path

    return write


@pytest.mark.parametrize("comment_type", ["custom", "source"])
def test_shared_comments_fill_missing_fields_before_validation(
    model_project, write_model, comment_type
):
    project_root, generate = model_project
    yaml_path = write_model("customers", model_description=" ", column_description=" ")
    comments = {
        f"{comment_type}_column_comments": {"customer_id": "Customer identifier"},
        f"{comment_type}_table_descriptions": {"customers": "Customer model"},
    }
    (project_root / "docs" / f"comments_{comment_type}.yml").write_text(
        safe_dump(comments), encoding="utf-8"
    )

    generate(models_path="models", docs_path="docs")

    model = safe_load(yaml_path.read_text(encoding="utf-8"))["models"][0]
    assert model["description"] == "Customer model"
    assert model["columns"][0]["description"] == "Customer identifier"


def test_all_missing_comments_are_reported_without_any_model_writes(
    model_project, write_model, caplog
):
    _, generate = model_project
    paths = [
        write_model("customers"),
        write_model("orders"),
        write_model("valid", "Valid model", "Identifier", column_name="valid_id"),
    ]
    originals = {path: path.read_bytes() for path in paths}

    with pytest.raises(SystemExit) as error:
        generate(models_path="models", docs_path="docs")

    assert error.value.code == 1
    for path in paths[:2]:
        assert (
            f"{path}: model '{path.parent.name}' is missing a non-empty description"
            in caplog.text
        )
        assert (
            f"{path}: model '{path.parent.name}', column 'customer_id' is missing a non-empty description"
            in caplog.text
        )
    assert "No model YAML files were written" in caplog.text
    assert "custom_table_descriptions / custom_column_comments" in caplog.text
    assert {path: path.read_bytes() for path in paths} == originals


@pytest.mark.parametrize("provide_comments", [False, True])
def test_new_model_is_only_created_with_complete_comments(
    model_project, caplog, provide_comments
):
    project_root, generate = model_project
    folder = project_root / "models" / "customers"
    folder.mkdir()
    (folder / "customers.sql").write_text(
        "select\n    customer_id\nfrom source_customers\n", encoding="utf-8"
    )
    yaml_path = folder / "_customers_models.yml"
    if provide_comments:
        (project_root / "docs" / "comments_custom.yml").write_text(
            safe_dump(
                {
                    "custom_column_comments": {"customer_id": "Identifier"},
                    "custom_table_descriptions": {"customers": "Customers"},
                }
            ),
            encoding="utf-8",
        )
        generate(models_path="models", docs_path="docs")
        model = safe_load(yaml_path.read_text(encoding="utf-8"))["models"][0]
        assert model["description"] == "Customers"
        assert model["columns"][0]["description"] == "Identifier"
    else:
        with pytest.raises(SystemExit):
            generate(models_path="models", docs_path="docs")
        assert str(yaml_path) in caplog.text
        assert "model 'customers'" in caplog.text
        assert "column 'customer_id'" in caplog.text
        assert not yaml_path.exists()


def test_missing_structure_is_reported_across_files(model_project, write_model, caplog):
    _, generate = model_project
    missing_name = write_model("customers")
    missing_columns = write_model("orders")
    missing_name.write_text(
        "models:\n  - description: Customers\n    columns: []\n", encoding="utf-8"
    )
    missing_columns.write_text(
        "models:\n  - name: orders\n    description: Orders\n", encoding="utf-8"
    )
    originals = {path: path.read_bytes() for path in [missing_name, missing_columns]}

    with pytest.raises(SystemExit):
        generate(models_path="models", docs_path="docs")

    assert f"{missing_name}: model entry #1 is missing 'name'" in caplog.text
    assert (
        f"{missing_columns}: model 'orders' is missing a 'columns' list" in caplog.text
    )
    assert {path: path.read_bytes() for path in originals} == originals


def test_existing_yaml_column_comments_are_reused(model_project, write_model):
    _, generate = model_project
    write_model("customers", "Customers", "Shared identifier")
    yaml_path = write_model("orders", "Orders")

    generate(models_path="models", docs_path="docs")

    model = safe_load(yaml_path.read_text(encoding="utf-8"))["models"][0]
    assert model["columns"][0]["description"] == "Shared identifier"


@pytest.mark.parametrize("use_custom", [False, True])
def test_comment_precedence_is_preserved(model_project, write_model, use_custom):
    project_root, generate = model_project
    yaml_path = write_model("customers", "YAML model", "YAML column")
    (project_root / "docs" / "comments_source.yml").write_text(
        safe_dump(
            {
                "source_column_comments": {"customer_id": "Source column"},
                "source_table_descriptions": {"customers": "Source model"},
            }
        ),
        encoding="utf-8",
    )
    if use_custom:
        (project_root / "docs" / "comments_custom.yml").write_text(
            safe_dump(
                {
                    "custom_column_comments": {"customer_id": "Custom column"},
                    "custom_table_descriptions": {"customers": "Custom model"},
                }
            ),
            encoding="utf-8",
        )

    generate(models_path="models", docs_path="docs")

    model = safe_load(yaml_path.read_text(encoding="utf-8"))["models"][0]
    prefix = "Custom" if use_custom else "YAML"
    assert model["description"] == f"{prefix} model"
    assert model["columns"][0]["description"] == f"{prefix} column"


@pytest.mark.parametrize("test_key", ["tests", "data_tests"])
def test_nested_tests_and_metadata_round_trip(comments_module, test_key):
    data = {
        "version": 2,
        "models": [
            {
                "name": "customers",
                "description": "Customer's model: \u00c9n rad per behandling.\nSecond line.",
                "config": {"tags": ["daily"], "meta": {"owner": "team"}},
                test_key: [
                    {
                        "dbt_utils.unique_combination_of_columns": {
                            "arguments": {
                                "combination_of_columns": ["customer_id", "status"]
                            },
                            "config": {"severity": "warn"},
                        }
                    }
                ],
                "columns": [
                    {
                        "name": "status",
                        "description": "Status: F\u00f8rstegangsbehandling or Revurdering",
                        "meta": {"classification": "internal"},
                        test_key: [
                            "not_null",
                            {
                                "accepted_values": {
                                    "arguments": {
                                        "values": [
                                            "F\u00f8rstegangsbehandling",
                                            "Revurdering",
                                        ]
                                    }
                                }
                            },
                            {
                                "not_null": {
                                    "config": {
                                        "where": "behandlingsresultat != 'AVSLAG' or behandlingsresultat is null"
                                    }
                                }
                            },
                            {
                                "relationships": {
                                    "arguments": {
                                        "to": "ref('statuses')",
                                        "field": "status",
                                    }
                                }
                            },
                        ],
                    }
                ],
            }
        ],
        "sources": [{"name": "raw", "tables": [{"name": "customers"}]}],
    }

    result = comments_module["make_yml_string"](data)

    assert safe_load(result) == data
    assert "\u00c9n rad" in result


@pytest.fixture
def snapshot_document():
    return {
        "version": 2,
        "snapshots": [
            {
                "name": "snapshot_aap_behandlinger_kelvin",
                "description": "Snapshot av Kelvin-vedtak. Felles kilde for utbetalings- og vedtakspipeline.\n",
                "config": {
                    "strategy": "timestamp",
                    "unique_key": "behandlingsreferanse",
                    "updated_at": "rad_endret_tid",
                },
                "columns": [
                    {
                        "name": "behandlingsreferanse",
                        "description": "Reference identifier",
                        "tests": ["unique", "not_null"],
                    },
                    {
                        "name": "behandlingstype",
                        "description": "Type behandling",
                        "tests": [
                            "not_null",
                            {
                                "accepted_values": {
                                    "arguments": {
                                        "values": [
                                            "F\u00f8rstegangsbehandling",
                                            "Revurdering",
                                        ]
                                    }
                                }
                            },
                        ],
                    },
                    {
                        "name": "rettighetstype",
                        "description": "Rettighetstype-perioder som JSON-array",
                        "tests": [
                            {
                                "not_null": {
                                    "config": {
                                        "where": "behandlingsresultat != 'AVSLAG' or behandlingsresultat is null"
                                    }
                                }
                            },
                        ],
                    },
                    {
                        "name": "dbt_scd_id",
                        "description": "Unique SCD2 row identifier",
                        "tests": ["unique", "not_null"],
                    },
                    {
                        "name": "dbt_valid_from",
                        "description": "SCD2 valid from timestamp",
                        "tests": ["not_null"],
                    },
                    {
                        "name": "dbt_valid_to",
                        "description": "SCD2 valid to timestamp; NULL means current",
                    },
                ],
            },
            {
                "name": "snapshot_raa_behandlinger",
                "description": "Raw snapshot",
                "columns": [
                    {
                        "name": "behandlingsreferanse",
                        "description": "Reference identifier",
                        "tests": ["unique", "not_null"],
                    }
                ],
            },
        ],
    }


def test_snapshot_document_round_trips(comments_module, snapshot_document):
    result = comments_module["make_yml_string"](snapshot_document, "snapshots.yml")
    assert safe_load(result) == snapshot_document


@pytest.mark.parametrize("mixed_document", [False, True])
def test_snapshots_and_tests_survive_full_update(
    model_project, snapshot_document, mixed_document
):
    project_root, generate = model_project
    folder = project_root / "models" / "kelvin"
    folder.mkdir()
    snapshot_name = snapshot_document["snapshots"][0]["name"]
    (folder / f"{snapshot_name}.sql").write_text(
        "{% snapshot example %}\nselect * from source_table\n{% endsnapshot %}\n",
        encoding="utf-8",
    )
    if mixed_document:
        snapshot_document["models"] = [
            {
                "name": "customers",
                "description": "Customers",
                "columns": [
                    {
                        "name": "customer_id",
                        "description": "Customer ID",
                        "data_tests": ["not_null"],
                    }
                ],
            }
        ]
        (folder / "customers.sql").write_text(
            "select\n    customer_id\nfrom source_customers\n", encoding="utf-8"
        )
    yaml_path = folder / "_kelvin.yml"
    yaml_path.write_text(
        safe_dump(snapshot_document, sort_keys=False), encoding="utf-8"
    )

    generate(models_path="models", docs_path="docs")

    assert safe_load(yaml_path.read_text(encoding="utf-8")) == snapshot_document


def test_snapshot_errors_include_resource_type_and_preserve_file(
    model_project, snapshot_document, caplog
):
    project_root, generate = model_project
    snapshot = snapshot_document["snapshots"][0]
    del snapshot["description"]
    del snapshot["columns"][1]["description"]
    yaml_path = project_root / "models" / "snapshots.yml"
    yaml_path.write_text(safe_dump(snapshot_document), encoding="utf-8")
    original = yaml_path.read_bytes()

    with pytest.raises(SystemExit):
        generate(models_path="models", docs_path="docs")

    location = f"{yaml_path}: snapshot 'snapshot_aap_behandlinger_kelvin'"
    assert f"{location} is missing a non-empty description" in caplog.text
    assert (
        f"{location}, column 'behandlingstype' is missing a non-empty description"
        in caplog.text
    )
    assert yaml_path.read_bytes() == original
