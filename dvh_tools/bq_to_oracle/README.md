## Enkel implementasjon for å flytte en tabell fra BigQuery til Oracle

```python
from dvh_tools.bq_to_oracle import DataTransfer
env = {
    "gcp": "projects/<project-id>/secrets/dvh_aareg_gcp_serviceaccount/versions/latest",
    "oracle": "projects/<project-id>/secrets/dvh_aareg_py/versions/latest",
}
colums = ["column1", "column2"]
table = {
    "source-query": "select {} from `gcp-project.bigquery_table`".format(
        ",".join(columns)
    ),
    "target-table": "schema_name.oracle_table",
}
data_transfer = DataTransfer(
    env,
    source_query=table["source-query"],
    target_table=table["target-table"],
)
# Sett convert_lists til True om BigQquery har json-datatyper
data_transfer.run(
    dry_run=False, datatypes=table.get("datatypes"), convert_lists=False
)  # dry_run settes til True dersom man ikke ønsker å skrive til db
```