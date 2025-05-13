## Enkel implementasjon for å flytte en tabell fra BigQuery til Oracle

```python
from google.cloud import secretmanager
from dvh_tools.bq_to_oracle import DataTransfer

def get_secret_env(resource_name) -> dict:
    secrets = secretmanager.SecretManagerServiceClient()
    secret = secrets.access_secret_version(name=resource_name)
    secret_str = secret.payload.data.decode("UTF-8")
    return json.loads(secret_str)

bq_env = get_secret_env(
        "projects/<project-id>/secrets/<service-account-json-key>/versions/latest"
    ) # gcp json-key
oracle_env = get_secret_env("projects/<project-id>/secrets/<oracle-secret>/versions/latest") # {"DB_USER":"", "DB_PASS":"", "DB_DSN":""}
env = {"gcp": bq_env, "oracle": oracle_env}
colums = ["column1", "column2"]
table = {
    "source-query": "select {} from `gcp-project.bigquery_table`".format(
        ",".join(columns)
    ),
    "target-table": "schema_name.oracle_table",
}
data_transfer = DataTransfer(
    config=env,
    source_query=table["source-query"],
    target_table=table["target-table"],
)
# Sett convert_lists til True om BigQquery har json-datatyper
data_transfer.run(
    dry_run=False, datatypes=table.get("datatypes"), convert_lists=False
)  # dry_run settes til True dersom man ikke ønsker å skrive til db
```

## env["gcp"] examples:

### service_account
```json
{
    "type": "",
    "project_id": "",
    "private_key_id": "",
    "private_key": "",
    "client_email": "",
    "client_id": "",
    "auth_uri": "",
    "token_uri": "",
    "auth_provider_x509_cert_url": "",
    "client_x509_cert_url": "",
    "universe_domain": ""
}
```
### impersonated
```json
{
    "project_id": "<prosjekt-id>",
    "target_principal": "min-sa@<prosjekt-id>.iam.gserviceaccount.com",
    "target_scopes": [
        "https://www.googleapis.com/auth/cloud-platform"
    ],
}

```