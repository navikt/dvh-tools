import json

from google.cloud import secretmanager


class SpennSecrets:
    def __init__(self, secret_id, environment):
        '''
        Example: how to use this Class:
        from SpennSecrets import SpennSecrets

        creds = SpennSecrets(secret_id='dvh_inntekter',environment='P')
        '''
        client = secretmanager.SecretManagerServiceClient()
        project_id = 'spenn-prod-23e0' if environment == 'P' else ('spenn-dev-5a1e' if environment == 'U' else None)
        name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
        response = client.access_secret_version(request={"name": name})

        secrets = json.loads(response.payload.data.decode('UTF-8'))
        if 'GCP' in secrets:
            self.GCP_secrets = dict(secrets['GCP'][environment].items())
        if 'ORACLE' in secrets:
            self.ORACLE_secrets = dict(secrets['ORACLE'][environment].items())
        if 'DBT' in secrets:
            self.DBT_secrets = dict(secrets['DBT'][environment].items())
        if 'SFTP' in secrets:
            self.SFTP_secrets = dict(secrets['SFTP'].items())
