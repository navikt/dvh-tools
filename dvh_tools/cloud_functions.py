import json

from google.cloud import secretmanager

def get_gsm_secret(project_id, secret_name):
    '''Returnerer secret-verdien
    '''
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_name}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    secret = json.loads(response.payload.data.decode('UTF-8'))
    return secret
