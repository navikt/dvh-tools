#%%
import json


# %%
def connection_as_json(user, env, env_name: str = '', schema: str = ''):
    con_dict = {
        "name": f"{env_name if env_name else env['service_name']}[{schema}]".upper() if schema else env_name if env_name else
        env['service_name'].upper(),
        "type": "jdbc",
        "info": {
            "role": "",
            "SavePassword": "true",
            "OracleConnectionType": "BASIC",
            "PROXY_TYPE": "USER NAME",
            "RaptorConnectionType": "Oracle",
            "Connection-Color-For-Editors": env['color'],
            "serviceName": env['service_name'],
            "customUrl": env['url'],
            "oraDriverType": "thin",
            "NoPasswordConnection": "TRUE",
            "hostname": env['hostname'],
            "driver": "oracle.jdbc.OracleDriver",
            "port": "1521",
            "subtype": "oraJDBC",
            "OS_AUTHENTICATION": "false",
            "IS_PROXY": "false",
            "PROXY_USER_NAME": "",
            "KERBEROS_AUTHENTICATION": "false",
            "user": f"{user}[{schema}]" if schema else user}
    }

    return con_dict
#%%
schemas = [
    'DVH_AAP',
    'DVH_DAGPENGER',
    'DVH_INNTEKTER',
    'DVH_UTDANNING',
]
#%%
envs = {
    'U': {
        'service_name': 'DWHU1',
        'color': '-65281', #color in sqldeveloper ui
        'url': 'jdbc:oracle:thin:@//DM07-SCAN.adeo.no:1521/DWHU1',
        'hostname' : 'dm07-scan.adeo.no',
        'schemas': schemas
    },
    'Q': {
        'service_name': 'DWHQ0',
        'color': '-13553153', #color in sqldeveloper ui
        'url': 'jdbc:oracle:thin:@//DM07-SCAN.ADEO.NO:1521/DWHQ0',
        'hostname' : 'dm07-scan.adeo.no',
        'schemas': schemas
    },
    'PROD': {
        'service_name': 'DWH_HA',
        'color': '-65536', #color in sqldeveloper ui
        'url': 'jdbc:oracle:thin:@//dm08-scan.adeo.no:1521/DWH_HA',
        'hostname' : 'dm08-scan.adeo.no',
        'schemas': schemas
    }
}
#%%
USER_NAME = 'fyll ut'

with open('connections.json', 'w') as f:
    a = []
    for name, env in envs.items():
        a.append(connection_as_json(USER_NAME, env, env_name=name))
        for schema in env['schemas']:
            a.append(connection_as_json(USER_NAME, env, env_name=name, schema=schema))
    f.write(json.dumps({"connections": a}))