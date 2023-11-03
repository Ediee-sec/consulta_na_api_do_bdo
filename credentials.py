from google.oauth2 import service_account
import pandas_gbq
import json

#var constante
key_path = "/home/emerson/.doc/.service_account_gcp.json"

#Cria a conexão com o bigquery, usando a service account json
def credentials():
    credentials = service_account.Credentials.from_service_account_file(key_path)
    
    return credentials

#Vai até o arquivo do service account e recupera o nome do projeto, para não deixar hard code
def get_project():
    with open(key_path, 'r') as file:
        project_id = json.load(file)

    project_id = project_id['project_id']

    return project_id

#Cria a conexão, usando a biblioteca pandas_gbq
def creating_context_gcp():
    pandas_gbq.context.credentials = credentials()
    pandas_gbq.context.project = get_project