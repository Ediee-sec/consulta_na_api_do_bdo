import requests
import credentials
import pandas_gbq
import pandas as pd
import concurrent.futures
import subprocess
import time 

start_time = time.time()

lang = 'pt'
server = 'sa'

credentials.creating_context_gcp()

df = pd.DataFrame()

def fetch_url(id):
    url = f'https://api.arsha.io/v2/{server}/item?id={id}&lang={lang}'
    headers = {"Accept": "application/json"}
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        ls = response.json()
        if ls[0]['name'] is None:
            print(f"ID {id} é inválido")
        else:
            # Achatar o JSON e criar um DataFrame
            new_df = pd.json_normalize(ls)
            print(f"ID {id} Atualizado")

            return new_df
    else:
        print(f'Erro ao consultar o ID {id}. Status code: {response.status_code}. Encerrando a busca.')
        return None

# Ler os IDs do arquivo .txt
with open('lista_ids.txt', 'r') as f:
    ids = [line.strip() for line in f]

with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
    future_to_url = {executor.submit(fetch_url, id): id for id in ids}
    for future in concurrent.futures.as_completed(future_to_url):
        new_df = future.result()
        if new_df is not None:
            df = pd.concat([df, new_df])

# Carregar o DataFrame no BigQuery
pandas_gbq.to_gbq(df, 'dados_black_desert_online.market_br', project_id='emersondai254', if_exists='append')

# Executa a query que irá recriar as tabelas no big query
bash = ('bq query --use_legacy_sql=false < query.sql')
subprocess.call(bash,shell=True)

end_time = time.time()
execution_time = end_time - start_time

print(f"O script levou {execution_time} segundos para executar")
