import requests
import credentials
import pandas_gbq
import pandas as pd
import io
import concurrent.futures

id = 50001
lang = 'pt'
server = 'sa'

credentials.creating_context_gcp()

#PAROU NO ID 763

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
            print(f'O ID {id} é válido')
            return new_df
    else:
        print(f'Erro ao consultar o ID {id}. Status code: {response.status_code}. Encerrando a busca.')
        return None

with concurrent.futures.ThreadPoolExecutor(max_workers=100) as executor:
    future_to_url = {executor.submit(fetch_url, id): id for id in range(50001, 55000)}
    for future in concurrent.futures.as_completed(future_to_url):
        new_df = future.result()
        if new_df is not None:
            df = pd.concat([df, new_df])

# Carregar o DataFrame no BigQuery
pandas_gbq.to_gbq(df, 'dados_black_desert_online.market_br', project_id='emersondai254', if_exists='append')
