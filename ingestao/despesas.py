import os
import logging
import requests
import pandas as pd
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor

LEGISLATURAS = range(57, 50, -1)

def fetch_all_pages(url):
    all_data = []
    page = 1
    while True:
        paginated_url = f"{url}&pagina={page}"
        response = requests.get(paginated_url)
        if response.status_code == 200:
            data = response.json()
            if 'dados' in data and data['dados']:
                all_data.extend(data['dados'])
                page += 1
            else:
                break
        else:
            logging.error(f"Erro na requisição: {response.status_code}")
            break
    return all_data

def process_legislatura(legislatura):
    logging.info(f"Iniciando processamento para a legislatura {legislatura}...")
    despesas_legislatura = []
    despesas_count_por_ano = {}

    url_deputados = f"https://dadosabertos.camara.leg.br/api/v2/deputados?idLegislatura={legislatura}&itens=10000&ordem=ASC&ordenarPor=nome"
    deputados_data = fetch_all_pages(url_deputados)
    deputados = pd.json_normalize(deputados_data)
    dep_ids = deputados['id'].tolist()

    logging.info(f"Encontrados {len(dep_ids)} deputados na legislatura {legislatura}")

    for dep_id in tqdm(dep_ids, desc=f"Baixando despesas para a legislatura {legislatura}", unit="deputado"):
        legislatura_url = f"https://dadosabertos.camara.leg.br/api/v2/legislaturas/{legislatura}"
        legislatura_response = requests.get(legislatura_url)
        if legislatura_response.status_code == 200:
            legislatura_data = legislatura_response.json()
            data_inicio = legislatura_data['dados']['dataInicio']
            data_fim = legislatura_data['dados']['dataFim']
            ano_inicio = int(data_inicio.split('-')[0])
            ano_fim = int(data_fim.split('-')[0])
            anos_legislatura = range(ano_inicio, ano_fim + 1)
            for ano in anos_legislatura:
                for mes in range(1, 13):
                    url_despesa = f"https://dadosabertos.camara.leg.br/api/v2/deputados/{dep_id}/despesas?ano={ano}&mes={mes}&idLegislatura={legislatura}&itens=1000"
                    despesas = fetch_all_pages(url_despesa)
                    if despesas:
                        for despesa in despesas:
                            despesa['deputado_id'] = dep_id
                            despesa['legislatura_id'] = legislatura
                        despesas_legislatura.extend(despesas)
                        if ano not in despesas_count_por_ano:
                            despesas_count_por_ano[ano] = 0
                        despesas_count_por_ano[ano] += len(despesas)

    if despesas_legislatura:
        df_despesas = pd.DataFrame(despesas_legislatura)
        logging.info(f"Total de despesas baixadas para a legislatura {legislatura}: {len(df_despesas)}")
        parquet_path = f"../data/despesas/parquet/despesas-{legislatura}.parquet"
        os.makedirs(os.path.dirname(parquet_path), exist_ok=True)
        df_despesas.to_parquet(parquet_path, index=False)
        for ano, count in despesas_count_por_ano.items():
            if count > 0:
                logging.info(f"Legislatura {legislatura} - Ano {ano}: {count} despesas")
    else:
        logging.warning(f"Nenhuma despesa encontrada para a legislatura {legislatura}")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    with ProcessPoolExecutor() as executor:
        executor.map(process_legislatura, LEGISLATURAS)
