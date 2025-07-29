from airflow.sdk import dag, task
from airflow.providers.amazon.aws.transfers.http_to_s3 import HttpToS3Operator
from datetime import datetime, timedelta
import pandas as pd
import requests
import boto3
from io import BytesIO
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

AWS_CONN_ID = "aws_s3_conn"
BUCKET_NAME = "learnsnowflakedbt-heitor"
HTTP_CONN_ID = "http_camara_conn"
ENDPOINT_DEP_BASE = "https://dadosabertos.camara.leg.br/api/v2/deputados"
ENDPOINT_DESPESAS = "https://dadosabertos.camara.leg.br/api/v2/deputados/{deputado_id}/despesas"

@dag(
    schedule='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['deputados', 'incremental'],
)
def incremental_despesas():

    @task
    def get_data_referencia():
        """Define mês atual e anterior para busca incremental."""
        today = pd.Timestamp.today()
        one_month_ago = today.replace(day=1) - pd.DateOffset(months=1)

        return {
            'ano_atual': today.year,
            'mes_atual': today.month,
            'ano_anterior': one_month_ago.year,
            'mes_anterior': one_month_ago.month,
            'dia_atual': today.day
        }

    @task
    def get_deputados_ids(data_referencia):
        """Obtém IDs dos deputados com mandato ativo."""
        data_inicio = f"{data_referencia['ano_atual']}-{data_referencia['mes_atual']:02d}-01"
        data_fim = f"{data_referencia['ano_atual']}-{data_referencia['mes_atual']:02d}-{data_referencia['dia_atual']:02d}"
        url = f"{ENDPOINT_DEP_BASE}?itens=10000&dataInicio={data_inicio}&dataFim={data_fim}&ordem=ASC&ordenarPor=nome"

        response = requests.get(url)
        if response.status_code != 200:
            raise Exception(f"Erro ao buscar deputados: {url}")

        deputados = response.json()["dados"]
        ids = [dep["id"] for dep in deputados]
        print(f"Encontrados {len(ids)} deputados em mandato ativo.")
        return ids

    @task
    def processa_despesas(dep_ids, data_referencia):
        """Executa as chamadas para obter despesas dos deputados para os meses atual e anterior."""
        
        
        s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
        
        for ano, mes in [
            (data_referencia['ano_atual'], data_referencia['mes_atual']),
            (data_referencia['ano_anterior'], data_referencia['mes_anterior']),
        ]:
            all_despesas = []
            
            for deputado_id in dep_ids:
                try:
                    url = ENDPOINT_DESPESAS.format(deputado_id=deputado_id)
                    params = {
                        'ano': ano,
                        'mes': mes,
                        'itens': 10000,
                        'ordem': 'ASC',
                        'ordenarPor': 'ano'
                    }
                    
                    response = requests.get(url, params=params)
                    if response.status_code == 200:
                        data = response.json()
                        if 'dados' in data and data['dados']:
                            for despesa in data['dados']:
                                despesa['deputado_id'] = deputado_id
                            all_despesas.extend(data['dados'])
                    else:
                        print(f"Erro para deputado {deputado_id} em {mes}/{ano}: Status {response.status_code}")
                        
                except Exception as e:
                    print(f"Erro para deputado {deputado_id} em {mes}/{ano}: {str(e)}")
            
            if all_despesas:
                # Consolida em DataFrame e salva como Parquet
                df = pd.DataFrame(all_despesas).drop_duplicates()
                
                # Salva em buffer de memória
                buffer = BytesIO()
                df.to_parquet(buffer, index=False)
                buffer.seek(0)
                
                # Upload para S3 usando S3Hook
                s3_key = f"camara/despesas/incremental/despesas-{ano}-{mes:02d}.parquet"
                s3_hook.load_bytes(
                    bytes_data=buffer.getvalue(),
                    key=s3_key,
                    bucket_name=BUCKET_NAME,
                    replace=True
                )
                
                print(f"Salvo {len(all_despesas)} despesas em {s3_key}")
            else:
                print(f"Nenhuma despesa encontrada para {mes}/{ano}")


    # Encadeamento da DAG
    data_referencia = get_data_referencia()
    deputados_ids = get_deputados_ids(data_referencia)
    processa_despesas(dep_ids=deputados_ids, data_referencia=data_referencia)

incremental_despesas()
