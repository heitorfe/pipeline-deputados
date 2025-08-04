
import os
import logging
import requests
import pandas as pd
import json
from tqdm import tqdm
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

# Configuração
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def fazer_requisicao(url, max_retries=3, delay=1):
    """Faz requisição com retry automático"""
    for tentativa in range(max_retries):
        try:
            response = requests.get(url, timeout=30)
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 429:
                time.sleep(delay * (2 ** tentativa))
                continue
            else:
                logger.warning(f"Status {response.status_code} para URL: {url}")
                return None
        except requests.exceptions.RequestException as e:
            logger.error(f"Erro na requisição (tentativa {tentativa + 1}): {e}")
            if tentativa < max_retries - 1:
                time.sleep(delay)
    return None

def fetch_all_pages(base_url, max_pages=None):
    """Busca todas as páginas de uma API paginada"""
    all_data = []
    page = 1

    while True:
        if max_pages and page > max_pages:
            break

        url = f"{base_url}&pagina={page}&itens=100"
        data = fazer_requisicao(url)

        if not data or 'dados' not in data or not data['dados']:
            break

        all_data.extend(data['dados'])
        page += 1
        time.sleep(0.1)

    return all_data

def obter_todas_legislaturas():
    """Obtém todas as legislaturas disponíveis na API"""
    url = "https://dadosabertos.camara.leg.br/api/v2/legislaturas?itens=100&ordem=DESC&ordenarPor=id"
    data = fazer_requisicao(url)

    if not data or 'dados' not in data:
        logger.error("Não foi possível obter as legislaturas")
        return []

    legislaturas = []
    for leg in data['dados']:
        ano_inicio = int(leg['dataInicio'].split('-')[0])
        if ano_inicio >= 2000:
            legislaturas.append({
                'id': leg['id'],
                'dataInicio': leg['dataInicio'],
                'dataFim': leg['dataFim'],
                'anoInicio': ano_inicio,
                'anoFim': int(leg['dataFim'].split('-')[0])
            })

    legislaturas.sort(key=lambda x: x['anoInicio'], reverse=True)
    return legislaturas

def obter_deputados_legislatura(legislatura_id):
    """Obtém todos os deputados de uma legislatura"""
    url = f"https://dadosabertos.camara.leg.br/api/v2/deputados?idLegislatura={legislatura_id}&ordem=ASC&ordenarPor=nome"
    deputados_data = fetch_all_pages(url)

    if not deputados_data:
        logger.warning(f"Nenhum deputado encontrado para legislatura {legislatura_id}")
        return []

    return [dep['id'] for dep in deputados_data]

def extrair_despesas_deputado(dep_id, legislatura_info):
    """Extrai todas as despesas de um deputado para uma legislatura"""
    todas_despesas = []
    anos = range(legislatura_info['anoInicio'], min(legislatura_info['anoFim'], datetime.now().year) + 1)

    for ano in anos:
        for mes in range(1, 13):
            if ano == datetime.now().year and mes > datetime.now().month:
                break

            url = f"https://dadosabertos.camara.leg.br/api/v2/deputados/{dep_id}/despesas?ano={ano}&mes={mes}&itens=100"
            despesas = fetch_all_pages(url)

            if despesas:
                for despesa in despesas:
                    despesa['deputado_id'] = dep_id
                    despesa['legislatura_id'] = legislatura_info['id']
                    despesa['ano_legislatura'] = f"{legislatura_info['anoInicio']}-{legislatura_info['anoFim']}"

                todas_despesas.extend(despesas)

            time.sleep(0.05)

    return todas_despesas

def extrair_despesas_deputado_thread(args):
    """Versão da função para usar com ThreadPoolExecutor"""
    dep_id, legislatura_info = args
    return extrair_despesas_deputado(dep_id, legislatura_info)

def extrair_despesas_legislatura_multithread(legislatura_info, max_workers=5):
    """Extrai despesas usando multithreading"""
    deputados = obter_deputados_legislatura(legislatura_info['id'])

    if not deputados:
        logger.warning(f"Nenhum deputado encontrado para legislatura {legislatura_info['id']}")
        return []

    logger.info(f"Extraindo despesas de {len(deputados)} deputados da legislatura {legislatura_info['id']} usando {max_workers} threads")

    todas_despesas = []
    args_list = [(dep_id, legislatura_info) for dep_id in deputados]

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_deputado = {executor.submit(extrair_despesas_deputado_thread, args): args[0] for args in args_list}

        for future in tqdm(as_completed(future_to_deputado), total=len(deputados), desc=f"Legislatura {legislatura_info['id']}"):
            deputado_id = future_to_deputado[future]
            try:
                despesas = future.result()
                todas_despesas.extend(despesas)
            except Exception as exc:
                logger.error(f"Erro ao processar deputado {deputado_id}: {exc}")

    return todas_despesas

def pipeline_completo(max_workers=5):
    """Pipeline completo para extrair todas as despesas desde 2000"""
    legislaturas = obter_todas_legislaturas()

    if not legislaturas:
        logger.error("Não foi possível obter as legislaturas")
        return

    logger.info(f"Iniciando extração para {len(legislaturas)} legislaturas")

    os.makedirs("../data/despesas/parquet", exist_ok=True)

    for leg in legislaturas:
        logger.info(f"Processando legislatura {leg['id']} ({leg['anoInicio']}-{leg['anoFim']})")

        arquivo_parquet = f"../data/despesas/parquet/despesas-{leg['id']}.parquet"
        if os.path.exists(arquivo_parquet):
            logger.info(f"Legislatura {leg['id']} já processada, pulando...")
            continue

        start_time = time.time()
        despesas = extrair_despesas_legislatura_multithread(leg, max_workers=max_workers)
        end_time = time.time()

        if despesas:
            df_despesas = pd.DataFrame(despesas)
            logger.info(f"Legislatura {leg['id']}: {len(df_despesas)} despesas coletadas em {end_time - start_time:.1f}s")
            df_despesas.to_parquet(arquivo_parquet, index=False)
            logger.info(f"Dados salvos em {arquivo_parquet}")
        else:
            logger.warning(f"Nenhuma despesa encontrada para legislatura {leg['id']}")

    logger.info("Pipeline completo finalizado!")

if __name__ == "__main__":
    pipeline_completo(max_workers=5)
