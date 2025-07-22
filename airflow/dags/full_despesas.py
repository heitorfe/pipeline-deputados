from airflow.sdk import dag, task
from airflow.providers.amazon.aws.transfers.http_to_s3 import HttpToS3Operator
import json
import requests
from datetime import datetime, timedelta
import math

AWS_CONN_ID = "aws_s3_conn"
BUCKET_NAME = "learnsnowflakedbt-heitor"
HTTP_CONN_ID = "http_camara_conn"
ENDPOINT_DEPUTADOS = "https://dadosabertos.camara.leg.br/api/v2/deputados"
ENDPOINT_DEPUTADOS_LEGISLATURA = "https://dadosabertos.camara.leg.br/api/v2/deputados?idLegislatura={legislatura_id}"
ENDPOINT_DESPESAS = "https://dadosabertos.camara.leg.br/api/v2/deputados/{deputado_id}/despesas"

@dag(
    schedule='@daily', 
    start_date=datetime(2020,1,1), 
    catchup=False, 
    tags=['deputados'],
    params={
        'anoInicio': 2020,
        'anoFim': 2024,
        'cargaFull': False,
        'idLegislatura': None  # New parameter for legislature
    }
)
def full_despesas():

    load_full_deputados = HttpToS3Operator(
        task_id="load_full_deputados",
        endpoint="deputados",
        method="GET",
        s3_key="camara/deputados/deputados.json",
        http_conn_id=HTTP_CONN_ID,
        s3_bucket=BUCKET_NAME,
        aws_conn_id=AWS_CONN_ID,
        replace=True
    )

    @task
    def get_deputados_ids(**context):
        """Get deputados IDs from current legislature or specific legislature"""
        params = context.get('params', {})
        id_legislatura = params.get('idLegislatura', None)
        
        deputados_ids = []
        
        if id_legislatura:
            # Get deputados from specific legislature
            print(f"Loading deputados from legislature {id_legislatura}")
            response = requests.get(ENDPOINT_DEPUTADOS_LEGISLATURA.format(legislatura_id=id_legislatura))
            data = response.json()
            deputados_ids = [deputado["id"] for deputado in data["dados"]]
            print(f"Found {len(deputados_ids)} deputados in legislature {id_legislatura}")
        else:
            # Get current deputados (default behavior)
            print("Loading current deputados")
            response = requests.get(ENDPOINT_DEPUTADOS)
            data = response.json()
            deputados_ids = [deputado["id"] for deputado in data["dados"]]
            print(f"Found {len(deputados_ids)} current deputados")
        
        return {
            'deputados_ids': deputados_ids,
            'legislatura': id_legislatura
        }

    @task
    def load_deputados_by_legislature(**context):
        """Load deputados data based on legislature parameter"""
        params = context.get('params', {})
        id_legislatura = params.get('idLegislatura', None)
        
        if id_legislatura:
            # Load specific legislature deputados
            operator = HttpToS3Operator(
                task_id="load_legislatura_deputados",
                endpoint=f"deputados?idLegislatura={id_legislatura}",
                method="GET",
                s3_key=f"camara/deputados/historico/deputadosLegislatura{id_legislatura}.json",
                http_conn_id=HTTP_CONN_ID,
                s3_bucket=BUCKET_NAME,
                aws_conn_id=AWS_CONN_ID,
                replace=True
            )
            result = operator.execute(context=context)
            print(f"Loaded deputados from legislature {id_legislatura}")
            return result
        else:
            # Load current deputados (existing behavior)
            operator = HttpToS3Operator(
                task_id="load_current_deputados",
                endpoint="deputados",
                method="GET",
                s3_key="camara/deputados/deputadosAtual.json",
                http_conn_id=HTTP_CONN_ID,
                s3_bucket=BUCKET_NAME,
                aws_conn_id=AWS_CONN_ID,
                replace=True
            )
            result = operator.execute(context=context)
            print("Loaded current deputados")
            return result

    @task
    def generate_years(**context):
        """Generate years to process based on parameters"""
        params = context.get('params', {})
        carga_full = params.get('cargaFull', False)
        
        if carga_full:
            # Full load mode - use anoInicio and anoFim parameters
            ano_inicio = params.get('anoInicio', 2020)
            ano_fim = params.get('anoFim', datetime.now().year)
            
            return list(range(ano_inicio, ano_fim + 1))
        else:
            # Incremental mode - current year only
            logical_date = context['logical_date']
            return [logical_date.year]

    @task
    def process_year_despesas(year, deputados_data, **context):
        """Process despesas for all deputados for a specific year"""
        params = context.get('params', {})
        carga_full = params.get('cargaFull', False)
        id_legislatura = params.get('idLegislatura', None)
        
        deputados_list = deputados_data['deputados_ids']
        legislatura = deputados_data['legislatura']
        
        print(f"Processing year {year} with {len(deputados_list)} deputados from {'legislature ' + str(legislatura) if legislatura else 'current mandate'}")
        
        # Determine months to process
        if carga_full:
            months = list(range(1, 13))  # All months
        else:
            # Incremental mode - current month only
            logical_date = context['logical_date']
            if year == logical_date.year:
                months = [logical_date.month]
            else:
                months = []  # Skip other years in incremental mode
        
        results = []
        
        for deputado_id in deputados_list:
            for mes in months:
                try:
                    # Build S3 key with legislature info if available
                    s3_key_prefix = f"camara/despesas/ano={year}/mes={mes:02d}"
                    if legislatura:
                        s3_key = f"{s3_key_prefix}/deputado={deputado_id}/despesas.json"
                    else:
                        s3_key = f"{s3_key_prefix}/deputado={deputado_id}/despesas.json"
                    
                    # Create and execute HttpToS3Operator
                    operator = HttpToS3Operator(
                        task_id=f"load_despesas_{deputado_id}_{year}_{mes:02d}",
                        endpoint=f"deputados/{deputado_id}/despesas",
                        method="GET",
                        data={
                            'ano': year,
                            'mes': mes,
                            'itens': 999,
                            'ordem': 'DESC',
                            'ordenarPor': 'ano'
                        },
                        s3_key=s3_key,
                        http_conn_id=HTTP_CONN_ID,
                        s3_bucket=BUCKET_NAME,
                        aws_conn_id=AWS_CONN_ID,
                        replace=True
                    )
                    
                    result = operator.execute(context=context)
                    results.append({
                        'deputado_id': deputado_id,
                        'ano': year,
                        'mes': mes,
                        'legislatura': legislatura,
                        'status': 'success',
                        'result': result
                    })
                    
                except Exception as e:
                    print(f"Error processing deputado {deputado_id} for {year}-{mes:02d}: {str(e)}")
                    results.append({
                        'deputado_id': deputado_id,
                        'ano': year,
                        'mes': mes,
                        'legislatura': legislatura,
                        'status': 'error',
                        'error': str(e)
                    })
        
        success_count = len([r for r in results if r['status'] == 'success'])
        error_count = len([r for r in results if r['status'] == 'error'])
        print(f"Year {year} completed: {success_count} successes, {error_count} errors")
        
        return {
            'year': year,
            'legislatura': legislatura,
            'total_processed': len(results),
            'successes': success_count,
            'errors': error_count,
            'results': results
        }

    # Task flow
    load_deputados_task = load_deputados_by_legislature()
    deputados_data = get_deputados_ids()
    years = generate_years()
    
    # Process each year using expand - deputados_data will be broadcast to all years
    year_results = process_year_despesas.partial(
        deputados_data=deputados_data
    ).expand(
        year=years
    )

    # Dependencies
    load_deputados_task >> deputados_data
    deputados_data >> [years, year_results]

full_despesas()