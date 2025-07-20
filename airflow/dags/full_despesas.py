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
ENDPOINT_DESPESAS = "https://dadosabertos.camara.leg.br/api/v2/deputados/{deputado_id}/despesas"

@dag(
    schedule='@daily', 
    start_date=datetime(2020,1,1), 
    catchup=False, 
    tags=['deputados'],
    params={
        'anoInicio': 2020,
        'anoFim': 2024,
        'cargaFull': False
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
    def get_deputados_ids():
        # Get data from the API
        response = requests.get(ENDPOINT_DEPUTADOS)
        data = response.json()
        
        # Extract IDs from the response
        deputados_ids = [deputado["id"] for deputado in data["dados"]]
        
        return deputados_ids

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
    def process_year_despesas(year, deputados_list, **context):
        """Process despesas for all deputados for a specific year"""
        params = context.get('params', {})
        carga_full = params.get('cargaFull', False)
        
        print(f"Processing year {year} with {len(deputados_list)} deputados")
        
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
                        s3_key=f"camara/despesas/ano={year}/mes={mes:02d}/deputado={deputado_id}/despesas.json",
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
                        'status': 'success',
                        'result': result
                    })
                    
                except Exception as e:
                    print(f"Error processing deputado {deputado_id} for {year}-{mes:02d}: {str(e)}")
                    results.append({
                        'deputado_id': deputado_id,
                        'ano': year,
                        'mes': mes,
                        'status': 'error',
                        'error': str(e)
                    })
        
        success_count = len([r for r in results if r['status'] == 'success'])
        error_count = len([r for r in results if r['status'] == 'error'])
        print(f"Year {year} completed: {success_count} successes, {error_count} errors")
        
        return {
            'year': year,
            'total_processed': len(results),
            'successes': success_count,
            'errors': error_count,
            'results': results
        }

    # Task flow
    deputados_ids = get_deputados_ids()
    years = generate_years()
    
    # Process each year using expand - deputados_list will be broadcast to all years
    year_results = process_year_despesas.partial(
        deputados_list=deputados_ids
    ).expand(
        year=years
    )

    # Dependencies
    load_full_deputados >> deputados_ids
    deputados_ids >> [years, year_results]

full_despesas()