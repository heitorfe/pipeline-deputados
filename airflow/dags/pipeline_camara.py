from airflow.sdk import dag, task
from airflow.providers.amazon.aws.transfers.http_to_s3 import HttpToS3Operator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import json
import requests
import os
from airflow.providers.amazon.aws.transfers.http_to_s3 import HttpToS3Operator
from datetime import datetime, timedelta

AWS_CONN_ID = "aws_s3_conn"
HTTP_CONN_ID = "http_camara_conn"
SNOWFLAKE_CONN_ID = "snowflake_default"
BUCKET_NAME = "learnsnowflakedbt-heitor"
ENDPOINT_DEPUTADOS = "https://dadosabertos.camara.leg.br/api/v2/deputados"
ENDPOINT_DESPESAS = "https://dadosabertos.camara.leg.br/api/v2/deputados/{deputado_id}/despesas"

@dag(schedule='@daily', start_date=datetime(2020,1,1), catchup=False, tags=['deputados'])
def pipeline_camara():

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
		
		return deputados_ids[0:10]

	@task
	def load_deputado_despesas(deputado_id, **context):
		
		# Get logical_date from Airflow context
		logical_date = context['logical_date']
		
		# Calculate current year and month for incremental logic
		ano_atual = logical_date.year
		mes_atual = logical_date.month
		
		operator = HttpToS3Operator(
			task_id=f"load_despesas_{deputado_id}",
			endpoint=f"deputados/{deputado_id}/despesas",
			method="GET",
			data={
				'ano': ano_atual,
				'mes': mes_atual,
				'itens': 999,  # Maximum items per page
				'ordem': 'DESC',
				'ordenarPor': 'ano'
			},
			s3_key=f"camara/despesas/deputado_{deputado_id}/despesas_{ano_atual}_{mes_atual:02d}.json",
			http_conn_id=HTTP_CONN_ID,
			s3_bucket=BUCKET_NAME,
			aws_conn_id=AWS_CONN_ID,
			replace=True  # Replace monthly files for data consistency
		)
		
		return operator.execute(context=context)


	# Dynamic Task Mapping for despesas
	deputados_ids = get_deputados_ids()
	load_despesas_tasks = load_deputado_despesas.expand(deputado_id=deputados_ids)

		# Get the absolute path to the SQL file

	load_stage = SQLExecuteQueryOperator(
	task_id="load_stage",
	conn_id=SNOWFLAKE_CONN_ID,           
	sql="sql/load_deputados.sql",  
	params={
		"database": "CAMARA",
		"schema": "RAW"
	}

)
	a = load_full_deputados
	b = load_stage

	

pipeline_camara()