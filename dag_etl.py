from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyTableOperator,
    BigQueryDeleteTableOperator,
    BigQueryOperator
)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 0,
}

dag = DAG(
    'ingest_transform_data',
    default_args=default_args,
    description='Ingest and Transform Data from Cloud Storage to BigQuery',
    schedule_interval=None,
)

# Variáveis com informações do projeto e bucket
PROJECT_ID = 'gb-project-385603'
BUCKET = 'us-east1-gb-project-compose-3b944bea-bucket'

# Nome da tabela no BigQuery que já está criada
TABLE_NAME = 'base_17_18_19'

# Caminho para os arquivos CSV no Cloud Storage
CSV_FILE_1 = f'gs://us-east1-gb-project-compose-3b944bea-bucket/dags/base_2017_1.csv'
CSV_FILE_2 = f'gs://us-east1-gb-project-compose-3b944bea-bucket/dags/base_2018_1.csv'
CSV_FILE_3 = f'gs://us-east1-gb-project-compose-3b944bea-bucket/dags/base_2019_2.csv'

# Configurações do BigQuery
BQ_CONN_ID = 'google_cloud_default'
BQ_LOCATION = 'us-east1'
BQ_DATASET_NAME = 'gb-project'

# SQL das transformações necessárias
TRANSFORM_SQL_1 = """
CREATE OR REPLACE TABLE `{dataset}.consolidado_vendas_ano_mes`
AS SELECT
  EXTRACT(YEAR FROM data_venda) AS ano,
  EXTRACT(MONTH FROM data_venda) AS mes,
  SUM(qtd_venda) AS total_vendas
FROM `{dataset}.{table_name}`
GROUP BY 1, 2
"""

TRANSFORM_SQL_2 = """
CREATE OR REPLACE TABLE `{dataset}.consolidado_vendas_marca_linha`
AS SELECT
  marca,
  linha,
  SUM(qtd_venda) AS total_vendas
FROM `{dataset}.{table_name}`
GROUP BY 1, 2
"""

TRANSFORM_SQL_3 = """
CREATE OR REPLACE TABLE `{dataset}.consolidado_vendas_marca_ano_mes`
AS SELECT
  marca,
  EXTRACT(YEAR FROM data_venda) AS ano,
  EXTRACT(MONTH FROM data_venda) AS mes,
  SUM(qtd_venda) AS total_vendas
FROM `{dataset}.{table_name}`
GROUP BY 1, 2, 3
"""

TRANSFORM_SQL_4 = """
CREATE OR REPLACE TABLE `{dataset}.consolidado_vendas_linha_ano_mes`
AS SELECT
  linha,
  EXTRACT(YEAR FROM data_venda) AS ano,
  EXTRACT(MONTH FROM data_venda) AS mes,
  SUM(qtd_venda) AS total_vendas
FROM `{dataset}.{table_name}`
GROUP BY 1, 2, 3
"""

# Define as tarefas da DAG

# Tarefa para criar tabela no BigQuery
create_bigquery_table = BigQueryCreateEmptyTableOperator(
    task_id='create_bigquery_table',
    project_id=PROJECT_ID,
    dataset_id=BQ_DATASET_NAME,
    table_id=TABLE_NAME,
    schema_fields=[
        {'name': 'data_venda', 'type': 'DATE', 'mode': 'REQUIRED'},
        {'name': 'marca', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'linha', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'qtd_venda', 'type': 'INTEGER', 'mode': 'REQUIRED'},
    ],
    bigquery_conn_id=BQ_CONN_ID,
    location=BQ_LOCATION,
    dag=dag,
)

# Tarefa para deletar tabela no BigQuery
delete_bigquery_table = BigQueryDeleteTableOperator(
    task_id='delete_bigquery_table',
    project_id=PROJECT_ID,
    dataset_id=BQ_DATASET_NAME,
    table_id=TABLE_NAME,
    bigquery_conn_id=BQ_CONN_ID,
    ignore_if_missing=True,
    location=BQ_LOCATION,
    dag=dag,
)

# Tarefa para carregar dados do CSV para o BigQuery
load_data_to_bigquery = GCSToBigQueryOperator(
    task_id='load_data_to_bigquery',
    bucket=BUCKET,
    source_objects=[CSV_FILE_1, CSV_FILE_2, CSV_FILE_3],
    destination_project_dataset_table=f"{BQ_DATASET_NAME}.{TABLE_NAME}",
    source_format='CSV',
    create_disposition='CREATE_IF_NEEDED',
    write_disposition='WRITE_TRUNCATE',
    skip_leading_rows=1,
    field_delimiter=',',
    bigquery_conn_id=BQ_CONN_ID,
    google_cloud_storage_conn_id=BQ_CONN_ID,
    location=BQ_LOCATION,
    dag=dag,
)

# Tarefa para executar as transformações no BigQuery
transform_data_in_bigquery_1 = BigQueryOperator(
    task_id='transform_data_in_bigquery_1',
    sql=TRANSFORM_SQL_1.format(dataset=BQ_DATASET_NAME, table_name=TABLE_NAME),
    use_legacy_sql=False,
    bigquery_conn_id=BQ_CONN_ID,
    location=BQ_LOCATION,
    dag=dag,
)

transform_data_in_bigquery_2 = BigQueryOperator(
    task_id='transform_data_in_bigquery_2',
    sql=TRANSFORM_SQL_2.format(dataset=BQ_DATASET_NAME, table_name=TABLE_NAME),
    use_legacy_sql=False,
    bigquery_conn_id=BQ_CONN_ID,
    location=BQ_LOCATION,
    dag=dag,
)

transform_data_in_bigquery_3 = BigQueryOperator(
    task_id='transform_data_in_bigquery_3',
    sql=TRANSFORM_SQL_3.format(dataset=BQ_DATASET_NAME, table_name=TABLE_NAME),
    use_legacy_sql=False,
    bigquery_conn_id=BQ_CONN_ID,
    location=BQ_LOCATION,
    dag=dag,
)

transform_data_in_bigquery_4 = BigQueryOperator(
    task_id='transform_data_in_bigquery_4',
    sql=TRANSFORM_SQL_4.format(dataset=BQ_DATASET_NAME, table_name=TABLE_NAME),
    use_legacy_sql=False,
    bigquery_conn_id=BQ_CONN_ID,
    location=BQ_LOCATION,
    dag=dag,
)

# Definir dependências entre as tarefas
create_bigquery_table >> delete_bigquery_table >> load_data_to_bigquery
load_data_to_bigquery >> [transform_data_in_bigquery_1, transform_data_in_bigquery_2, transform_data_in_bigquery_3, transform_data_in_bigquery_4]

