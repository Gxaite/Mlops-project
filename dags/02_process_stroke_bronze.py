"""
DAG: Processamento Bronze - Stroke Dataset
Camada: BRONZE

Converte os dados raw (CSV) para formato Parquet.
Adiciona metadados de ingestao.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.filesystem import FileSensor
from datetime import datetime, timedelta
import pandas as pd
import os

DATA_PATH = "/opt/airflow/data"
RAW_PATH = f"{DATA_PATH}/raw"
BRONZE_PATH = f"{DATA_PATH}/bronze"


def process_stroke_bronze(**context):
    """
    Converte CSV para Parquet e adiciona metadados
    """
    os.makedirs(BRONZE_PATH, exist_ok=True)
    
    input_file = f"{RAW_PATH}/stroke_data.csv"
    output_file = f"{BRONZE_PATH}/stroke_data.parquet"
    
    print(f"Processando camada Bronze...")
    print(f"Lendo: {input_file}")
    
    # Verifica se arquivo existe
    if not os.path.exists(input_file):
        raise FileNotFoundError(f"Arquivo nao encontrado: {input_file}. Execute a DAG de ingestao primeiro!")
    
    df = pd.read_csv(input_file)
    
    # Adiciona metadados
    df['_ingestion_date'] = datetime.now().isoformat()
    df['_source'] = 'kaggle_stroke_dataset'
    df['_file_name'] = 'stroke_data.csv'
    
    # Salva como Parquet
    df.to_parquet(output_file, index=False)
    
    print(f"Bronze salvo: {output_file}")
    print(f"Shape: {df.shape}")
    print(f"Colunas: {list(df.columns)}")
    
    return output_file


default_args = {
    'owner': 'mlops',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='02_process_stroke_bronze',
    default_args=default_args,
    description='Processamento Bronze - CSV para Parquet',
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['bronze', 'stroke', 'etl'],
) as dag:
    
    start = EmptyOperator(task_id='start')
    
    process_bronze = PythonOperator(
        task_id='process_bronze',
        python_callable=process_stroke_bronze,
    )
    
    end = EmptyOperator(task_id='end')
    
    start >> process_bronze >> end
