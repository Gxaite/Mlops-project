"""
DAG: Ingestão de Dados - Stroke Dataset
Camada: RAW

Baixa o dataset de AVC da internet e salva na camada raw.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import pandas as pd
import os

# Configuracoes
# Dataset de AVC - Stroke Prediction Dataset (seu repositorio)
DATASET_URL = "https://github.com/Gxaite/Mlops-project/raw/main/datasets/healthcare-dataset-stroke-data.xls"
BACKUP_URL = "https://raw.githubusercontent.com/plotly/datasets/master/stroke_prediction_dataset.csv"

DATA_PATH = "/opt/airflow/data"
RAW_PATH = f"{DATA_PATH}/raw"


def ingest_stroke_data(**context):
    """
    Baixa o dataset de AVC da internet e salva como CSV
    """
    import urllib.request
    import tempfile
    
    os.makedirs(RAW_PATH, exist_ok=True)
    output_file = f"{RAW_PATH}/stroke_data.csv"
    
    print(f"Baixando dataset de AVC...")
    
    try:
        # Baixa arquivo Excel e converte para CSV
        temp_file = f"{RAW_PATH}/temp_stroke.xls"
        urllib.request.urlretrieve(DATASET_URL, temp_file)
        print(f"Dataset baixado da URL principal (Excel)")
        
        # Le Excel e salva como CSV
        df = pd.read_excel(temp_file)
        df.to_csv(output_file, index=False)
        os.remove(temp_file)
        print(f"Convertido de Excel para CSV")
        
    except Exception as e:
        print(f"URL principal falhou: {e}")
        print(f"Tentando URL de backup (CSV)...")
        urllib.request.urlretrieve(BACKUP_URL, output_file)
        df = pd.read_csv(output_file)
        print(f"Dataset baixado da URL de backup")
    
    # Valida o arquivo
    df = pd.read_csv(output_file)
    print(f"Dataset carregado: {len(df)} linhas, {len(df.columns)} colunas")
    print(f"Colunas: {list(df.columns)}")
    
    return output_file


default_args = {
    'owner': 'mlops',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='01_ingest_stroke_raw',
    default_args=default_args,
    description='Ingestao do dataset de AVC - Camada RAW',
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['ingest', 'raw', 'stroke'],
) as dag:
    
    start = EmptyOperator(task_id='start')
    
    ingest = PythonOperator(
        task_id='ingest_stroke_data',
        python_callable=ingest_stroke_data,
    )
    
    end = EmptyOperator(task_id='end')
    
    start >> ingest >> end
