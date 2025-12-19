"""
DAG: Processamento Silver - Stroke Dataset
Camada: SILVER

Limpa e transforma os dados:
- Remove colunas desnecessarias
- Trata valores nulos
- Remove duplicatas
- Padroniza tipos de dados
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import pandas as pd
import os

DATA_PATH = "/opt/airflow/data"
BRONZE_PATH = f"{DATA_PATH}/bronze"
SILVER_PATH = f"{DATA_PATH}/silver"


def process_stroke_silver(**context):
    """
    Limpa e transforma os dados da camada Bronze
    """
    os.makedirs(SILVER_PATH, exist_ok=True)
    
    input_file = f"{BRONZE_PATH}/stroke_data.parquet"
    output_file = f"{SILVER_PATH}/stroke_data.parquet"
    
    print(f"Processando camada Silver...")
    print(f"Lendo: {input_file}")
    
    if not os.path.exists(input_file):
        raise FileNotFoundError(f"Arquivo nao encontrado: {input_file}. Execute a DAG Bronze primeiro!")
    
    df = pd.read_parquet(input_file)
    initial_rows = len(df)
    print(f"Linhas iniciais: {initial_rows}")
    
    # ========== LIMPEZA DE DADOS ==========
    
    # 1. Remove coluna 'id' (nao e feature util)
    if 'id' in df.columns:
        df = df.drop('id', axis=1)
        print("Removida coluna 'id'")
    
    # 2. Trata valores 'N/A' em bmi
    if 'bmi' in df.columns:
        df['bmi'] = pd.to_numeric(df['bmi'], errors='coerce')
        null_count = df['bmi'].isnull().sum()
        median_bmi = df['bmi'].median()
        df['bmi'] = df['bmi'].fillna(median_bmi)
        print(f"BMI: {null_count} nulos preenchidos com mediana ({median_bmi:.2f})")
    
    # 3. Remove duplicatas
    df = df.drop_duplicates()
    removed = initial_rows - len(df)
    print(f"Removidas {removed} duplicatas")
    
    # 4. Padroniza tipos
    df['age'] = df['age'].astype(float)
    df['hypertension'] = df['hypertension'].astype(int)
    df['heart_disease'] = df['heart_disease'].astype(int)
    df['stroke'] = df['stroke'].astype(int)
    print("Tipos padronizados")
    
    # 5. Adiciona metadados
    df['_processed_date'] = datetime.now().isoformat()
    
    # Salva
    df.to_parquet(output_file, index=False)
    
    print(f"Silver salvo: {output_file}")
    print(f"Shape final: {df.shape}")
    
    # Estatisticas
    print(f"\nEstatisticas:")
    print(f"   - Idade media: {df['age'].mean():.1f}")
    print(f"   - % Hipertensao: {df['hypertension'].mean()*100:.1f}%")
    print(f"   - % Doenca cardiaca: {df['heart_disease'].mean()*100:.1f}%")
    print(f"   - % AVC (target): {df['stroke'].mean()*100:.2f}%")
    
    return output_file


default_args = {
    'owner': 'mlops',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='03_process_stroke_silver',
    default_args=default_args,
    description='Processamento Silver - Limpeza e transformacao',
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['silver', 'stroke', 'etl', 'cleaning'],
) as dag:
    
    start = EmptyOperator(task_id='start')
    
    process_silver = PythonOperator(
        task_id='process_silver',
        python_callable=process_stroke_silver,
    )
    
    end = EmptyOperator(task_id='end')
    
    start >> process_silver >> end
