"""
DAG: Processamento Gold - Stroke Dataset
Camada: GOLD

Feature Engineering para Machine Learning:
- Encoding de variaveis categoricas
- Criacao de novas features
- Dataset pronto para treinamento
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import pandas as pd
import os

DATA_PATH = "/opt/airflow/data"
SILVER_PATH = f"{DATA_PATH}/silver"
GOLD_PATH = f"{DATA_PATH}/gold"


def process_stroke_gold(**context):
    """
    Feature Engineering - Prepara dados para ML
    """
    os.makedirs(GOLD_PATH, exist_ok=True)
    
    input_file = f"{SILVER_PATH}/stroke_data.parquet"
    
    print(f"Processando camada Gold...")
    print(f"Lendo: {input_file}")
    
    if not os.path.exists(input_file):
        raise FileNotFoundError(f"Arquivo nao encontrado: {input_file}. Execute a DAG Silver primeiro!")
    
    df = pd.read_parquet(input_file)
    df_encoded = df.copy()
    
    # ========== FEATURE ENGINEERING ==========
    
    # 1. Encoding de variaveis categoricas
    categorical_cols = ['gender', 'ever_married', 'work_type', 'Residence_type', 'smoking_status']
    
    for col in categorical_cols:
        if col in df_encoded.columns:
            df_encoded[f'{col}_encoded'] = df_encoded[col].astype('category').cat.codes
            print(f"Encoded: {col}")
    
    # 2. Feature: Faixa etaria
    df_encoded['age_group'] = pd.cut(
        df_encoded['age'], 
        bins=[0, 18, 35, 50, 65, 100],
        labels=[0, 1, 2, 3, 4]  # child, young_adult, adult, middle_age, senior
    )
    print("Criada feature: age_group")
    
    # 3. Feature: Risco cardiovascular (combinacao de fatores)
    df_encoded['cardio_risk'] = (
        df_encoded['hypertension'] + 
        df_encoded['heart_disease'] + 
        (df_encoded['age'] > 55).astype(int) +
        (df_encoded['avg_glucose_level'] > 140).astype(int)
    )
    print("Criada feature: cardio_risk")
    
    # 4. Feature: Categoria de BMI
    df_encoded['bmi_category'] = pd.cut(
        df_encoded['bmi'],
        bins=[0, 18.5, 25, 30, 100],
        labels=[0, 1, 2, 3]  # underweight, normal, overweight, obese
    )
    print("Criada feature: bmi_category")
    
    # 5. Feature: Glucose category
    df_encoded['glucose_category'] = pd.cut(
        df_encoded['avg_glucose_level'],
        bins=[0, 100, 140, 200, 500],
        labels=[0, 1, 2, 3]  # normal, prediabetes, diabetes, high
    )
    print("Criada feature: glucose_category")
    
    # ========== DATASETS DE SAIDA ==========
    
    # Dataset completo (para analise exploratoria)
    full_output = f"{GOLD_PATH}/stroke_full.parquet"
    df_encoded.to_parquet(full_output, index=False)
    print(f"Gold (full) salvo: {full_output}")
    
    # Dataset ML Ready (apenas features numericas + target)
    feature_cols = [
        'age', 'hypertension', 'heart_disease', 'avg_glucose_level', 'bmi',
        'gender_encoded', 'ever_married_encoded', 'work_type_encoded',
        'Residence_type_encoded', 'smoking_status_encoded',
        'age_group', 'cardio_risk', 'bmi_category', 'glucose_category'
    ]
    
    target_col = 'stroke'
    
    ml_features = [col for col in feature_cols if col in df_encoded.columns]
    df_ml = df_encoded[ml_features + [target_col]].copy()
    
    # Converte categorias para int
    for col in df_ml.columns:
        if df_ml[col].dtype.name == 'category':
            df_ml[col] = df_ml[col].astype(int)
    
    ml_output = f"{GOLD_PATH}/stroke_ml_ready.parquet"
    df_ml.to_parquet(ml_output, index=False)
    print(f"Gold (ML ready) salvo: {ml_output}")
    
    # ========== ESTATISTICAS FINAIS ==========
    
    print(f"\nDataset ML Ready:")
    print(f"   Shape: {df_ml.shape}")
    print(f"   Features: {len(ml_features)}")
    print(f"   Target: {target_col}")
    print(f"\nDistribuicao do Target:")
    print(f"   Sem AVC (0): {(df_ml[target_col] == 0).sum()} ({(df_ml[target_col] == 0).mean()*100:.1f}%)")
    print(f"   Com AVC (1): {(df_ml[target_col] == 1).sum()} ({(df_ml[target_col] == 1).mean()*100:.1f}%)")
    print(f"\nATENCAO: Dataset desbalanceado! Considere tecnicas de balanceamento no treino.")
    
    return ml_output


default_args = {
    'owner': 'mlops',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='04_process_stroke_gold',
    default_args=default_args,
    description='Processamento Gold - Feature Engineering para ML',
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['gold', 'stroke', 'feature_engineering', 'ml'],
) as dag:
    
    start = EmptyOperator(task_id='start')
    
    process_gold = PythonOperator(
        task_id='process_gold',
        python_callable=process_stroke_gold,
    )
    
    end = EmptyOperator(task_id='end')
    
    start >> process_gold >> end
