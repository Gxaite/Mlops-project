FROM python:3.11-slim

WORKDIR /mlflow

# Instala dependências
RUN pip install --no-cache-dir \
    mlflow==2.9.2 \
    boto3==1.34.14 \
    psycopg2-binary==2.9.9

# Expõe a porta do MLflow
EXPOSE 5000

# Comando padrão
CMD ["mlflow", "server", "--host", "0.0.0.0", "--port", "5000"]
