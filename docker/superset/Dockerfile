FROM apache/superset:3.1.0

USER root

# Instala drivers de banco de dados
RUN pip install --no-cache-dir \
    psycopg2-binary==2.9.9 \
    sqlalchemy==1.4.50 \
    redis==5.0.1

USER superset

# Copia configuração customizada
COPY superset_config.py /app/pythonpath/superset_config.py

EXPOSE 8088
