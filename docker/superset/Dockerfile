FROM apache/superset:3.1.0

USER root

# Instala drivers de banco de dados (versões compatíveis)
RUN pip install --no-cache-dir \
    psycopg2-binary==2.9.9 \
    redis==4.6.0

USER superset

# Copia configuração customizada
COPY superset_config.py /app/pythonpath/superset_config.py

EXPOSE 8088
