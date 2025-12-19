.PHONY: help up down restart logs ps clean init

# ============================================
# MLOps Project - Makefile
# ============================================

help: ## Mostra esta ajuda
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

# ============================================
# Docker Compose Commands
# ============================================

init: ## Inicializa o projeto (primeira vez)
	@echo "🚀 Inicializando projeto MLOps..."
	@mkdir -p dags logs plugins src data
	@mkdir -p src/data src/features src/models src/utils
	@mkdir -p data/raw data/processed data/features
	@echo "AIRFLOW_UID=$$(id -u)" >> .env
	@chmod +x docker/postgres/init-multiple-dbs.sh
	@echo "✅ Projeto inicializado!"

up: ## Sobe todos os containers
	@echo "🚀 Subindo containers..."
	docker-compose up -d
	@echo "✅ Containers iniciados!"
	@echo ""
	@echo "📊 Serviços disponíveis:"
	@echo "   - Airflow:  http://localhost:8080  (airflow/airflow)"
	@echo "   - MLflow:   http://localhost:5000"
	@echo "   - MinIO:    http://localhost:9001  (minio/minio123)"

down: ## Para todos os containers
	@echo "🛑 Parando containers..."
	docker-compose down
	@echo "✅ Containers parados!"

restart: ## Reinicia todos os containers
	@echo "🔄 Reiniciando containers..."
	docker-compose restart
	@echo "✅ Containers reiniciados!"

logs: ## Mostra logs de todos os containers
	docker-compose logs -f

logs-airflow: ## Mostra logs do Airflow
	docker-compose logs -f airflow-webserver airflow-scheduler airflow-worker

logs-mlflow: ## Mostra logs do MLflow
	docker-compose logs -f mlflow

ps: ## Lista containers em execução
	docker-compose ps

clean: ## Remove containers, volumes e imagens
	@echo "🧹 Limpando ambiente..."
	docker-compose down -v --rmi local
	@rm -rf logs/*
	@echo "✅ Ambiente limpo!"

# ============================================
# Development Commands
# ============================================

build: ## Rebuilda as imagens
	docker-compose build --no-cache

shell-airflow: ## Abre shell no container do Airflow
	docker-compose exec airflow-webserver bash

shell-mlflow: ## Abre shell no container do MLflow
	docker-compose exec mlflow bash

# ============================================
# Airflow Commands
# ============================================

airflow-info: ## Mostra informações do Airflow
	docker-compose exec airflow-webserver airflow info

airflow-dags: ## Lista DAGs do Airflow
	docker-compose exec airflow-webserver airflow dags list

# ============================================
# Database Commands
# ============================================

db-shell: ## Abre shell do PostgreSQL
	docker-compose exec postgres psql -U airflow -d airflow
