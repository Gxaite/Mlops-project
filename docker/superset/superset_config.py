import os

# ============================================
# Superset Configuration
# ============================================

# Security
SECRET_KEY = os.environ.get('SUPERSET_SECRET_KEY', 'supersecretkey123')

# Database
SQLALCHEMY_DATABASE_URI = os.environ.get('DATABASE_URL', 'postgresql://airflow:airflow@postgres:5432/superset')

# Cache
CACHE_CONFIG = {
    'CACHE_TYPE': 'RedisCache',
    'CACHE_DEFAULT_TIMEOUT': 300,
    'CACHE_KEY_PREFIX': 'superset_',
    'CACHE_REDIS_URL': os.environ.get('REDIS_URL', 'redis://redis:6379/1'),
}

# Celery (para queries assíncronas)
class CeleryConfig:
    broker_url = os.environ.get('REDIS_URL', 'redis://redis:6379/1')
    result_backend = os.environ.get('REDIS_URL', 'redis://redis:6379/1')

CELERY_CONFIG = CeleryConfig

# Feature Flags
FEATURE_FLAGS = {
    'ENABLE_TEMPLATE_PROCESSING': True,
    'DASHBOARD_NATIVE_FILTERS': True,
    'DASHBOARD_CROSS_FILTERS': True,
    'DASHBOARD_NATIVE_FILTERS_SET': True,
    'ALERT_REPORTS': True,
}

# Configurações de UI
APP_NAME = 'MLOps Analytics'
APP_ICON = '/static/assets/images/superset-logo-horiz.png'

# Timezone
BABEL_DEFAULT_TIMEZONE = 'America/Sao_Paulo'

# Row limit
ROW_LIMIT = 50000
SQL_MAX_ROW = 100000

# Habilita upload de CSV
CSV_UPLOAD = True
EXCEL_UPLOAD = True

# Logs
ENABLE_PROXY_FIX = True
