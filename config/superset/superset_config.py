"""
Superset Configuration
"""
import os

# Superset secret key
SECRET_KEY = os.environ.get('SUPERSET_SECRET_KEY', 'your_secret_key_here')

# SQLAlchemy connection string for metadata database
SQLALCHEMY_DATABASE_URI = (
    f"postgresql://{os.environ.get('POSTGRES_USER', 'admin')}:"
    f"{os.environ.get('POSTGRES_PASSWORD', 'password')}@"
    f"postgres:5432/superset_metadata"
)

# Flask-WTF flag for CSRF
WTF_CSRF_ENABLED = True
WTF_CSRF_EXEMPT_LIST = []
WTF_CSRF_TIME_LIMIT = 60 * 60 * 24 * 365

# Cache configuration
CACHE_CONFIG = {
    'CACHE_TYPE': 'RedisCache',
    'CACHE_DEFAULT_TIMEOUT': 60,
    'CACHE_KEY_PREFIX': 'superset_',
    'CACHE_REDIS_URL': 'redis://redis:6379/1',
}

# Data cache
DATA_CACHE_CONFIG = {
    'CACHE_TYPE': 'RedisCache',
    'CACHE_DEFAULT_TIMEOUT': 60,
    'CACHE_KEY_PREFIX': 'superset_data_',
    'CACHE_REDIS_URL': 'redis://redis:6379/2',
}

# Results backend
RESULTS_BACKEND = {
    'CACHE_TYPE': 'RedisCache',
    'CACHE_DEFAULT_TIMEOUT': 60 * 60 * 24,
    'CACHE_KEY_PREFIX': 'superset_results_',
    'CACHE_REDIS_URL': 'redis://redis:6379/3',
}

# Feature flags
FEATURE_FLAGS = {
    'DASHBOARD_NATIVE_FILTERS': True,
    'DASHBOARD_CROSS_FILTERS': True,
    'DASHBOARD_NATIVE_FILTERS_SET': True,
    'ENABLE_TEMPLATE_PROCESSING': True,
    'EMBEDDED_SUPERSET': True,
    'ALERT_REPORTS': True,
}

# Timezone
SUPERSET_WEBSERVER_TIMEOUT = 60
SQLLAB_TIMEOUT = 300
SUPERSET_WEBSERVER_PORT = 8088

# Enable scheduled reports
ENABLE_SCHEDULED_EMAIL_REPORTS = True

# Allowed database URIs
SQLALCHEMY_CUSTOM_PASSWORD_STORE = None
SQLALCHEMY_TRACK_MODIFICATIONS = False

# Database engine specs
PREFERRED_DATABASES = [
    'postgresql',
    'clickhouse',
]

# Row limit
ROW_LIMIT = 50000
SQL_MAX_ROW = 100000

# Auto-refresh settings
SUPERSET_DASHBOARD_PERIODICAL_REFRESH_LIMIT = 30
SUPERSET_DASHBOARD_PERIODICAL_REFRESH_WARNING_MESSAGE = None

# Logging
ENABLE_PROXY_FIX = True
LOG_FORMAT = '%(asctime)s:%(levelname)s:%(name)s:%(message)s'
LOG_LEVEL = 'INFO'

# Additional database settings
EXTRA_CATEGORICAL_COLOR_SCHEMES = []

# Public role settings
PUBLIC_ROLE_LIKE = 'Gamma'

# Alert and report settings
ALERT_REPORTS_NOTIFICATION_DRY_RUN = False

# Webserver settings
ENABLE_CORS = True
CORS_OPTIONS = {
    'supports_credentials': True,
    'allow_headers': ['*'],
    'resources': ['*'],
    'origins': ['*']
}
