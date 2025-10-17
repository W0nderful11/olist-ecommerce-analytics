# Local Superset configuration
# Generated for development usage inside project venv.
import os
from secrets import token_urlsafe

# IMPORTANT: SECRET_KEY must be stable across restarts; it is used to encrypt DB passwords
# in Superset metadata. Randomizing it on each run leads to "Invalid decryption key" errors
# after restart. You can still override via env SUPERSET_SECRET_KEY.
SECRET_KEY = os.environ.get(
    'SUPERSET_SECRET_KEY',
    'olist-ecommerce-analytics-dev-secret-key-please-change-in-prod-9d4f1c8c1a3a4f5f8b9c0d1e2f3a4b5c6d7e8f9a0b1c2d3e4f5a6b7c8d9e0f1'
)

# Basic security overrides for local dev
WTF_CSRF_ENABLED = True
SESSION_COOKIE_SAMESITE = None
TALISMAN_ENABLED = False

# Feature flags (enable dashboards cross filters improvements if needed)
FEATURE_FLAGS = {
    'ENABLE_TEMPLATE_PROCESSING': True,
}

# Allow embedding local resources
ENABLE_PROXY_FIX = True

# Force DB sessions to UTC for Postgres only (SQLite doesn't support 'options')
import urllib.parse as _urlparse
_uri = os.environ.get('SQLALCHEMY_DATABASE_URI') or os.environ.get('SUPERSET_DATABASE_URI')
_scheme = ''
try:
    if _uri:
        _scheme = _urlparse.urlparse(_uri).scheme
except Exception:
    _scheme = ''

if _scheme.startswith('postgresql'):
    SQLALCHEMY_ENGINE_OPTIONS = {
        "connect_args": {
            "options": "-c timezone=UTC"
        }
    }
else:
    # leave default engine options (works for default SQLite metadata)
    SQLALCHEMY_ENGINE_OPTIONS = {}
