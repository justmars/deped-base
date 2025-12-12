set dotenv-load

db := env('DB_FILE')

# upload to pypi
publish:
  python -m build && \
  python -m twine upload dist/* -u __token__ -p $PYPI_TOKEN

# Manually checkpoint the WAL (Write-Ahead Logging) file.
db-check:
  sqlite3 {{db}} 'PRAGMA wal_checkpoint(TRUNCATE);'
