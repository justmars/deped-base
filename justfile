set dotenv-load

db := env('DB_FILE')

start:
  source .venv/bin/activate && \
  cli prep && \
  cli build

# Manually checkpoint the WAL (Write-Ahead Logging) file.
db-check:
  sqlite3 {{db}} 'PRAGMA wal_checkpoint(TRUNCATE);'
