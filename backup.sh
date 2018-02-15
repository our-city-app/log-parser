#!/usr/bin/env bash
set -eufx pipefail
function clean {
  find ./monitoring/backup/influxdb -mindepth 1 -delete
  rm -f ./monitoring/backup/grafana.db
  rm -f ./monitoring/backup.tar.gz
}

function backup {
  # Backup metastore (system info, user info, db shards)
  docker exec influxdb influxd backup /tmp/backup
  # Backup data
  docker exec influxdb influxd backup -database monitoring /tmp/backup
  # Backup grafana data
  sqlite3 monitoring/grafana/grafana.db ".backup './monitoring/backup/grafana.db'"
}

function compress {
  tar -cjvf ./monitoring/backup.tar.gz ./monitoring/backup
}

function upload {
  credentials_file=./backup/credentials.json
  if [ ! -f ${credentials_file} ]; then
    echo "No google credentials file present in location ${credentials_file}"
    exit 1
  fi
  docker-compose up --build backup
}

clean
backup
compress
upload
clean
