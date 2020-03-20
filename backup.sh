#!/usr/bin/env bash
set -eufx pipefail
cd "$(dirname "$0")"
function clean {
  find ./monitoring/backup/influxdb -mindepth 1 -delete
  rm -f ./monitoring/backup/grafana.db
}

function backup {
  docker exec influxdb influxd backup -portable /tmp/backup
  # Backup grafana data
  sqlite3 monitoring/grafana/grafana.db ".backup './monitoring/backup/grafana.db'"
  cp ./monitoring/parser/settings.json ./monitoring/backup/settings.json
}

function upload {
  credentials_file=./backup/credentials.json
  if [ ! -f ${credentials_file} ]; then
    echo "No google credentials file present in location ${credentials_file}"
    exit 1
  fi
  /usr/local/bin/docker-compose up --build backup
}

clean
backup
upload
clean
