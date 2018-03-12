#!/usr/bin/env bash
set -eufx pipefail
cd "$(dirname "$0")"
function clean {
  find ./monitoring/backup/influxdb -mindepth 1 -delete
  rm -f ./monitoring/backup/grafana.db
  rm -f ./monitoring/backup.tar.gz
}

function backup {
  docker exec influxdb influxd backup -portable /tmp/backup
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
  /usr/local/bin/docker-compose up --build backup
}

clean
backup
compress
upload
clean
