#!/usr/bin/env bash
set -euxo pipefail
docker-compose up -d grafana
mkdir -p /var/tmp/log_parser
mv monitoring/parser/data /var/tmp/log_parser
docker-compose up -d --build log_parser
mv /var/tmp/log_parser/data monitoring/parser
docker-compose up -d backup
