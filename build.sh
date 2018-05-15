#!/usr/bin/env bash
set -euxo pipefail
docker-compose up -d grafana
mv monitoring/parser/data /tmp
docker-compose up -d --build log_parser
mv /tmp/data monitoring/parser
docker-compose up -d backup
