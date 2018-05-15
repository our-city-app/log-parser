#!/usr/bin/env bash
set -euxo pipefail
docker-compose up -d grafana
mv monitoring/parser/data .
docker-compose up -d --build log_parser
mv data monitoring/parser
docker-compose up -d backup
