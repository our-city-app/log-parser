# -*- coding: utf-8 -*-
# Copyright 2018 GIG Technology NV
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# @@license_version:1.4@@

import argparse
import json
import logging
import os
import time
from multiprocessing.pool import Pool

from google.cloud import storage
from influxdb import InfluxDBClient

from log_parser.bizz import start_processing_logs, process_logs
from log_parser.config import LogParserConfig
from log_parser.db import DatabaseConnection

logging.basicConfig(format='%(levelname)-8s %(process)s %(asctime)s,%(msecs)3.0f [%(filename)s:%(lineno)d] %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging.DEBUG)
logging.getLogger('urllib3').setLevel(logging.WARNING)
CURRENT_DIR = os.path.dirname(__file__)


def get_gcs_bucket(cloudstorage_bucket):
    storage_client = storage.Client.from_service_account_json(os.path.join(CURRENT_DIR, 'credentials.json'))
    return storage_client.bucket(cloudstorage_bucket)


def process_file(file_name: str, db: DatabaseConnection, influxdb_client: InfluxDBClient,
                 configuration: LogParserConfig) -> None:
    cloudstorage_bucket = get_gcs_bucket(configuration.cloudstorage_bucket)
    process_logs(db, influxdb_client, cloudstorage_bucket, file_name)


def get_client(config: LogParserConfig) -> InfluxDBClient:
    return InfluxDBClient(host=config.influxdb.host,
                          port=config.influxdb.port,
                          ssl=config.influxdb.ssl,
                          verify_ssl=config.influxdb.ssl,
                          database=config.influxdb.db,
                          username=config.influxdb.username,
                          password=config.influxdb.password)


def main(process_count: int):
    logging.info('Starting log parser with %s processes', process_count)
    pool = Pool(process_count, maxtasksperchild=1)
    with open(os.path.join(os.path.dirname(__file__), 'configuration.json'), 'r') as f:
        configuration = LogParserConfig(json.load(f))
    db = DatabaseConnection(os.path.join(CURRENT_DIR, '..', 'parser'))
    cloudstorage_bucket = get_gcs_bucket(configuration.cloudstorage_bucket)
    influxdb_client = get_client(configuration)
    while True:
        iterable = list(start_processing_logs(db, cloudstorage_bucket))
        if len(iterable) == 0:
            logging.info('Nothing to process, sleeping %s seconds.', configuration.interval)
            time.sleep(configuration.interval)
            continue
        logging.info('Processing %s files', len(iterable))
        for file_name in iterable:
            if configuration.debug:
                process_file(file_name, db, influxdb_client, configuration)
            else:
                pool.apply_async(process_file, [file_name, db, influxdb_client, configuration])




if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    parser = argparse.ArgumentParser(description='Processes logs uploaded on cloudstorage')
    parser.add_argument('--processes', type=int, help='Number of processes to use', default=os.cpu_count() * 2)
    args = parser.parse_args()
    main(args.processes)
