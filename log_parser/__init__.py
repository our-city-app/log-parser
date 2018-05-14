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
from typing import Set, Tuple

from google.cloud import storage
from google.cloud.storage import Bucket
from influxdb import InfluxDBClient

from log_parser.bizz import start_processing_logs, process_logs, clean_old_files
from log_parser.config import LogParserConfig
from log_parser.db import DatabaseConnection

logging.basicConfig(format='%(levelname)-8s %(process)s %(asctime)s,%(msecs)3.0f [%(filename)s:%(lineno)d] %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging.DEBUG)
logging.getLogger('urllib3').setLevel(logging.WARNING)
CURRENT_DIR = os.path.dirname(__file__)

queue: Set[str] = set()
processed_files = set()


def get_gcs_bucket(cloudstorage_bucket):
    storage_client = storage.Client.from_service_account_json(os.path.join(CURRENT_DIR, 'credentials.json'))
    return storage_client.bucket(cloudstorage_bucket)


def process_file(file_name: str, db: DatabaseConnection, influxdb_client: InfluxDBClient,
                 configuration: LogParserConfig) -> Tuple[bool, str]:
    cloudstorage_bucket = get_gcs_bucket(configuration.cloudstorage_bucket)
    try:
        process_logs(db, influxdb_client, cloudstorage_bucket, file_name)
        return True, file_name
    except Exception as e:
        logging.error('Failed to process file %s', file_name)
        logging.exception(e)
        return False, file_name


def get_client(config: LogParserConfig) -> InfluxDBClient:
    return InfluxDBClient(host=config.influxdb.host,
                          port=config.influxdb.port,
                          ssl=config.influxdb.ssl,
                          verify_ssl=config.influxdb.ssl,
                          database=config.influxdb.db,
                          username=config.influxdb.username,
                          password=config.influxdb.password,
                          retries=5)


def main(process_count: int, data_path: str, clean: bool):
    with open(os.path.join(os.path.dirname(__file__), 'configuration.json'), 'r') as f:
        configuration = LogParserConfig(json.load(f))
    db = DatabaseConnection(data_path)
    cloudstorage_bucket = get_gcs_bucket(configuration.cloudstorage_bucket)
    if clean:
        clean_old_files(db, cloudstorage_bucket)
    else:
        logging.info('Starting log parser with %s processes', process_count)
        process(configuration, db, cloudstorage_bucket, process_count)


def after_processed(processed_file: Tuple[bool, str]) -> None:
    success, filename = processed_file
    if success:
        processed_files.add(filename)
    else:
        logging.info('Failed to process file %s, will retry', processed_file)
    if filename in queue:
        queue.remove(filename)


def after_error(result):
    logging.exception(result)


def process(configuration: LogParserConfig, db: DatabaseConnection, cloudstorage_bucket: Bucket, process_count: int):
    influxdb_client = get_client(configuration)
    pool = Pool(process_count, maxtasksperchild=1)

    while True:
        new_files = [f for f in start_processing_logs(db, cloudstorage_bucket)
                     if f not in queue and f not in processed_files]
        if len(new_files) == 0:
            logging.info('Nothing to process, sleeping %s seconds. %s tasks still in queue.', configuration.interval,
                         len(queue))
            time.sleep(configuration.interval)
            continue
        logging.info('Processing %s files', len(new_files))
        for file_name in new_files:
            queue.add(file_name)
            pool.apply_async(process_file, [file_name, db, influxdb_client, configuration], {}, after_processed,
                             after_error)


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    parser = argparse.ArgumentParser(description='Processes logs uploaded on cloudstorage')
    parser.add_argument('--processes', type=int, help='Number of processes to use', default=os.cpu_count() * 2)
    parser.add_argument('--data_path', type=str, help='Path where the data will be stored. Defaults to ../parser',
                        default=os.path.join(CURRENT_DIR, '..', 'parser'))
    parser.add_argument('--clean', action='store_true', help='Clean old data files to free up disk space',
                        default=False)
    args = parser.parse_args()
    main(args.processes, args.data_path, args.clean)
