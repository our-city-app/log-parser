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
import time
import os
from multiprocessing.pool import Pool

from google.cloud import storage

from log_parser import influx
from log_parser.bizz import start_processing_logs, process_logs
from log_parser.config import LogParserConfig
from log_parser.db import DatabaseConnection
import logging

def get_gcs_bucket(cloudstorage_bucket):
    storage_client = storage.Client.from_service_account_json(os.path.join(os.path.dirname(__file__), '..', 'credentials.json'))
    return storage_client.bucket(cloudstorage_bucket)

def process_file(file_name) -> None:
    with open(os.path.join(os.path.dirname(__file__), '..', 'configuration.json'), 'r') as f:
        configuration = LogParserConfig(json.load(f))
    db = DatabaseConnection(os.path.join(os.path.dirname(__file__),'..', 'monitoring', 'parser'))

    cloudstorage_bucket = get_gcs_bucket(configuration.cloudstorage_bucket)

    influxdb_client = influx.get_client(configuration)
    process_logs(db, influxdb_client, cloudstorage_bucket, file_name)

def main(thread_count: int, configuration: LogParserConfig):
    pool = Pool(thread_count)
    db = DatabaseConnection(os.path.join(os.path.dirname(__file__),'..', 'monitoring', 'parser'))
    cloudstorage_bucket = get_gcs_bucket(configuration.cloudstorage_bucket)

    while True:
        for fn in start_processing_logs(db, cloudstorage_bucket):
            if configuration.debug:
                process_file(fn)
            else:
                pool.map(process_file, [fn])
        time.sleep(configuration.interval)


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    parser = argparse.ArgumentParser(description='Processes logs uploaded on cloudstorage')
    parser.add_argument('--threads', type=int, help='Number of threads for the processing')
    args = parser.parse_args()
    with open(os.path.join(os.path.dirname(__file__), '..', 'configuration.json'), 'r') as f:
        configuration = LogParserConfig(json.load(f))
    main(args.threads, configuration)
