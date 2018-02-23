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
from multiprocessing.pool import Pool

from google.cloud import storage

from log_parser import influx
from log_parser.bizz import start_processing_logs, process_logs
from log_parser.config import LogParserConfig
from log_parser.db import DatabaseConnection


def main(thread_count: int, configuration: LogParserConfig):
    pool = Pool(thread_count)
    db = DatabaseConnection('../monitoring/parser/db.sqlite')
    # todo: auth via service account (try GOOGLE_APPLICATION_CREDENTIALS=credentials.json env var?)
    cloudstorage_bucket = storage.Client().bucket(configuration.cloudstorage_bucket)
    influxdb_client = influx.get_client(configuration)

    def process_file(file_name: str) -> None:
        process_logs(db, influxdb_client, cloudstorage_bucket, file_name)

    while True:
        pool.map(process_file, start_processing_logs(db, cloudstorage_bucket))
        time.sleep(configuration.interval)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Processes logs uploaded on cloudstorage')
    parser.add_argument('--threads', type=int, help='Number of threads for the processing')
    args = parser.parse_args()
    with open('../configuration.json', 'r') as f:
        configuration = LogParserConfig(json.load(f))
    main(args.threads, configuration)
