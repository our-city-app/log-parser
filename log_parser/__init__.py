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
from collections import defaultdict
from multiprocessing import SimpleQueue
from multiprocessing.pool import Pool
from typing import Tuple

from google.cloud import storage
from influxdb import InfluxDBClient

from log_parser.bizz import process_logs, get_new_files_to_process
from log_parser.config import LogParserConfig
from log_parser.db import DatabaseConnection

logging.basicConfig(format='%(levelname)-8s %(process)s %(asctime)s,%(msecs)3.0f [%(filename)s:%(lineno)d] %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging.DEBUG)
logging.getLogger('urllib3').setLevel(logging.WARNING)
CURRENT_DIR = os.path.dirname(__file__)

finished_queue = SimpleQueue()


def get_client(config: LogParserConfig) -> InfluxDBClient:
    return InfluxDBClient(host=config.influxdb.host,
                          port=config.influxdb.port,
                          ssl=config.influxdb.ssl,
                          verify_ssl=config.influxdb.ssl,
                          database=config.influxdb.db,
                          username=config.influxdb.username,
                          password=config.influxdb.password)


def main(process_count: int, data_path: str):
    with open(os.path.join(os.path.dirname(__file__), 'configuration.json'), 'r') as f:
        configuration = LogParserConfig(json.load(f))
    db = DatabaseConnection(data_path)
    logging.info('Starting log parser with %s processes', process_count)
    process(configuration, db, process_count)


def get_gcs_bucket(cloudstorage_bucket):
    storage_client = storage.Client.from_service_account_json(os.path.join(CURRENT_DIR, 'credentials.json'))
    return storage_client.bucket(cloudstorage_bucket)


def process_file(bucket_name: str, file_name: str, download_directory: str, influxdb_client: InfluxDBClient) -> Tuple[
    bool, str, str]:
    """Processes the file in a separate process."""
    try:
        cloudstorage_bucket = get_gcs_bucket(bucket_name)
        process_logs(download_directory, influxdb_client, cloudstorage_bucket, file_name)
        return True, bucket_name, file_name
    except Exception as e:
        logging.error('Failed to process file %s', file_name)
        logging.exception(e)
        return False, bucket_name, file_name


def after_processed(processed_file: Tuple[bool, str, str]) -> None:
    success, bucket_name, filename = processed_file
    if success:
        logging.info('%s: Finished processing file %s', bucket_name, filename)
    else:
        logging.info('%s: Failed to process file %s, will retry', bucket_name, filename)
    finished_queue.put(processed_file)


def after_error(result):
    # This should not be called since we try / catch everything in the 'process_file' function
    logging.error('Failed to process file')
    logging.exception(result)


def process(configuration: LogParserConfig, db: DatabaseConnection, process_count: int):
    influxdb_client = get_client(configuration)
    pool = Pool(process_count)
    currently_processing = defaultdict(list)
    dl_dir = os.path.join(db.root_dir, 'data')

    while True:
        logging.info('Checking for new files to process')
        settings = db.get_settings()
        new_files = get_new_files_to_process(configuration.buckets, settings)
        settings.files.extend(new_files)
        timestamp = int(time.time())
        completed_files = defaultdict(list)
        # Empty queue of finished work and create a lists of all completed files per bucket
        while True:
            if finished_queue.empty():
                break
            success, bucket, filename = finished_queue.get()
            currently_processing[bucket].remove(filename)
            if success:
                completed_files[bucket].append(filename)
        # Set processed timestamp on processed files
        for file in settings.files:
            if file.name in completed_files[file.bucket]:
                file.processed_timestamp = timestamp
        logging.info('%d files completed processing since last loop', sum(len(l) for l in completed_files.values()))
        completed_files.clear()
        db.save_settings(settings)
        added = 0
        for file in settings.files:
            if file.processed_timestamp is None and file.name not in currently_processing[file.bucket]:
                currently_processing[file.bucket].append(file.name)
                pool.apply_async(process_file, (file.bucket, file.name, dl_dir, influxdb_client), {}, after_processed,
                                 after_error)
                added += 1
        processing_count = sum(len(l) for l in currently_processing.values())
        if added:
            logging.info('Added %s files to pool, %s files currently processing.', added, processing_count)
        else:
            logging.info('Nothing new to process, sleeping %s seconds. %s files currently in queue to be processed.',
                         configuration.interval, processing_count)
            time.sleep(configuration.interval)
            continue


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    parser = argparse.ArgumentParser(description='Processes logs uploaded on cloudstorage')
    parser.add_argument('--processes', type=int, help='Number of processes to use', default=os.cpu_count() * 2)
    parser.add_argument('--data_path', type=str, help='Path where the data will be stored. Defaults to ../parser',
                        default=os.path.join(CURRENT_DIR, '..', 'parser'))
    args = parser.parse_args()
    main(args.processes, args.data_path)
