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
import logging
import os
from datetime import datetime
from typing import Optional, List

from google.cloud import storage
from google.cloud.storage import Bucket, Blob
from influxdb import InfluxDBClient
from influxdb.exceptions import InfluxDBClientError

from log_parser.analyzer import analyze
from log_parser.db import DatabaseConnection, create_folder
from log_parser.models import LogParserFile, LogParserSettings

MAX_DB_ENTRIES_PER_RPC = 2000
storage_client = storage.Client.from_service_account_json(os.path.join(os.path.dirname(__file__), 'credentials.json'))


def _get_foldername(file_path) -> str:
    return file_path.split('/')[-2]


def _get_filename(file_path) -> str:
    return file_path.split('/')[-1]


def get_log_folder(date: datetime) -> str:
    return u'%04d-%02d-%02d %02d:00:00' % (date.year, date.month, date.day, date.hour)


def _get_date_from_filename(filename):
    return datetime.strptime(_get_foldername(filename), '%Y-%m-%d %H:%M:%S')


def _get_next_date(cloudstorage_bucket: Bucket, min_date: datetime = None) -> Optional[datetime]:
    directories = sorted([f.name for f in cloudstorage_bucket.list_blobs()])
    if not directories:
        return None
    if not min_date:
        return _get_date_from_filename(directories[0])
    for directory in directories:
        dir_date = _get_date_from_filename(directory)
        if dir_date > min_date:
            return dir_date
    else:
        return None


def get_new_files_to_process(buckets: List[str], settings: LogParserSettings) -> List[LogParserFile]:
    file_names = [f.name for f in settings.files]
    new_files = []
    for bucket_name in buckets:
        bucket = storage_client.bucket(bucket_name)
        # TODO provide a date prefix so we don't have to fetch every single filename every time
        # This takes long because a request is done for every 1000 files
        for file in bucket.list_blobs():  # type: Blob
            if file.name not in file_names and file.name.endswith('.json'):
                new_files.append(LogParserFile(file.name, None, bucket_name))
    return new_files


def get_unprocessed_logs(db: DatabaseConnection, cloudstorage_bucket: Bucket, year: str) -> List[str]:
    done_log_filenames = db.get_all_processed_logs(year)
    filenames_in_year = map(lambda b: b.name, cloudstorage_bucket.list_blobs(prefix=year))
    return list(sorted(filter(lambda f: f not in done_log_filenames, filenames_in_year)))


def save_statistic_entries(client: InfluxDBClient, entries: List[dict]) -> bool:
    try:
        return client.write_points(entries)
    except InfluxDBClientError as e:
        if 'timeout' in e.content:
            logging.warning('Timeout while writing to influxdb. Retrying with smaller batch size...')
            client.write_points(entries, batch_size=MAX_DB_ENTRIES_PER_RPC / 5)
        else:
            logging.exception('Failed to write data to influxdb')
            raise


def process_logs(download_directory: str, influxdb_client: InfluxDBClient, cloudstorage_bucket: Bucket,
                 bucket_path: str):
    """
    Processes a log file its contents.
    Downloads the file if it's not already on disk.
    """
    line_number = 0
    to_save = []  # type: List[dict]

    blob = Blob(bucket_path, cloudstorage_bucket)
    disk_path = os.path.join(download_directory, cloudstorage_bucket.name, bucket_path)
    create_folder(os.path.dirname(disk_path))
    if not os.path.exists(disk_path):
        logging.info('Downloading %s', bucket_path)
        blob.download_to_filename(disk_path)
    logging.debug('Processing logs in file %s/%s', cloudstorage_bucket.name, bucket_path)
    with open(disk_path) as file_obj:
        logging.info('Processing %s', bucket_path)
        file_obj.seek(0)
        for line in file_obj:
            line_number += 1
            if line_number % 10000 == 0:
                logging.info('Processing line %s of %s', line_number, bucket_path)
            try:
                to_save.extend(analyze(line))
            except Exception:
                logging.exception('Could not process line %s', line)
            if len(to_save) > MAX_DB_ENTRIES_PER_RPC:
                save_statistic_entries(influxdb_client, to_save[:MAX_DB_ENTRIES_PER_RPC])
                to_save = to_save[MAX_DB_ENTRIES_PER_RPC:]
        if to_save:
            save_statistic_entries(influxdb_client, to_save)
    os.remove(disk_path)
