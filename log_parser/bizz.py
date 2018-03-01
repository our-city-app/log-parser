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
import tempfile
import types
from datetime import datetime
from typing import Optional, Iterator, List, Union, Dict

from google.cloud.storage import Bucket, Blob
from influxdb import InfluxDBClient

from log_parser.analyzer import analyze
from log_parser.db import DatabaseConnection

MAX_DB_ENTRIES_PER_RPC = 5000
LOG_PARSING_QUEUE = 'log-parsing'


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


def start_processing_logs(db: DatabaseConnection, cloudstorage_bucket: Bucket) -> Iterator[str]:
    settings = db.get_settings()
    logging.info('Starting processing logs from %s', settings.last_date)
    if not settings.last_date:
        settings.last_date = _get_next_date(cloudstorage_bucket)
        db.save_settings(settings)
    log_folder = get_log_folder(settings.last_date)
    done_log_filenames = db.get_processed_logs(log_folder)
    gcs_files = cloudstorage_bucket.list_blobs(prefix=log_folder)
    for file_ in gcs_files:
        if _get_filename(file_.name) not in done_log_filenames:
            yield file_.name
    now = datetime.now()
    current_hour_date = datetime(year=now.year, month=now.month, day=now.day, hour=now.hour)
    if current_hour_date > settings.last_date:
        next_date = _get_next_date(cloudstorage_bucket, settings.last_date)
        if not next_date:
            logging.info('No new logs to process yet.')
        elif next_date != settings.last_date:
            logging.info('Setting next date for log parsing from %s to %s', settings.last_date, next_date)
            settings.last_date = next_date
            db.save_settings(settings)


def save_statistic_entries(client, entries) -> bool:
    logging.info('Writing %d datapoints to influxdb', len(entries))
    return client.write_points(entries, batch_size=MAX_DB_ENTRIES_PER_RPC)


def flatten(l: Union[Iterator[Dict], Iterator[Iterator[Dict]]]) -> Iterator[Dict]:
    for sublist in l:
        if sublist:
            if isinstance(sublist, types.GeneratorType):
                for item in sublist:
                    if item:
                        yield item
            elif isinstance(sublist, dict):
                yield sublist


def process_logs(db: DatabaseConnection, influxdb_client: InfluxDBClient, cloudstorage_bucket: Bucket,
                 bucket_path: str):
    date = _get_date_from_filename(bucket_path)
    log_folder = get_log_folder(date)
    file_name = _get_filename(bucket_path)
    processed_log = db.get_processed_log(log_folder, file_name)
    line_number = 0
    if processed_log:
        logging.warning('File %s already processed, doing nothing', bucket_path)
        return
    to_save = []  # type: List[dict]
    logging.info('Downloading %s', bucket_path)
    blob = Blob(bucket_path, cloudstorage_bucket)
    with tempfile.NamedTemporaryFile(delete=True) as file_obj:
        blob.download_to_file(file_obj)
        logging.info('Processing %s', bucket_path)
        file_obj.seek(0)
        for line in file_obj:
            line_number += 1
            if line_number % 1000 == 0:
                logging.info('Processing line %s of %s', line_number, bucket_path)
            to_save.extend(flatten(analyze(line.decode('utf-8'))))
            if len(to_save) > MAX_DB_ENTRIES_PER_RPC:
                save_statistic_entries(influxdb_client, to_save[:MAX_DB_ENTRIES_PER_RPC])
                to_save = to_save[MAX_DB_ENTRIES_PER_RPC:]
        if to_save:
            save_statistic_entries(influxdb_client, to_save)
        db.save_processed_file(_get_foldername(bucket_path), _get_filename(bucket_path))
