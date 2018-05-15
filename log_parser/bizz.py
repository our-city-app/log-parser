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

from google.cloud.storage import Bucket, Blob
from influxdb import InfluxDBClient
from influxdb.exceptions import InfluxDBClientError

from log_parser.analyzer import analyze
from log_parser.db import DatabaseConnection, create_folder

MAX_DB_ENTRIES_PER_RPC = 2000


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


def start_processing_logs(db: DatabaseConnection, cloudstorage_bucket: Bucket) -> List[str]:
    # Returns all log files for a whole year
    settings = db.get_settings()
    logging.info('Starting processing logs from %s', settings.last_date)
    if not settings.last_date:
        settings.last_date = _get_next_date(cloudstorage_bucket)
        db.save_settings(settings)
    year_str = str(settings.last_date.year)
    files = get_unprocessed_logs(db, cloudstorage_bucket, year_str)
    if not files:
        return []
    min_date = datetime.strptime((files[-1]).split('/')[0], '%Y-%m-%d %H:%M:%S')
    next_date = _get_next_date(cloudstorage_bucket, min_date) or min_date
    if next_date != settings.last_date:
        settings.last_date = next_date
        logging.info('Saving next date as %s', settings.last_date)
        db.save_settings(settings)
    return files


def get_unprocessed_logs(db: DatabaseConnection, cloudstorage_bucket: Bucket, year: str) -> List[str]:
    done_log_filenames = db.get_all_processed_logs(year)
    filenames_in_year = map(lambda b: b.name, cloudstorage_bucket.list_blobs(prefix=year))
    return list(sorted(filter(lambda f: f not in done_log_filenames, filenames_in_year)))


def clean_old_files(db: DatabaseConnection, cloudstorage_bucket: Bucket):
    data_dir = os.path.join(db.root_dir, 'data')
    year = datetime.now().year
    while True:
        if year < 2016:
            break
        year_str = str(year)
        unprocessed_files = get_unprocessed_logs(db, cloudstorage_bucket, year_str)
        if len(unprocessed_files) != 0:
            year -= 1
            logging.info('Not cleaning files for year %d because not all files have been processed yet', year)
            continue
        logging.info('Cleaning old files for year %d', year)
        for folder_name in os.listdir(data_dir):
            if folder_name.startswith(year_str):
                date_dir = os.path.join(data_dir, folder_name)
                logging.debug('Removing all files in folder %s', date_dir)
                for filename in os.listdir(date_dir):
                    file_path = os.path.join(data_dir, folder_name, filename)
                    logging.debug('Removing file %s', file_path)
                    os.remove(file_path)
                os.rmdir(date_dir)
        year -= 1


def save_statistic_entries(client: InfluxDBClient, entries: List[dict]) -> bool:
    logging.info('Writing %d datapoints to influxdb', len(entries))
    try:
        return client.write_points(entries)
    except InfluxDBClientError as e:
        if 'timeout' in e.content:
            logging.warning('Timeout while writing to influxdb. Retrying...')
            client.write_points(entries, batch_size=MAX_DB_ENTRIES_PER_RPC / 5)
        else:
            logging.exception('Failed to write data to influxdb')
            raise


def process_logs(db: DatabaseConnection, influxdb_client: InfluxDBClient, cloudstorage_bucket: Bucket,
                 bucket_path: str):
    """
    Processes a log file its contents.
    Downloads the file if it's not already on disk.
    """
    date = _get_date_from_filename(bucket_path)
    log_folder = get_log_folder(date)
    file_name = _get_filename(bucket_path)
    processed_log = db.get_processed_log(log_folder, file_name)
    line_number = 0
    if processed_log:
        logging.warning('File %s already processed, doing nothing', bucket_path)
        return
    to_save = []  # type: List[dict]

    blob = Blob(bucket_path, cloudstorage_bucket)
    directory, filename = bucket_path.split('/')
    dir_path = os.path.join(db.root_dir, 'data', directory)
    create_folder(dir_path)
    disk_path = os.path.join(dir_path, filename)
    if not os.path.exists(filename):
        logging.info('Downloading %s', bucket_path)
        blob.download_to_filename(disk_path)
    logging.debug('Processing logs in file %s', bucket_path)
    with open(disk_path) as file_obj:
        logging.info('Processing %s', bucket_path)
        file_obj.seek(0)
        for line in file_obj:
            line_number += 1
            if line_number % 5000 == 0:
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
        db.save_processed_file(_get_foldername(bucket_path), _get_filename(bucket_path))
