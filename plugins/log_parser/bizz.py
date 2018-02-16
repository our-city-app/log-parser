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
import types
from datetime import datetime

from google.appengine.ext import ndb
from google.appengine.ext.deferred import deferred
from plugins.log_parser.analyzer import analyze
from plugins.log_parser.models import LogParserSettings, ProcessedLogs, get_log_folder
from plugins.log_parser.plugin_consts import NAMESPACE

import cloudstorage
from framework.plugin_loader import get_config
from influxdb import InfluxDBClient

MAX_DB_ENTRIES_PER_RPC = 500


def get_client():
    config = get_config(NAMESPACE)
    return InfluxDBClient(host=config.influxdb.host, database=config.influxdb.db, username=config.influxdb.username,
                          password=config.influxdb.password)


def get_bucket_name():
    return get_config(NAMESPACE).cloudstorage_bucket


def _get_foldername(file_path):
    return file_path.split('/')[-2]


def _get_date_from_filename(filename):
    return datetime.strptime(_get_foldername(filename), '%Y-%m-%d %H:%M:%S')


def _get_next_date(min_date=None):
    directories = sorted([f.filename for f in cloudstorage.listbucket('/%s' % get_bucket_name(), delimiter='/')])
    if not min_date:
        return _get_date_from_filename(directories[0])
    for directory in directories:
        dir_date = _get_date_from_filename(directory)
        if dir_date > min_date:
            return dir_date


def start_processing_logs():
    config = LogParserSettings.get_config()
    # if DEBUG:
    #     ndb.delete_multi(ProcessedLogs.query().fetch(keys_only=True))
    #     get_client().drop_database('monitoring')
    #     get_client().create_database('monitoring')
    #     config.last_date = None
    if not config.last_date:
        config.last_date = _get_next_date()
        config.put()
    folder = u'/%s/%s/' % (get_bucket_name(), get_log_folder(config.last_date))
    processed_logs_key = ProcessedLogs.create_key(config.last_date)
    processed_logs_model = processed_logs_key.get()
    if not processed_logs_model:
        processed_logs_model = ProcessedLogs(key=processed_logs_key)
        processed_logs_model.put()
    files_to_process = [f.filename for f in cloudstorage.listbucket(folder, delimiter='/')
                        if _get_foldername(f.filename) not in processed_logs_model.processed_files]
    for file_path in files_to_process:
        logging.info('Starting task to process %s', file_path)
        deferred.defer(process_logs, file_path)
    now = datetime.now()
    current_hour_date = datetime(year=now.year, month=now.month, day=now.day, hour=now.hour)
    if current_hour_date > config.last_date:
        next_date = _get_next_date(config.last_date)
        if not next_date:
            logging.info('No new logs to process yet.')
        elif next_date != config.last_date:
            logging.info('Setting next date for log parsing from %s to %s', config.last_date, next_date)
            config.last_date = next_date
            config.put()


def save_statistic_entries(entries):
    logging.info('Writing %d datapoints to influxdb', len(entries))
    return get_client().write_points(entries, batch_size=MAX_DB_ENTRIES_PER_RPC)


def flatten(l):
    for sublist in l:
        if sublist:
            if isinstance(sublist, types.GeneratorType):
                for item in sublist:
                    if item:
                        yield item
            else:
                yield sublist


def process_logs(file_path):
    date = _get_date_from_filename(file_path)
    db_entries = []
    with cloudstorage.open(file_path, read_buffer_size=1024 * 1024 * 8) as f:
        while True:
            line = f.readline()
            if line:
                db_entries.extend(flatten(analyze(line)))
                if len(db_entries) > MAX_DB_ENTRIES_PER_RPC:
                    save_statistic_entries(db_entries[:MAX_DB_ENTRIES_PER_RPC])
                    db_entries = db_entries[MAX_DB_ENTRIES_PER_RPC:]
            else:
                break
    if db_entries:
        save_statistic_entries(db_entries)
    deferred.defer(save_processed_file, date, file_path)


@ndb.transactional()
def save_processed_file(date, filename):
    processed_logs_model = ProcessedLogs.create_key(date).get()
    processed_logs_model.processed_files.append(_get_foldername(filename))
    processed_logs_model.put()
