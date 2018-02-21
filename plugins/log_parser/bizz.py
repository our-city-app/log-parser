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
import time
import types
from datetime import datetime

from framework.plugin_loader import get_config
from google.appengine.ext import ndb
from google.appengine.ext.deferred import deferred
from plugins.log_parser.analyzer import analyze
from plugins.log_parser.models import LogParserSettings, ProcessedLogs, get_log_folder, ProcessedFile
from plugins.log_parser.plugin_consts import NAMESPACE

import cloudstorage
from influxdb import InfluxDBClient
from mcfw.consts import DEBUG

MAX_DB_ENTRIES_PER_RPC = 500
LOG_PARSING_QUEUE = 'log-parsing'


def get_client():
    config = get_config(NAMESPACE)
    return InfluxDBClient(host=config.influxdb.host,
                          port=config.influxdb.port,
                          ssl=not DEBUG,
                          verify_ssl=not DEBUG,
                          database=config.influxdb.db,
                          username=config.influxdb.username,
                          password=config.influxdb.password)


def get_bucket_name():
    return get_config(NAMESPACE).cloudstorage_bucket


def _get_foldername(file_path):
    return file_path.split('/')[-2]


def _get_filename(file_path):
    return file_path.split('/')[-1]


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
    processed_log_filenames = [f.filename for f in processed_logs_model.processed_files if f.done]
    files_to_process = [f.filename for f in cloudstorage.listbucket(folder, delimiter='/')
                        if _get_foldername(f.filename) not in processed_log_filenames]
    for file_path in files_to_process:
        logging.info('Starting task to process %s', file_path)
        deferred.defer(process_logs, file_path, _queue=LOG_PARSING_QUEUE)
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


def save_statistic_entries(client, entries):
    logging.info('Writing %d datapoints to influxdb', len(entries))
    return client.write_points(entries, batch_size=MAX_DB_ENTRIES_PER_RPC)


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
    processed_logs_model = ProcessedLogs.create_key(date).get()  # type: ProcessedLogs
    to_save = []
    start_time = time.time()
    line_number = 0
    start_line_number = 0
    processed_files = filter(lambda f: f.filename == file_path, processed_logs_model.processed_files)
    if processed_files:
        if processed_files[0].done:
            logging.warn('File %s already completely processed, doing nothing', file_path)
            return
        start_line_number = processed_files[0].line_number
    client = get_client()
    max_processing_time = 9 * 60
    with cloudstorage.open(file_path, read_buffer_size=1024 * 1024 * 8) as f:
        while True:
            if time.time() - start_time > max_processing_time:
                if to_save:
                    save_statistic_entries(client, to_save)
                _defer_process_logs(date, file_path, line_number)
                return
            line = f.readline()
            if not line:
                if to_save:
                    save_statistic_entries(client, to_save)
                deferred.defer(save_processed_file, date, _get_filename(file_path), line_number, done=True)
                break
            line_number += 1
            if line_number < start_line_number:
                continue
            if line_number % 1000 == 0:
                logging.info('Processing line %s', line_number)
            to_save.extend(flatten(analyze(line)))
            if len(to_save) > MAX_DB_ENTRIES_PER_RPC:
                save_statistic_entries(client, to_save[:MAX_DB_ENTRIES_PER_RPC])
                to_save = to_save[MAX_DB_ENTRIES_PER_RPC:]


@ndb.transactional()
def _defer_process_logs(date, file_path, line_number):
    save_processed_file(date, file_path, line_number, False)
    deferred.defer(process_logs, file_path, _transactional=True, _queue=LOG_PARSING_QUEUE)


@ndb.transactional()
def save_processed_file(date, filename, line_number, done):
    processed_logs_model = ProcessedLogs.create_key(date).get()
    files = filter(lambda f: f.filename == filename, processed_logs_model.processed_files)
    if files:
        processed_file = files[0]
        processed_logs_model.processed_files.pop(processed_logs_model.processed_files.index(processed_file))
    else:
        processed_file = ProcessedFile()
    processed_file.populate(filename=filename, line_number=line_number, done=done)
    processed_logs_model.processed_files.append(processed_file)
    processed_logs_model.put()
