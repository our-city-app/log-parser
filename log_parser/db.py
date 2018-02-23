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

import sqlite3
import typing

from log_parser.models import LogParserSettings, LogFile


class DatabaseConnection(object):

    def __init__(self, database_file: str) -> None:
        self.connection = sqlite3.connect(database_file)
        self.connection.execute('CREATE TABLE IF NOT EXISTS settings (id INTEGER PRIMARY KEY, last_date DATETIME)')
        self.connection.execute('CREATE TABLE IF NOT EXISTS log_files (folder_name TEXT, file_name TEXT)')
        self.connection.execute('INSERT OR IGNORE INTO settings(id, last_date) VALUES(1, null);')
        self.connection.commit()

    def execute(self, query):
        self.connection.execute(query)
        self.connection.commit()

    def __del__(self):
        self.connection and self.connection.close()

    def get_settings(self) -> LogParserSettings:
        result = self.connection.execute('SELECT last_date from settings WHERE id = 1').fetchone()
        return LogParserSettings(*result)

    def save_settings(self, settings: LogParserSettings) -> LogParserSettings:
        self.connection.execute('UPDATE OR ROLLBACK settings SET last_date = ? WHERE id = 1', settings.last_date)
        self.connection.commit()
        return settings

    def get_processed_logs(self, log_folder: str) -> typing.List[LogFile]:
        results = self.connection.execute('SELECT name, file_name FROM log_files WHERE folder_name = ?',
                                          log_folder).fetchall()
        return [LogFile(*result) for result in results]

    def save_processed_file(self, folder_name: str, file_name: str) -> None:
        self.connection.execute('INSERT OR IGNORE INTO log_files (folder_name, file_name) VALUES (?, ?)', folder_name,
                                file_name)
        self.connection.commit()

    def get_processed_log(self, log_folder: str, file_name: str):
        result = self.connection.execute(
            'SELECT name, file_name FROM log_files WHERE folder_name = ? AND file_name = ?',
            log_folder, file_name).fetchone()
        return LogFile(*result) if result else None
