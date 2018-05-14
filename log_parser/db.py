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

import json
import os
import typing
from datetime import datetime

from log_parser.models import LogParserSettings, LogFile


def touch(path):
    with open(path, 'a'):
        os.utime(path, None)


def create_folder(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)


# todo: use a database (mysql or something)
class DatabaseConnection(object):
    root_dir = None

    def __init__(self, root_dir) -> None:
        self.root_dir = os.path.realpath(root_dir)

    def get_settings(self) -> LogParserSettings:
        f_path = os.path.join(self.root_dir, 'settings.json')
        if not os.path.exists(f_path):
            return LogParserSettings(None)
        with open(f_path, 'r') as f:
            settings = LogParserSettings(datetime.strptime(json.load(f)['last_date'], '%Y-%m-%dT%H:%M:%S'))
        return settings

    def save_settings(self, settings: LogParserSettings) -> LogParserSettings:
        f_path = os.path.join(self.root_dir, 'settings.json')
        with open(f_path, 'w') as f:
            f.write(json.dumps({'last_date': settings.last_date.isoformat()}))
        return settings

    def get_all_processed_logs(self, year: str) -> typing.List[str]:
        all_dirs = []
        year_folder_path = os.path.join(self.root_dir, year)
        if not os.path.exists(year_folder_path):
            return []
        for directory in os.listdir(year_folder_path):
            all_dirs.extend(['%s/%s' % (directory, filename) for filename in
                             os.listdir(os.path.join(self.root_dir, year, directory))])
        return all_dirs

    def get_processed_logs(self, log_folder: str) -> typing.List[str]:
        """Returns a list of filenames"""
        year = log_folder.split('-')[0]
        create_folder(os.path.join(self.root_dir, year))
        f_path = os.path.join(self.root_dir, year, log_folder)
        if not os.path.exists(f_path):
            return []
        return os.listdir(f_path)

    def save_processed_file(self, log_folder: str, file_name: str) -> None:
        year = log_folder.split('-')[0]
        create_folder(os.path.join(self.root_dir, year, log_folder))
        f_path = os.path.join(self.root_dir, year, log_folder, file_name)
        touch(f_path)

    def get_processed_log(self, log_folder: str, file_name: str) -> typing.Union[LogFile, None]:
        year = log_folder.split('-')[0]
        create_folder(os.path.join(self.root_dir, year))
        f_path = os.path.join(self.root_dir, year, log_folder, file_name)
        if not os.path.exists(f_path):
            return None
        return LogFile(log_folder, file_name)
