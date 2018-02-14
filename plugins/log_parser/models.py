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
from datetime import datetime

from google.appengine.ext import ndb
from plugins.log_parser.plugin_consts import NAMESPACE

from framework.models.common import NdbModel


def get_log_folder(date):
    # type: (datetime) -> unicode
    return u'%04d-%02d-%02d %02d:00:00' % (date.year, date.month, date.day, date.hour)


class LogParserSettings(NdbModel):
    NAMESPACE = NAMESPACE
    last_date = ndb.DateTimeProperty()

    @classmethod
    def get_config(cls):
        return cls.get_or_insert('settings')


class ProcessedLogs(NdbModel):
    NAMESPACE = NAMESPACE
    processed_files = ndb.StringProperty(repeated=True, indexed=False)  # List of filenames

    def folder_name(self):
        return self.key.id().decode('utf-8')

    @classmethod
    def create_key(cls, date):
        # type: (datetime) -> ndb.Key
        # e.g. 2018-02-02 01
        return ndb.Key(cls, get_log_folder(date), namespace=cls.NAMESPACE)
