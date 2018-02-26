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


class LogParserSettings(object):
    def __init__(self, last_date: datetime) -> None:
        self.last_date = last_date

    def last_date_object(self) -> datetime:
        if not self.last_date:
            return None
        return datetime.strptime(self.last_date, '%Y-%m-%d %H:%M:%S')


class LogFile(object):
    def __init__(self, folder_name: str, file_name: str) -> None:
        self.folder_name = folder_name
        self.file_name = file_name
