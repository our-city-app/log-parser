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


class InfluxConfig(object):
    def __init__(self, config: dict) -> None:
        self.host = config.get('host')  # type: str
        self.port = config.get('port')  # type: int
        self.db = config.get('host')  # type: str
        self.username = config.get('username')  # type: str
        self.password = config.get('password')  # type: str


class LogParserConfig(object):
    def __init__(self, config: dict) -> None:
        self.cloudstorage_bucket = config.get('cloudstorage_bucket')  # type: str
        self.influxdb = InfluxConfig(config.get('influxdb', {}))  # type: InfluxConfig
        self.debug = config.get('debug', False)  # type: bool
        self.interval = config.get('interval', 120)  # type: int
