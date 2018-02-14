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
from framework.to import TO
from mcfw.properties import unicode_property, typed_property


class InfluxConfig(TO):
    host = unicode_property('host')
    db = unicode_property('db')
    username = unicode_property('username')
    password = unicode_property('password')


class LogParserConfig(TO):
    cloudstorage_bucket = unicode_property('cloudstorage_bucket')
    influxdb = typed_property('influxdb', InfluxConfig)
