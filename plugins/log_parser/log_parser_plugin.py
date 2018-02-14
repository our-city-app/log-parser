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

from config import LogParserConfig
from framework.plugin_loader import Plugin
from framework.utils.plugins import Handler
from handlers import ProcessLogsHandler


class LogParserPlugin(Plugin):
    def __init__(self, configuration):
        super(LogParserPlugin, self).__init__(LogParserConfig.from_dict(configuration))
        assert isinstance(self.configuration, LogParserConfig)

    def get_handlers(self, auth):
        if auth == Handler.AUTH_ADMIN:
            yield Handler('/admin/cron/log_parser/process', ProcessLogsHandler)
