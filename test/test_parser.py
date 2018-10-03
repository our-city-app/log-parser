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
import os
import unittest
from typing import List

from log_parser.analyzer import analyze
from log_parser.parsers import oca


def _analyze(line: str) -> List[dict]:
    return [r for r in analyze(line)]


DATA_DIR = os.path.join(os.path.dirname(__file__), 'data')


def get_file_content(filename: str) -> str:
    with open(os.path.join(DATA_DIR, filename), 'r') as f:
        return f.read()


class ParserTest(unittest.TestCase):

    def check_length(self, filename, length):
        line = get_file_content(filename)
        result = _analyze(line)
        self.assertEquals(length, len(result))
        return result

    def test_sandwich_callback(self):
        self.check_length('sandwich-callback.json', 1)

    def test_sandwich_result(self):
        self.check_length('sandwich-fmr.json', 1)

    def test_request(self):
        self.check_length('request-log.json', 1)

    def test_empty_app_log(self):
        self.check_length('empty-app-log.json', 0)

    def test_app_log(self):
        self.check_length('app-log.json', 3)

    def test_truncated_log(self):
        self.check_length('truncated.json', 2)

    def test_callback_api(self):
        self.check_length('callback-api.json', 1)

    def test_callback_api_loyalty(self):
        result = self.check_length('callback-api-loyalty.json', 1)
        self.assertEqual(result[0]['tags']['method'], 'solutions.loyalty.load')
        self.assertEqual(result[0]['tags']['function'], 'system.api_call')

    def test_callback_garbage_tag(self):
        self.check_length('callback-api-bad-tag.json', 1)

    def test_api(self):
        self.check_length('api.json', 1)

    def test_without_type(self):
        self.check_length('without-type.json', 1)

    def test_created_apps(self):
        result = self.check_length('created-apps.json', 5)
        self.assertDictEqual({'country': 'BE', 'type': 'Enterprise'}, result[0]['tags'])

    def test_total_users(self):
        result = self.check_length('total-users.json', 31)
        self.assertDictEqual({'app': 'em-be-mobietrain-demo2'}, result[0]['tags'])

    def test_total_services(self):
        result = self.check_length('total-services.json', 17)
        self.assertDictEqual({'type': 'Merchant', 'app': 'be-berlare'}, result[0]['tags'])
        self.assertEqual(6, result[0]['fields']['amount'])

    def test_active_modules(self):
        result = self.check_length('active-modules.json', 31)
        self.assertDictEqual({'app': 'be-berlare', 'module': 'static_content'}, result[0]['tags'])
        self.assertEqual(5, result[0]['fields']['amount'])
        self.assertEqual('qr_codes', result[-1]['tags']['module'])
        self.assertEqual(101, result[-1]['fields']['amount'])

    def test_ignore_channel(self):
        self.check_length('channel.json', 0)

    def test_oca_loyalty(self):
        result = self.check_length('oca-custom-loyalty-cards.json', 5)
        self.assertEqual(result[0]['fields']['amount'], 958)
        self.assertEqual(result[0]['tags']['app'], 'be-destelbergen')
        self.assertEqual(result[0]['measurement'], oca.Measurements.CUSTOM_LOYALTY_CARDS)

    def test_request_log(self):
        result = self.check_length('full-request-log.json', 2)
        self.assertEqual(result[0]['fields'], {'host': 'rogerthat-server.appspot.com',
                                               'ip': '195.130.155.140',
                                               'latency': 0.73131,
                                               'mcycles': 138,
                                               'resource': '/json-rpc',
                                               'response_size': 291,
                                               'status': 200,
                                               'task_retry_count': 0,
                                               'user_agent': 'be-herentals/2.1.2920 CFNetwork/974.2.1 Darwin/18.0.0'})
        self.assertDictEqual(result[0]['tags'], {'project': 'e~rogerthat-server', 'status': 200})

    def test_task(self):
        result = self.check_length('test-log-task.json', 1)
        self.assertDictEqual(result[0]['fields'], {'host': 'rogerthat-server.appspot.com',
                                                   'ip': '0.1.0.2',
                                                   'latency': 0.723739,
                                                   'mcycles': 21,
                                                   'resource': '/_ah/queue/deferred',
                                                   'response_size': 84,
                                                   'status': 200,
                                                   'task_retry_count': 0,
                                                   'task_name': '3202316446794980958',
                                                   'user_agent': 'AppEngine-Google; (+http://code.google.com/appengine)'})
        self.assertDictEqual(result[0]['tags'],
                             {'project': 'e~rogerthat-server',
                              'task_queue_name': 'fast',
                              'status': 200})
