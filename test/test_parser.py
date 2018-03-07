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


def _analyze(line: str) -> List[dict]:
    return [r for r in analyze(line)]


DATA_DIR = os.path.join(os.path.dirname(__file__), 'data')


def get_file_content(filename: str) -> str:
    with open(os.path.join(DATA_DIR, filename), 'r') as f:
        return f.read()


class ParserTest(unittest.TestCase):

    def test_sandwich_callback(self):
        line = get_file_content('sandwich-callback.json')
        self.assertEquals(1, len(_analyze(line)))

    def test_sandwich_result(self):
        line = get_file_content('sandwich-fmr.json')
        self.assertEquals(2, len(_analyze(line)))

    def test_request(self):
        line = get_file_content('request-log.json')
        self.assertEquals(1, len(_analyze(line)))

    def test_empty_app_log(self):
        line = get_file_content('empty-app-log.json')
        self.assertEquals(0, len(_analyze(line)))

    def test_app_log(self):
        line = get_file_content('app-log.json')
        self.assertEquals(3, len(_analyze(line)))

    def test_truncated_log(self):
        line = get_file_content('truncated.json')
        self.assertEqual(2, len(_analyze(line)))

    def test_callback_api(self):
        line = get_file_content('callback-api.json')
        self.assertEqual(1, len(_analyze(line)))

    def test_callback_garbage_tag(self):
        line = get_file_content('callback-api-bad-tag.json')
        self.assertEqual(1, len(_analyze(line)))

    def test_api(self):
        line = get_file_content('api.json')
        self.assertEqual(1, len(_analyze(line)))

    def test_without_type(self):
        line = get_file_content('without-type.json')
        self.assertEqual(1, len(_analyze(line)))
