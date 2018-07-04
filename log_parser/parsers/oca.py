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
from typing import Iterator

from log_parser.parsers.rogerthat import _get_time


class Measurements(object):
    ACTIVE_MODULES = 'oca.active_modules'
    CUSTOM_LOYALTY_CARDS = 'oca.custom_loyalty_cards'


def active_modules(value: dict) -> Iterator[dict]:
    for app_id, values in value.get('request_data', {}).items():
        for module, amount in values.items():
            yield {
                'measurement': Measurements.ACTIVE_MODULES,
                'tags': {
                    'module': module,
                    'app': app_id
                },
                'time': _get_time(value),
                'fields': {
                    'amount': amount
                }
            }


def custom_loyalty_cards(value: dict) -> Iterator[dict]:
    for stats in value.get('request_data', []):
        yield {
            'measurement': Measurements.CUSTOM_LOYALTY_CARDS,
            'tags': {
                'country': stats['country'],
                'app': stats['app_id']
            },
            'time': _get_time(value),
            'fields': {
                'amount': stats['amount']
            }
        }
