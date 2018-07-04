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

import io
import logging
from typing import Iterator, Dict, Any, List, Union

import ijson

from log_parser.parsers import request_log, rogerthat, threefold, oca
from log_parser.parsers.filter import registry, request_filter

log_types = {
    '_request': request_log.process,
    'callback_api': rogerthat.callback_api,
    'api': rogerthat.api,
    'app': rogerthat.app,
    'web': rogerthat.web,
    'rogerthat.created_apps': rogerthat.created_apps,
    'rogerthat.released_apps': rogerthat.released_apps,
    'rogerthat.total_users': rogerthat.all_users,
    'rogerthat.total_services': rogerthat.total_services,
    'oca.active_modules': oca.active_modules,
    'oca.custom_loyalty_cards': oca.custom_loyalty_cards,
    'web_channel': rogerthat.web_channel,
    'tf.web': threefold.web
}


@request_filter('')
def process_log(value: dict) -> Iterator[dict]:
    type_ = value.get('type')
    if not type_:
        type_ = guess_log_type(value)
    f = log_types.get(type_)
    if f:
        yield from f(value)
    elif not type_:
        pass
    else:
        logging.warning('Unsupported log type %s for line %s', type_, value)
        yield from ()


def guess_log_type(value: dict) -> Union[None, str]:
    result = None
    request_data = value.get('request_data', {})
    if 'params' in request_data:
        result = 'callback_api'
    elif 'a' in request_data:
        result = 'app'
    return result


def analyze(line: str) -> Iterator[dict]:
    readers: Any = {}  # too complicated for proper types
    f = io.StringIO(line)
    data: Union[Dict, List]
    try:
        for key, type_, value in ijson.parse(f):
            listeners = registry.get(key, [])
            for reader in readers.values():
                _, stack, property_name = reader
                parent = stack[-1]
                if type_ == 'start_map':
                    data = {}
                    stack.append(data)
                    if isinstance(parent, dict):
                        parent[property_name] = data
                    else:
                        parent.append(data)
                    continue
                if type_ in ('end_map', 'end_array'):
                    stack.pop()
                    continue
                if type_ == 'map_key':
                    reader[2] = value
                    continue
                if type_ == 'start_array':
                    data = []
                    stack.append(data)
                    if isinstance(parent, dict):
                        parent[property_name] = data
                    else:
                        parent.append(data)
                    continue
                if isinstance(parent, dict):
                    parent[property_name] = value
                else:
                    parent.append(value)
            for func in listeners:
                if type_ == 'start_map':
                    # Start reading
                    initial_data: Dict = {}
                    readers[func] = [initial_data, [initial_data], None]
                elif type_ == 'end_map':
                    yield from func(readers.pop(func)[0])
    except (ijson.common.IncompleteJSONError, ijson.backends.python.UnexpectedSymbol):
        pass
    finally:
        f.close()
    for func, reader in readers.items():
        for result in func(reader[0]):
            yield result
