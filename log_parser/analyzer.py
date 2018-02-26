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
from typing import Any, Iterator
import logging
import ijson

from log_parser.parsers import request_log, rogerthat
from log_parser.parsers.filter import registry, request_filter

OTHER = object()

request_calls = []

log_types = {
    '_request': request_log.process,
    'callback_api': rogerthat.callback_api,
    'api': rogerthat.api,
    'app': rogerthat.app
}


@request_filter('')
def process_log(value: dict) -> Iterator[Any]:
    type_ = value.get('type')
    f = log_types.get(type_)
    return f and f(value)


def analyze(line: str) -> Iterator[Any]:
    readers = {}
    f = io.StringIO(line)
    try:
        for key, type_, value in ijson.parse(f):
            listeners = registry.get(key, OTHER)
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
            if listeners != OTHER:
                for func in listeners:
                    if type_ == 'start_map':
                        # Start reading
                        data = {}
                        readers[func] = [data, [data], None]
                    elif type_ == 'end_map':
                        yield func(readers.pop(func)[0])
    except (ijson.common.IncompleteJSONError, ijson.backends.python.UnexpectedSymbol):
        pass
    finally:
        f.close()
    for func, reader in readers.items():
        yield func(reader[0])
