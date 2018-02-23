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
from typing import Any, Iterator
from urllib.parse import urlparse

def process(value: dict) -> Iterator[Any]:
    request_info = value['data']
    tags = {
        'project': request_info.get('app_id'),  # e.g. e~rogerthat-server,
        'host': request_info['host'],
        'ip': request_info['ip'],
        'resource': urlparse(request_info['resource']).path,  # strip query parameters
        'status': request_info['status'],
        'user_agent': request_info['user_agent'],
    }
    if request_info.get('task_name'):
        tags['task_name'] = request_info['task_name']
        tags['task_queue_name'] = request_info['task_queue_name']
    return {
        'measurement': 'request-info',
        'tags': tags,
        'time': datetime.utcfromtimestamp(request_info['start_time']).isoformat() + 'Z',
        'fields': {
            'latency': int(request_info['latency']),
            'status': int(request_info['status']),
            'mcycles': int(request_info['mcycles']),
            'pending_time': float(request_info['pending_time']),
            'response_size': request_info['response_size'],
            'task_retry_count': int(request_info.get('task_retry_count', 0))
        }
    }
