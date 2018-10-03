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
from typing import Iterator, Dict, Any
from urllib.parse import urlparse

properties = ['app_id', 'end_time', 'host', 'ip', 'latency', 'status', 'start_time', 'mcycles', 'resource',
              'response_size', 'user_agent', 'task_queue_name', 'task_name', 'pending_time']


def process(value: dict) -> Iterator[Dict[str, Any]]:
    request_info = value['data']
    tags = {
        'project': request_info.get('app_id'),  # e.g. e~rogerthat-server,
        'status': request_info['status'],  # 200, 204, 500, ...
    }
    fields = {
        'host': request_info['host'],  # e.g. version-xxx.rogerthat-server.appspot.com
        'resource': urlparse(request_info['resource']).path,  # strip query parameters
        'ip': request_info['ip'],
        'user_agent': request_info['user_agent'],
        'latency': int(request_info['latency']),
        'status': int(request_info['status']),
        'mcycles': int(request_info['mcycles']),
        'pending_time': float(request_info['pending_time']),
        'response_size': request_info['response_size'],
        'task_retry_count': int(request_info.get('task_retry_count', 0))
    }
    if request_info.get('task_name'):
        tags['task_queue_name'] = request_info['task_queue_name']
        fields['task_name'] = request_info['task_name']
    yield {
        'measurement': 'request-info',
        'tags': tags,
        'time': datetime.utcfromtimestamp(request_info['start_time']).isoformat() + 'Z',
        'fields': fields
    }


def process_request_log(request_log: dict) -> Iterator[Dict[str, Any]]:
    request_info = {prop: request_log.get(prop) for prop in properties}
    if request_info['task_name'] and request_log['protoPayload'].get('line', []):
        log = request_log['protoPayload']['line'][0]
        if 'X-Appengine-Taskretrycount' in log['message']:
            headers = dict([tuple(header.split(':')) for header in log['message'].split(', ')])
            request_info['task_retry_count'] = int(headers['X-Appengine-Taskretrycount'])
    return process({'data': request_info})
