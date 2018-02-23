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
import json
import re
import urllib
from datetime import datetime
from functools import lru_cache
from typing import Union, Iterator, Any

from google.appengine.api import urlfetch

HUMAN_READABLE_TAG_REGEX = re.compile('(.*?)\\s*\\{.*\\}')


class Measurements(object):
    FLOW_MEMBER_RESULTS = 'rogerthat.flow_member_result'
    CALLBACK_API = 'rogerthat.callback_api'
    API_CALLS = 'rogerthat.api_calls'
    MESSAGES = 'rogerthat.messages'


def _get_time(value: dict) -> str:
    return datetime.utcfromtimestamp(value['timestamp']).isoformat() + 'Z'


def parse_to_human_readable_tag(tag: str) -> Union[str, None]:
    if not tag:
        return None

    if tag.startswith('{') and tag.endswith('}'):
        try:
            tag_dict = json.loads(tag)
        except:
            return tag
        return tag_dict.get('__rt__.tag', tag)

    m = HUMAN_READABLE_TAG_REGEX.match(tag)
    if m:
        return m.group(1)

    return tag


def callback_api(value: dict) -> Iterator[Any]:
    request_data = value.get('request_data', {})
    function_type = value.get('function') or request_data.get('method')
    timestamp = _get_time(value)
    params = request_data.get('params', {})
    user_email = 'unknown'
    app_id = 'unknown'
    tag = parse_to_human_readable_tag(params.get('tag'))
    if params.get('user_details'):
        if isinstance(params['user_details'], list):
            user_details = params['user_details'][0]
        else:
            user_details = params['user_details']
        app_id = user_details['app_id']
        user_email = user_details['email']
    if tag and not tag.startswith('{'):
        if function_type == 'messaging.flow_member_result':
            fields = {
                'parent_message_key': params.get('parent_message_key')
            }
            if params.get('steps'):
                fields['last_step_id'] = params['steps'][-1]['step_id']
            yield {
                'measurement': Measurements.FLOW_MEMBER_RESULTS,
                'tags': {
                    'method': request_data.get('method'),
                    'tag': tag,
                    'flush_id': params.get('flush_id'),
                    'end_id': params.get('end_id'),
                    'app': app_id,
                    'service': value.get('user')
                },
                'time': timestamp,
                'fields': fields
            }
        else:
            yield {
                'measurement': Measurements.CALLBACK_API,
                'tags': {
                    'tag': parse_to_human_readable_tag(params.get('tag')),
                    'app': app_id,
                    'method': params.get('method'),
                    'service': value.get('user')
                },
                'time': timestamp,
                'fields': {
                    'user': user_email
                }
            }


def app(value: dict) -> Iterator[Any]:
    # {
    #   "timestamp": 1518603982,
    #   "request_data": {
    #     "a": [
    #       "68443b87-e849-4406-b52f-d0413a433445"
    #     ],
    #     "c": [],
    #     "r": [
    #       {
    #         "s": "success",
    #         "r": {
    #           "received_timestamp": 1518603981
    #         },
    #         "av": 1,
    #         "ci": "7ff631c7-1171-11e8-972f-316b0a392f23",
    #         "t": 1518603981
    #       }
    #     ],
    #     "av": 1
    #   },
    #   "type": "app",
    #   "response_data": {
    #     "a": [
    #       "7ff631c7-1171-11e8-972f-316b0a392f23"
    #     ],
    #     "ap": "https://rogerthat-server.appspot.com/json-rpc",
    #     "av": 1,
    #     "t": 1518603982,
    #     "more": false
    #   },
    #   "user": "c356f0adc203397a9d89ff9e1a6e6b54:em-be-idola"
    # }
    request_data = value.get('request_data', {})
    user = value.get('user', 'unknown')
    if ':' in user:
        user, app = user.split(':', 1)
    elif user:
        app = 'rogerthat'
    else:
        app = 'unknown'
    for r in request_data.get('r', []):
        if r.get('item', {}).get('r'):
            if r['item']['r'].keys == ['received_timestamp']:
                yield {
                    'measurement': Measurements.MESSAGES,
                    'tags': {
                        'app': app,
                    },
                    'time': _get_time(value),
                    'fields': {
                        'user': user
                    }
                }


def api(value: dict) -> Iterator[Any]:
    # value = {
    #     u'function': u'system.get_identity',
    #     u'success': True,
    #     u'user': u'5c31adac01cad92a435c44f514798d88',
    #     u'type': u'api',
    #     u'request_data': {},
    #     u'response_data': {...},
    #     u'timestamp': 1518583750
    # }
    tags = {
        'method': value.get('function'),
    }
    fields = {
        'success': value.get('success')
    }

    if 'user' in value:
        tags['app_id'] = _get_app_id_by_service_hash(value['user'])
        fields['service'] = value['user']
    yield {
        'measurement': Measurements.API_CALLS,
        'tags': tags,
        'time': _get_time(value),
        'fields': fields
    }


@lru_cache(maxsize=1000)
def _get_app_id_by_service_hash(service_hash: str) -> Union[str, None]:
    # TODO refactor to not use urlfetch
    qry_string = urllib.urlencode({'user': service_hash})
    res = urlfetch.fetch('https://rogerth.at/unauthenticated/service-app?' + qry_string)
    if res.status_code != 200:
        raise Exception('Failed to get app_id for service hash %s', service_hash)
    return json.loads(res.content)['app_id']
