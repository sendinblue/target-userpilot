#!/usr/bin/env python3

import argparse
import io
import os
import sys
import json
import threading
import http.client
import urllib
from datetime import datetime
import collections
import requests

import pkg_resources
from jsonschema.validators import Draft4Validator
import singer

REQUIRED_CONFIG_KEYS = ["api_key", "date_from"]

logger = singer.get_logger()

def emit_state(state):
    if state is not None:
        line = json.dumps(state)
        logger.debug('Emitting state {}'.format(line))
        sys.stdout.write("{}\n".format(line))
        sys.stdout.flush()

def flatten(d, parent_key='', sep='__'):
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, collections.MutableMapping):
            items.extend(flatten(v, new_key, sep=sep).items())
        else:
            items.append((new_key, str(v) if type(v) is list else v))
    return dict(items)
        
def persist_lines(config, lines):
    state = None
    schemas = {}
    key_properties = {}
    headers = {}
    validators = {}
    
    now = datetime.now().strftime('%Y%m%dT%H%M%S')

    # Loop over lines from stdin
    for line in lines:
        try:
            o = json.loads(line)
        except json.decoder.JSONDecodeError:
            logger.error("Unable to parse:\n{}".format(line))
            raise

        if 'type' not in o:
            raise Exception("Line is missing required key 'type': {}".format(line))
        t = o['type']

        if t == 'RECORD':
            if 'stream' not in o:
                raise Exception("Line is missing required key 'stream': {}".format(line))
            if o['stream'] not in schemas:
                raise Exception("A record for stream {} was encountered before a corresponding schema".format(o['stream']))

            # Get schema for this record's stream
            schema = schemas[o['stream']]

            # Validate record
            validators[o['stream']].validate(o['record'])

            # If the record needs to be flattened, uncomment this line
            # flattened_record = flatten(o['record'])
            
            # TODO: Process Record message here..

                        
            headers = {
                # Already added when you pass json= but not when you pass data=
                # 'Content-Type': 'application/json',
                'Authorization': f'Token {config["api_key"]}',
                'X-API-Version': '2020-09-22',
            }

            json_data = {
                'user_id': f"{o['record']['organization_id']}",
                'metadata': {
                    'name': f"{o['record']['first_name']} {o['record']['last_name']}",
                    'first_name': f"{o['record']['first_name']}",
                    'last_name': f"{o['record']['last_name']}",
                    'orga_creation_date': f"{o['record']['orga_creation_date']}",
                    'is_email_campaign_validated': f"{o['record']['is_email_campaign_validated']}",
                    'is_transactional_email_validated': f"{o['record']['is_transactional_email_validated']}",
                    'is_profile_completed': f"{o['record']['is_profile_completed']}",
                    'enabled_internal_apps': f"{o['record']['enabled_internal_apps']}",
                    'is_bot': f"{o['record']['is_bot']}",
                    'is_receive_newsletter': f"{o['record']['is_receive_newsletter']}"
                },
            }

            response = requests.post('https://analytex-eu.userpilot.io/v1/identify', headers=headers, json=json_data)
            # response.json()

            # Note: json_data will not be serialized by requests
            # exactly as it was in the original request.
            #data = '{"user_id": "2370546",                     "metadata": {"name": "removing null", "email": "sendinblue.curriculum@iboux.com", "is_email_campaign_validated":"false"}}'
            #response = requests.post('https://analytex-eu.userpilot.io/v1/identify', headers=headers, data=data)

            state = None
        elif t == 'STATE':
            logger.debug('Setting state to {}'.format(o['value']))
            state = o['value']
        elif t == 'SCHEMA':
            if 'stream' not in o:
                raise Exception("Line is missing required key 'stream': {}".format(line))
            stream = o['stream']
            schemas[stream] = o['schema']
            validators[stream] = Draft4Validator(o['schema'])
            if 'key_properties' not in o:
                raise Exception("key_properties field is required")
            key_properties[stream] = o['key_properties']
        else:
            raise Exception("Unknown message type {} in message {}"
                            .format(o['type'], o))
    
    return state


def send_usage_stats():
    try:
        version = pkg_resources.get_distribution('target-csv').version
        conn = http.client.HTTPConnection('collector.singer.io', timeout=10)
        conn.connect()
        params = {
            'e': 'se',
            'aid': 'singer',
            'se_ca': 'target-userpilot',
            'se_ac': 'open',
            'se_la': version,
        }
        conn.request('GET', '/i?' + urllib.parse.urlencode(params))
        response = conn.getresponse()
        conn.close()
    except:
        logger.debug('Collection request failed')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', help='Config file')
    args = parser.parse_args()

    if args.config:
        with open(args.config) as input:
            config = json.load(input)
    else:
        config = {}

    if not config.get('disable_collection', False):
        logger.info('Sending version information to singer.io. ' +
                    'To disable sending anonymous usage data, set ' +
                    'the config parameter "disable_collection" to true')
        threading.Thread(target=send_usage_stats).start()

    input = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')
    state = persist_lines(config, input)
        
    emit_state(state)
    logger.debug("Exiting normally")


if __name__ == '__main__':
    main()
