#!/usr/bin/env python

import shlex
import signalfx
import subprocess
import time

cmd = '/opt/kafka/bin/kafka-consumer-groups.sh --group benchmark-group --describe --bootstrap-server localhost:9092'
args = shlex.split(cmd)

# number of columns in the output of the above comment to simplify parsing required information from the output
num_columns = 8

# valid headers
headers = ['TOPIC', 'PARTITION', 'CURRENT-OFFSET', 'LOG-END-OFFSET', 'LAG', 'CONSUMER-ID', 'HOST', 'CLIENT-ID']

# meaningful dimensions
dimensions = set(['TOPIC', 'PARTITION', 'LAG', 'CONSUMER-ID', 'HOST', 'CLIENT-ID'])

consumer_group = 'benchmark-group'
ingest_token = 'MjZnxbLVBuoFhLF7C1KVYw'
log = True
bootstrap_server = '34.220.78.197'

sfx = signalfx.SignalFx().ingest(ingest_token)

while True:
    out = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True)

    std_out, st_derr = out.communicate()
    lines = std_out.split('\n')

    # for a roughly 10s reporting interval
    time.sleep(9)
    datapoints = []

    for line in lines:
        parsed_line = line.split()
        num_columns_in_line = len(parsed_line)
        print parsed_line
        try:
            if num_columns_in_line == num_columns:
                if parsed_line == headers:
                    continue
                topic = parsed_line[0]
                partition = parsed_line[1]
                consumer_id = parsed_line[5]
                host = parsed_line[6]
                client_id = parsed_line[7]
                if host == '-' or client_id == '-':
                    print
                    'Skipping entry. Consumer might be down.'
                    continue
                try:
                    value = int(parsed_line[4])
                except ValueError:
                    print
                    parsed_line
                    pass
                dimensions = {
                    'topic': topic,
                    'partition': partition,
                    'consumer_id': consumer_id,
                    'host': host,
                    'client_id': client_id,
                    'path': 'kafka_consumer',
                }

                datapoint = {
                    'metric': 'kafka_consumer_lag',
                    'value': value,
                    'dimensions': dimensions,
                }
                datapoints.append(datapoint)
            elif num_columns_in_line == num_columns - 1:
                try:
                    topic = parsed_line[0]
                    partition = parsed_line[1]
                    temp = parsed_line[5].split('/')
                    consumer_id = temp[0]
                    host = temp[1]
                    client_id = parsed_line[6]

                    if host == '-' or client_id == '-':
                        print
                        'Skipping entry. Consumer might be down.'
                        continue
                    try:
                        value = int(parsed_line[4])
                    except ValueError:
                        print
                        parsed_line
                        pass
                    dimensions = {
                        'topic': topic,
                        'partition': partition,
                        'consumer_id': consumer_id,
                        'host': host,
                        'client_id': client_id,
                        'path': 'kafka_consumer',
                    }

                    datapoint = {
                        'metric': 'kafka_consumer_lag',
                        'value': value,
                        'dimensions': dimensions,
                    }
                    datapoints.append(datapoint)
                except IndexError:
                    print
                    parsed_line
                    continue
        finally:
            pass
    if log:
        print datapoints
    try:
        sfx.send(gauges=datapoints)
    finally:
        sfx.stop()
