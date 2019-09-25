#
# Copyright (c) 2019, Motion Workshop
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# 1. Redistributions of source code must retain the above copyright notice,
#    this list of conditions and the following disclaimer.
#
# 2. Redistributions in binary form must reproduce the above copyright notice,
#    this list of conditions and the following disclaimer in the documentation
#    and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS 'AS IS'
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.
#
import shadow.fileio

from datetime import datetime
import fastavro
import logging


def data_node_generator(fields, data, timestamp):
    """
    Generator function that emits one data_node as per our Avro schema.
    Iterates over the frame data internally. Computes the Avro int timestamp
    for each frame.

    The fields parameter is an array of structs that lists all of the channels
    (aka fields) in the Avro schema. The fields are aligned with the columns of
    the data matrix.

    The data parameter is a 2D numpy.array. Each row is one sample in time.
    Each column is one channel of data in floating point format.

    The timestamp parameter is a (base, offset) pair. Set the 0th field to an
    int formatted value.
    """
    # For each sample in time.
    for i in range(data.shape[0]):
        data_node = {}
        data_node['timestamp'] = int(timestamp[0] + i * timestamp[1])

        # For each channel in this sample.
        for j in range(data.shape[1]):
            data_node[fields[j]] = data[i, j]

        yield data_node


def write_avro(f, fields, info, node_map, data):
    #
    # Populate an Avro schema for this take. One row represents all of the
    # measurement and tracking data for all nodes.
    #
    schema = {
        'namespace': 'shadowmocap',
        'type': 'record',
        'name': 'data_node',
        'fields': [{
            'name': 'timestamp',
            'type': {
                'type': 'long',
                'logicalType': 'timestamp-micros'
            }
        }] + [{'name': item, 'type': 'float'} for item in fields]
    }

    schema = fastavro.parse_schema(schema)

    # Parse the string timestamp into a datetime object. We will use this to
    # add microsecond precision timestamps to each data_node object.
    timestamp = datetime.strptime(
        info.get('timestamp'), '%Y-%m-%d %H:%M:%S.%f')

    # Timestep in seconds.
    h = info.get('h', 0.01)

    # Convert datetime to Avro timestamp in integer microseconds since epoch.
    # Pair the start time with a per frame offset in microseconds.
    timestamp = (
        int((timestamp - datetime(1970, 1, 1)).total_seconds() * 1e6),
        int(h * 1e6))

    fastavro.writer(f, schema, data_node_generator(fields, data, timestamp))
