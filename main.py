#
# Copyright (c) 2021, Motion Workshop
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

import logging
import numpy as np
import os


def load_take(path=None):
    """
    Load take data and the name mapping. Return a 2D numpy.array where each
    row is one sample in time and each column is one measurement channel.
    """
    path = shadow.fileio.find_newest_take(path)

    with open('{}/data.mStream'.format(path), 'rb') as f:
        info, node_list, data = shadow.fileio.read_stream(f)

    #
    # Read the mTake file to get the node string ids. Create a name
    # mapping for all of the nodes and the data fields that are present in the
    # take data stream. For example, create something like:
    #
    #   node_map['Hips']['Gq'] = (0, 4)
    #
    # Which can be used to index into the big array of data loaded from the
    # take.
    #
    with open('{}/take.mTake'.format(path)) as f:
        node_map = shadow.fileio.make_node_map(f, node_list)

    #
    # The take stream data is arranged as a flat 1D array.
    #   [ax0 ay0 az0 ... axN ayN azN]
    # Use the reshape function access as 2D array with rows as time.
    #   [[ax0 ay0 az0],
    #    ...
    #    [axN ayN azN]]
    #
    num_frame = int(info['num_frame'])
    stride = int(info['frame_stride'] / 4)

    data = np.reshape(np.array(data), (num_frame, stride))

    return path, info, node_map, data


def make_field_list(node_map):
    """
    Create a flat list of all channels in the take. Format as an Avro schema.
    For example, Hips.la breaks out into Hips_lax, Hips_lay, Hips_laz. Use
    the underscore as the separator because the dot is reserved for
    namespaces in Avro. Also, prefix raw channels with 'RAW' to avoid
    conflicts with case insensitive field names in database schemas (like
    BigQuery).
    """
    field_list = []
    for name in node_map:
        for channel in node_map[name]:
            item = node_map[name][channel]
            dim = item[1] - item[0]

            # Prefix the raw channels to prevent conflicts with the sensor
            # channels.
            if channel in ['A', 'M', 'G']:
                channel = ''.join(['RAW', channel])

            channel_name = '_'.join([name, channel])

            axis_list = []
            if dim == 4:
                axis_list = ['w', 'x', 'y', 'z']
            elif dim == 3:
                axis_list = ['x', 'y', 'z']
            else:
                axis_list = ['']

            for axis in axis_list:
                field_list.append(''.join([channel_name, axis]))

    return field_list


def main(argv):
    import argparse

    logging.basicConfig(level=logging.INFO, format='%(message)s')

    parser = argparse.ArgumentParser(
        description='Convert a take to an Avro or CSV formatted file')

    parser.add_argument(
        '--quiet', action='store_true', help='suppress console output')
    parser.add_argument(
        '--format', choices=['avro', 'csv'], default='avro',
        help='choose the output file format')
    parser.add_argument(
        '--output', type=str,
        help='output filename, defaults to "data.avro" in take folder')
    parser.add_argument('path', nargs='*')

    args = parser.parse_args(argv)

    if len(args.path) == 0:
        args.path.append(None)

    filename_list = []
    for path in args.path:
        path, info, node_map, data = load_take(path)

        fields = make_field_list(node_map)
        if args.output:
            filename = os.path.normpath(args.output)
        else:
            filename = os.path.join(path, 'data.{}'.format(args.format))

        if args.format == 'avro':
            from write_avro import write_avro

            with open(filename, 'wb') as f:
                write_avro(f, fields, info, node_map, data)
        elif args.format == 'csv':
            with open(filename, 'w') as f:
                f.write('{}\n'.format(','.join(fields)))

                np.savetxt(f, data, fmt='%.6f', delimiter=',')

        filename_list.append(filename)

    if not args.quiet:
        for filename in filename_list:
            logging.info('wrote output file "{}"'.format(filename))

    return filename_list


if __name__ == '__main__':
    import sys

    main(sys.argv[1:])
