#!/usr/bin/env python

"""
Read Kafka topic contents and output it to the stdout.
"""

import getopt
import kafka
import os.path
import sys


def usage(exitcode = 1):
    """
    Show usage info and exit.
    """
    argv0 = os.path.basename(sys.argv[0])
    sys.stdout.write(
        'Usage: ' + argv0 + ' [options] topicname\n'
        ' Options:\n'
        '  --host Host   Kafka node hostname. Default is "localhost";\n'
        '  --port Port   Kafka node port number. Default is 9092;\n'
        '  -f            output appended data as the topic grows.\n')
    sys.exit(exitcode)


if __name__ == '__main__':
    try:
        cmd_opts, cmd_args = getopt.getopt(
            sys.argv[1:], 'f', ['host=', 'port='])
        cmd_opts = dict(cmd_opts)
    except getopt.GetoptError as exc:
        sys.stderr.write('Error: ' + str(exc) + '\n')
        usage()
    if len(cmd_args) != 1:
        usage()
    host = cmd_opts.get('--host', 'localhost')
    port = int(cmd_opts.get('--port', '9092'))
    tailf = '-f' in cmd_opts
    topic = cmd_args[0]
    try:
        client = kafka.KafkaClient(host + ':' + str(port))
        if tailf:
            consumer = kafka.SimpleConsumer(
                client, 'kafcat', topic)
        else:
            consumer = kafka.SimpleConsumer(
                client, 'kafcat', topic, iter_timeout = 0.5)
        consumer.seek(0, 0)
        for message in consumer:
            print(message.message.value)
    except KeyboardInterrupt:
        sys.exit(130)
