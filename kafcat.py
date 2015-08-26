#!/usr/bin/env python

"""
Read Kafka topic contents and output it to the stdout.
"""

import getopt
import os.path
import sys
import time

import kafka
import kafka.common


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
        '  --id ClientID\n'
        '                Kafka client ID. Default is \'kafcat\';\n'
        '  --maxtime Seconds\n'
        '                Maximum time to work. If defined, kafcat will\n'
        '                exit after the given amount of time even if there\n'
        '                is data to read in the Kafka;\n'
        '  --maxmsgs Count\n'
        '                Maximum messages to read. If defined, kafcat will\n'
        '                exit after Count messages will be read even if\n'
        '                there is data to read in the Kafka;\n'
        '  -b            extract from the beginning;\n'
        '  -f            output appended data as the topic grows.\n')
    sys.exit(exitcode)


if __name__ == '__main__':
    # Parse command line options
    try:
        cmd_opts, cmd_args = getopt.getopt(
            sys.argv[1:], 'bf', ['host=', 'port=', 'id=', 'maxtime=',
                                 'maxmsgs='])
        cmd_opts = dict(cmd_opts)
    except getopt.GetoptError as exc:
        sys.stderr.write('Error: ' + str(exc) + '\n')
        usage()
    if len(cmd_args) != 1:
        usage()
    consumer = None
    try:
        # connect to Kafka
        host = cmd_opts.get('--host', 'localhost')
        port = int(cmd_opts.get('--port', '9092'))
        client_id = cmd_opts.get('--id', 'kafcat')
        maxtime = cmd_opts.get('--maxtime', None)
        if maxtime is not None:
            maxtime = int(maxtime)
        maxmsgs = cmd_opts.get('--maxmsgs', None)
        if maxmsgs is not None:
            maxmsgs = int(maxmsgs)
        client = kafka.KafkaClient(host + ':' + str(port))
        # Create consumer object
        consumer = kafka.SimpleConsumer(
            client, client_id, cmd_args[0], iter_timeout = 0.5)
        if '-b' in cmd_opts:
            # seek very first message
            consumer.seek(0, 0)
            consumer.commit()
        # start consuming
        total_messages_read = 0
        started = time.time()
        while True:
            if maxtime is not None and time.time() >= started + maxtime:
                break
            try:
                for message in consumer:
                    total_messages_read += 1
                    sys.stdout.write(message.message.value + '\n')
                    if maxtime is not None and \
                       time.time() >= started + maxtime:
                        break
                    if maxmsgs is not None and \
                       total_messages_read >= maxmsgs:
                        break
            except kafka.common.OffsetOutOfRangeError:
                if total_messages_read == 0:
                    # This kind of error arises when we try to fetch
                    # logs which already was rolled out from the storage.
                    # On the server side Kafka emits message like:
                    #     "kafka.common.OffsetOutOfRangeException: Request
                    #     for offset 0 but we only have log segments in
                    #     the range 39943 to 11471647."
                    # This issue is described in
                    #  https://github.com/mumrah/kafka-python/issues/72
                    # The workaround is to make consumer.seek(0,0).
                    # Obviously, the issue is not reproduced when kafcat
                    # is invoked with '-b' command line option.
                    consumer.seek(0, 0)
                    consumer.commit()
                    # try to consume again:
                    for message in consumer:
                        total_messages_read += 1
                        sys.stdout.write(message.message.value + '\n')
                        if maxtime is not None and \
                           time.time() >= started + maxtime:
                            break
                        if maxmsgs is not None and \
                           total_messages_read >= maxmsgs:
                            break
            if '-f' not in cmd_opts:
                break
        # save current position for the future use
        consumer.commit()
    except KeyboardInterrupt:
        if consumer is not None:
            # save current position for the future use
            consumer.commit()
        sys.exit(130)
