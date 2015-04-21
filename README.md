# Forward contents of an Apache Kafka topic to the stdout

## Synopsis

```sh
$ kafcat [options] topicname
```

Options:

* _--host Host_ - Kafka node hostname. Default is "localhost";
* _--port Port_ - Kafka node port number. Default is 9092;
* _--id ClientID_ - Kafka client ID. Default is 'kafcat';
* _--maxtime Seconds_ - max time to work. Default is no limit;
* _-f_ - output appended data as the topic grows.

Examples:

```sh
$ kafcat my-topic
message-1
message-2
message-3
...
```

```sh
$ kafcat my-topic
...
...
```
