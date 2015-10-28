#!/bin/sh

# Download Kafka binaries and extract them
if [ $1 = 'begin' ]
then
wget -nv -O kafka.tgz http://filer.criteo.prod/remote_files/kafka/kafka_2.10-0.8.2.1.tgz
tar zxf kafka.tgz
rm kafka.tgz
fi

