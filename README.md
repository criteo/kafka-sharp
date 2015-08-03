High performance .NET Kafka producer

Features:
=========

* Kafka 0.8+
* Producer only
* Full pipelining of requests over one connection
* Batching of produce requests
* Fully asynchronous non blocking implementation
* Resilience to broker/cluster crashes, partition topology changes, network errors, etc.

TODO:
=====

* Per topic configuration
* More verbose error feedback
* Partitioner injection
* Dynamic configuration change?
* Per topic metadata fetches instead of always getting the full metadata?
* Be verbose when sending invalid topics
* Better handling of shutdown
* More logging
* Consumer
* More tests
* Optimizations (more memory tuning, crc32, etc.)
* Rewrite serialization
* Simpler design (merge Router and Cluster for less messy metadata change management?)
* Front interface more like the Java official driver

Inspirations:
=============

* Crc32 implementation comes from DamienGKit: https://github.com/damieng/DamienGKit
* Serialization code is largely borrowed from NTent driver: https://github.com/ntent-ad/kafka4net
