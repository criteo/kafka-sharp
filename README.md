High performance .NET Kafka driver 

Features:
=========

* Kafka 0.8+
* Full pipelining of requests over one connection
* Batching of produce requests and fetch requests
* Compression support
* Fully asynchronous non blocking implementation
* Resilience to broker/cluster crashes, partition topology changes, network errors, etc.
* Consumer is simple only: you have to inject which partitions you're consuming from

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
* More tests: more Moq usage, remove some stuff in Mocks.cs
* Optimizations (more memory tuning, crc32, etc.)
* Rewrite serialization
* Simpler design (merge Producer and Consumer? Merge all in Cluster?)
* Front interfaces more like the Java official driver
* Implement Offset Fetch/Commit for consumers synchronization

Inspirations:
=============

* Crc32 implementation comes from DamienGKit: https://github.com/damieng/DamienGKit
* Serialization code is largely borrowed from NTent driver: https://github.com/ntent-ad/kafka4net
