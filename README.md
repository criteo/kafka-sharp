# "High Performance" .NET Kafka Driver

A .NET implementation of the Apache Kafka client side protocol geared toward performance (both
throughput and memory wise). It is especially suited for scenarios where applications
are streaming a large number of messages across a fair number of topics.


## Features

* Fully asynchronous batched Producer
* Simple asynchronous event based Consumer
* Consumer groups support
* No Zookeeper dependency
* Compression support (both Gzip and Snappy)
* High configurability
* Memory friendly APIs and internals

## Cluster

The `ClusterClient` class is the main entry point to access a Kafka cluster. It provides an untyped API
to Produce and Consume operations.

		var cluster = new ClusterClient(new Configuration{Seeds = "broker.local:9091"}, new SomeLogger());
		cluster.Produce("some_topic", someValue);

		cluster.MessageReceived += kafkaRecord => { /* do something */ };
		cluster.ConsumeFromLatest("some_topic", somePartition);
		// OR (for consumer group usage)
		cluster.Subscribe("some group", new[] { "topic", "some_other_topic" }, new ConsumerGroupConfiguration { AutoCommitEveryMs = 5000 });

## Producer

Producer API is accessed either through the `ClusterClient` class or the `KafkaProducer` class
if you prefer a typed interface. Configuration options allow:

* setting the batch size and time window for Produce requests
* setting a strategy for error management (either discard messages or retry)
* setting the maximum number of pending messages allowed in the system (can be uncapped)
* setting a TTL on messages: messages will be kept up to the TTL in the driver in case
  of difficulties to send Produce request to the Kafka brokers.

### Partitioning

By default messages are round robined between all available partitions. The `NumberOfMessagesBeforeRoundRobin`
enables tweaking the round robin scheme to pack more messages in a partition before switching to another. This is especially useful when compressing the data.

If you need custom partitioning it's possible to send messages directly to a given partition.

### Serialization

Serialization of messages can be handled two ways:

* you can provide a serializer for each topic, as well as providing a default serializer for all topics.
* if your message keys or values implement the provided `IMemorySerializable` interface, that will be used to serialize data.

You also have the option serialize the messages a soon as they enter the producer or at send time.
In the first case no reference will be held on the original data (key and value) so it may be more
efficient memory wise, however in case of error ending in discarding/expriing messages the producer
won't be able to provide the original message when signaling the error. In the later case references
will be held and the original data will be returned in case of error. This means the original  data will survive for the duration of a batching cycle, so more generation garbage collection is to
be expected, however if the original data implements `IDisposable`, `Dispose` will be called in due
time, which may help implementing a pooling mechanism.

### Compression

Both gzip and snappy compression are supported. This is controlled by a parameter of the
`ClusterClient` configuration. Snappy makes use of the native Google library.

### Error handling

Most errors will be automanaged by the driver. "Irrecoverable" errors which end up discarding
some messages are signaled through events.

## Consumer

You can either consume messages as member of a consumer group or as a standalone simple consumer. In simple mode
you can discover topics and partitions and choose which ones to consume. In consumer group mode you just start
a subscription and let the protocol handle the partition attribution.

Messages are received as a stream of events. The API is accessed either through
`ClusterClient` (untyped API) or `KafkaConsumer` (typed API). When using a consumer group you can manage offsets
via specifying an autcommit period or requiring commits directly (or mix both). In simple mode you can directly specify
which offsets to start consuming from (including latest and earliest offsets). `Pause` and `Resume` methods are available
in both modes to stop / resume consuming from a topic.

### Deserialization

You can provide a deserializer for each topic so that messages are returned as objects instead of byte arrays.

## Configuration

The client is configured via the `Configuration` class, by setting the following properties:

        /// <summary>
        /// Kafka version compatibility mode.
        /// 0.8.2 compatibility will work with any kafka version >= 0.8.2.
        /// However consumer group support will require brokers >= 0.9 to actually work in this mode.
        /// 0.10.1 compatibility will work with kafka >= 0.10.1.
        /// </summary>
        public Compatibility Compatibility = Compatibility.V0_8_2;

        /// <summary>
        /// Maximum amount a message can stay alive before being discarded in case of repeated errors.
        /// </summary>
        public TimeSpan MessageTtl = TimeSpan.FromMinutes(1);

        /// <summary>
        /// Period between each metadata autorefresh.
        /// </summary>
        public TimeSpan RefreshMetadataInterval = TimeSpan.FromMinutes(5);

        /// <summary>
        /// Strategy in case of network errors.
        /// </summary>
        public ErrorStrategy ErrorStrategy = ErrorStrategy.Discard;

        /// <summary>
        /// Time slice for batching messages. We wait  that much time at most before processing
        /// a batch of messages.
        /// </summary>
        public TimeSpan ProduceBufferingTime = TimeSpan.FromMilliseconds(5000);

        /// <summary>
        /// Maximum size of message batches.
        /// </summary>
        public int ProduceBatchSize = 200;

        /// <summary>
        /// Strategy for batching (per node, or global).
        /// </summary>
        public BatchStrategy BatchStrategy = BatchStrategy.ByNode;

        /// <summary>
        /// The compression codec used to compress messages to this cluster.
        /// </summary>
        public CompressionCodec CompressionCodec = CompressionCodec.None;

        /// <summary>
        /// If you don't provide a partition when producing a message, the partition selector will
        /// round robin between all available partitions. Use this variable to delay switching between
        /// partitions until a set number of messages has been sent (on a given topic).
        /// This is useful if you have a large number of partitions per topic and want to fully take
        /// advantage of compression (because message sets are compressed per topic/per partition).
        /// </summary>
        public int NumberOfMessagesBeforeRoundRobin = 1;

        /// <summary>
        /// Socket buffer size for send.
        /// </summary>
        public int SendBufferSize = 100 * 1024;

        /// <summary>
        /// Socket buffer size for receive.
        /// </summary>
        public int ReceiveBufferSize = 64 * 1024;

        /// <summary>
        /// Acknowledgements required.
        /// </summary>
        public RequiredAcks RequiredAcks = RequiredAcks.AllInSyncReplicas;

        /// <summary>
        /// Minimum in sync replicas required to consider a partition as alive.
        /// <= 0 means only leader is required.
        /// </summary>
        public int MinInSyncReplicas = -1;

        /// <summary>
        /// If you use MinInSyncReplicas and a broker replies with the NotEnoughReplicasAfterAppend error,
        /// we will resend the messages if this value is true.
        /// </summary>
        public bool RetryIfNotEnoughReplicasAfterAppend = false;

        /// <summary>
        /// Kafka server side timeout for requests.
        /// </summary>
        public int RequestTimeoutMs = 10000;

        /// <summary>
        /// Client side timeout: if a request is not acknowledged
        /// by this time, the connection will be reset. Obviously
        /// it should be greater than RequestTimeoutMs
        /// </summary>
        public int ClientRequestTimeoutMs = 20000;

        /// <summary>
        /// The maximum number of unacknowledged requests the client will
        /// send on a single connection before async waiting for the connection.
        /// Note that if this setting is set to be greater than 1 and there are
        /// failed sends, there is a risk of message re-ordering due to retries.
        /// </summary>
        public int MaxInFlightRequests = 5;

        /// <summary>
        /// Your client name.
        /// </summary>
        public string ClientId = "Kafka#";

        /// <summary>
        /// Brokers to which to connect to boostrap the cluster and discover the toplogy.
        /// </summary>
        public string Seeds = "";

        /// <summary>
        /// A TaskScheduler to use for all driver internal work.
        /// Useful if you want to limit the ressources taken by the driver.
        /// By default we use the default scheduler (which maps to .NET thread pool),
        /// which may induce "busy neighbours" problems in case of high overload.
        ///
        /// If this is not TaskScheduler.Default, this will always superseed
        /// the MaximumConcurrency variable.
        /// </summary>
        public TaskScheduler TaskScheduler = TaskScheduler.Default;

        /// <summary>
        /// Maximum concurrency used inside the driver. This is only taken into
        /// account if TaskScheduler is set to its default value. This is
        /// implemented using a special custom TaskScheduler which makes use of the
        /// .NET threadpool without squatting threads when there's nothing to do.
        /// </summary>
        public int MaximumConcurrency = 3;

        /// <summary>
        /// Maximum number of messages in the system before blocking/discarding send from clients.
        /// By default we never block and the number is unbounded.
        /// </summary>
        public int MaxBufferedMessages = -1;

        /// <summary>
        /// The strategy to use when the maximum number of pending produce messages
        /// has been reached. By default we block.
        /// </summary>
        public OverflowStrategy OverflowStrategy = OverflowStrategy.Block;

        /// <summary>
        /// The maximum amount of time in ms brokers will block before answering fetch requests
        /// if there isn't sufficient data to immediately satisfy FetchMinBytes
        /// </summary>
        public int FetchMaxWaitTime = 100;

        /// <summary>
        /// The minimum amount of data brokers should return for a fetch request.
        /// If insufficient data is available the request will wait for that much
        /// data to accumulate before answering the request.
        /// </summary>
        public int FetchMinBytes = 1;

        /// <summary>
        /// The number of bytes of messages to attempt to fetch for each topic-partition
        /// in each fetch request. These bytes will be read into memory for each partition,
        /// so this helps control the memory used by the consumer. The fetch request size
        /// must be at least as large as the maximum message size the server allows or else
        /// it is possible for producers to send messages larger than the consumer can fetch.
        /// </summary>
        public int FetchMessageMaxBytes = 1024 * 1024;

        /// <summary>
        /// Time slice for batching messages used when consuming (Offset and Fetch).
        /// We wait  that much time at most before processing a batch of messages.
        /// </summary>
        public TimeSpan ConsumeBufferingTime = TimeSpan.FromMilliseconds(1000);

        /// <summary>
        /// Maximum size of consume message batches (Offset and Fetch). Keep it small.
        /// </summary>
        public int ConsumeBatchSize = 10;

        /// <summary>
        /// Serialization configuration options and (de)serializers.
        /// </summary>
        public SerializationConfig SerializationConfig = new SerializationConfig();

## Technical details

### Internals

The driver makes heavy use of the Tpl.Dataflow library for its actors system. Rx.NET streams of all events
are also provided. Of course plain .NET events are also provided.

### Network layer

The driver makes direct use of the .NET asynchronous Socket API (the one based on `SocketAsyncEventArgs`).

### Memory management

Pretty much everything is pooled in the driver and message buffers are never pinned so the stress
of the driver on the GC should stay low.

When producing messages you can control the maximum number of pending messages via the `MaxBufferedMessages`
configuration option. When the limit is reached the Send operations will become blocking or will discard messages
according to the value of the `OverflowStrategy` configuration option.

You can also decide if messages are preserialized upon calling `Produce` methods or kept until an entire
batch is serialized. In the latter case if your message keys/values implement `IDisposable` they will be
disposed when the messages have effectively been sent (you may take advantage of this to implement a pooling
mechanism for keys/values).

When consuming messages, the `FetchMessageMaxBytes` configuration option can help limit the maximum amount of
data retrieved in consumer operations. Also the `MessageReceived` event is always emitted from one thread at a time
and until the subscribers return the consumer will not perform any operation. You can take advantage of that
to effectively throttle the consumer.


### Threading

Threading can be controlled by the `MaximumConcurrency` configuration parameter. Alternatively you can provide
your own TaskScheduler via the `TaskScheduler` configuration option. In any case the driver will never squat threads,
all IO operations are non blocking and the driver never does blocking wait, so any concurrency configuration is only
a hint on up to how many threads the driver may be using at a given time. If you don't provide your own TaskScheduler,
the threads will be picked from the .NET ThreadPool. By default the driver will use at most 3 threads from the threadpool.

## Acknowledgements

* Crc32 implementation comes from DamienGKit: https://github.com/damieng/DamienGKit
* Serialization code is largely borrowed from NTent driver: https://github.com/ntent-ad/kafka4net
