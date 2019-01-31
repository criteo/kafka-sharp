// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

using System;
using System.Threading.Tasks;

namespace Kafka.Public
{
    public enum CompressionCodec : byte
    {
        None = 0,
        Gzip = 1,
        Snappy = 2,
        Lz4 = 3,
    }

    public enum RequiredAcks : short
    {
        /// <summary>
        /// Kafka brokers won't send backs acknowledgements.
        /// </summary>
        None = 0,

        /// <summary>
        /// Kafka brokers will send aknowledgement when they
        /// written the message to their local  log.
        /// </summary>
        Leader = 1,

        /// <summary>
        /// Kafka brokers will wait for all in sync replicas
        /// to have written the message to their local log before
        /// sending back acknowledgements.
        /// </summary>
        AllInSyncReplicas = -1
    }

    /// <summary>
    /// Kafka version compatibility mode.
    /// </summary>
    public enum Compatibility
    {
        /// <summary>
        /// Use V0 version of all APIs. This should work with any Kafka version >= 0.8.2.
        /// Exception: to use consumer groups, brokers must be version >= 0.9 (we don't support
        /// any client side zookeeper management).
        /// </summary>
        V0_8_2,

        /// <summary>
        /// Use latest versions of all API, as supported by Kafka 0.10.1. This means that producer
        /// will use v1 message format, and consumer will be able to read v0 and v1 message format.
        /// </summary>
        V0_10_1,

        /// <summary>
        /// Use latest versions of some API, as supported by Kafka 0..0. This means that producer
        /// will use v2 message format, and consumer will be able to read v0, v1 and v2 message format.
        /// For now, only api key Produce (0) and Fetch (1) are updated, mostly to take advantage of
        /// the new message format.
        /// </summary>
        V0_11_0
    }

    /// <summary>
    /// In case of network or protocol errors
    /// </summary>
    public enum ErrorStrategy
    {
        /// <summary>
        /// Discard messages
        /// </summary>
        Discard,

        /// <summary>
        /// Retry sending messsages (this may end up in duplicate messages) for Producer.
        /// Retry fetching messages (this may end up in an infinite loop) for Consumer.
        /// </summary>
        Retry
    }

    /// <summary>
    /// Batching strategy
    /// </summary>
    public enum BatchStrategy
    {
        /// <summary>
        /// A global accumulator will be used before sending batches to nodes.
        /// This means no node will receveive requests until the batch limit
        /// had been reached (i.e. ProduceBatchSize is accross all nodes). This may
        /// be more efficient if your cluster has a large number of nodes (several hundreds).
        /// </summary>
        Global,

        /// <summary>
        /// Each node will use its own accumulator (i.e. ProduceBatchSize is applied by node).
        /// This is the default.
        /// </summary>
        ByNode
    }

    /// <summary>
    /// Strategy when limiting the number of messages in the system.
    /// </summary>
    public enum OverflowStrategy
    {
        /// <summary>
        /// Produce will block once the max number of pending messages has been reached.
        /// </summary>
        Block,

        /// <summary>
        /// Produce will instantly discard the message when the max pending has been reached.
        /// </summary>
        Discard
    }

    public class Configuration
    {
        /// <summary>
        /// Kafka version compatibility mode.
        /// </summary>
        public Compatibility Compatibility = Compatibility.V0_8_2;

        /// <summary>
        /// Maximum amount a message can stay alive before being discard in case of repeated errors.
        /// </summary>
        public TimeSpan MessageTtl = TimeSpan.FromMinutes(1);

        /// <summary>
        /// Maximum number of retry when sending messages in error.
        /// -1 to have an unbounded number of retry (until reaching message TTL).
        /// </summary>
        public int MaxRetry = 3;

        /// <summary>
        /// Period between each metadata autorefresh.
        /// </summary>
        public TimeSpan RefreshMetadataInterval = TimeSpan.FromMinutes(10);

        /// <summary>
        /// Minimum interval between two refresh metadata (to avoid bombing
        /// the cluster with too many calls when you have a large number of clients).
        /// </summary>
        public TimeSpan MinimumTimeBetweenRefreshMetadata = TimeSpan.FromSeconds(42);

        /// <summary>
        /// When a recoverable error corresponding to a topology change is returned,
        /// involved partitions will be ignored for a time by the producer.
        /// </summary>
        public TimeSpan TemporaryIgnorePartitionTime = TimeSpan.FromSeconds(42);

        /// <summary>
        /// Strategy in case opf network errors for Producer.
        /// </summary>
        public ErrorStrategy ErrorStrategy = ErrorStrategy.Discard;

        /// <summary>
        /// Strategy in case of deserialization Error for Consumer.
        /// </summary>
        public ErrorStrategy ConsumerErrorStrategy = ErrorStrategy.Discard;

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
        /// Strategy for batching (per node, or global)
        /// </summary>
        public BatchStrategy BatchStrategy = BatchStrategy.ByNode;

        /// <summary>
        /// The compression codec used to compress messages to this cluster.
        /// </summary>
        public CompressionCodec CompressionCodec = CompressionCodec.None;

        /// <summary>
        /// If you don't provide a partition when producing a message, the parttion selector will
        /// round robin between all available partitions. Use this variable to delay switching between
        /// partitions until a set number of messages have been sent (on a given topic).
        /// This is useful if you have a large number of partitions per topic and want to fully take
        /// advantage of compression because message set are compressed per topic/per partition.
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
        /// Alternative to the "Seeds" configuration setting, for having dynamic discovery of the cluster.
        /// If present, will override the "Seeds" setting.
        /// 
        /// Seeds are used as fallback in case the driver ends up with a topology with 0 brokers
        /// (can potentially happen if cluster is down for some time, or metadata refresh issue).
        /// At this point, maybe the original seed brokers are not up anymore, so having a way to
        /// dynamically request the list of seed can solve this issue.
        /// </summary>
        public Func<string> SeedsGetter = null;

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
        /// Maximum number of produce messages in the system before blocking/discarding send from clients.
        /// By default we never block and the number is unbounded.
        /// </summary>
        public int MaxBufferedMessages = -1;

        /// <summary>
        /// Maximum number of postponed produce messages when no partition is available.
        /// </summary>
        public int MaxPostponedMessages = 1000;

        /// <summary>
        /// Number of errors on a node before considering it dead
        /// </summary>
        public int MaxSuccessiveNodeErrors = 2;

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
        /// The maximum amount of data brokers should return for a fetch request.
        /// If the first message to be sent takes more size than this limit, then the
        /// broker will still send it (alone) so that the consumer is not blocked.
        /// </summary>
        public int FetchMaxBytes = 50 * 1024 * 1024;

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

        /// <summary>
        /// What to do in case of offset out range error when reading a partition.
        /// Switch to earliest offset or latest offset.
        /// </summary>
        public Offset OffsetOutOfRangeStrategy = Offset.Earliest;

        /// <summary>
        /// Class holding information about the partition selection strategy for
        /// each topic. It uses the default Round Robin strategy if none is set
        /// for a given topic.
        /// </summary>
        public PartitionSelectionConfig PartitionSelectionConfig = new PartitionSelectionConfig();
    }

    public enum Offset
    {
        Earliest = -2,
        Latest = -1,

        [Obsolete]
        Lastest = -1
    }

    /// <summary>
    /// A class used to configure one consumer group
    /// </summary>
    public class ConsumerGroupConfiguration
    {
        /// <summary>
        /// Maximum time between two Heartbeat requests
        /// </summary>
        public int SessionTimeoutMs = 15000;

        /// <summary>
        /// Maximum time to rejoin when a rebalance occurs
        /// </summary>
        public int RebalanceTimeoutMs = 10000;

        /// <summary>
        /// If > 0, offsets will be autocommitted at this rate
        /// </summary>
        public int AutoCommitEveryMs = -1;

        /// <summary>
        /// Time to keep committed offsets on the broker side.
        /// </summary>
        public long OffsetRetentionTimeMs = -1;

        /// <summary>
        /// If no saved offset is found when joining a group, start consumeing from
        /// this offset
        /// </summary>
        public Offset DefaultOffsetToReadFrom = Offset.Earliest;

        /// <summary>
        /// Time to wait before retrying to discover a coordinator
        /// </summary>
        public TimeSpan CoordinatorDiscoveryRetryTime = TimeSpan.FromMilliseconds(5000);
    }
}
