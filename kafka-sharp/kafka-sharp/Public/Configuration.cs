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
        Snappy = 2
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
    /// In case of network errors
    /// </summary>
    public enum ErrorStrategy
    {
        /// <summary>
        /// Discard messages
        /// </summary>
        Discard,

        /// <summary>
        /// Retry sending messsages (this may end up in duplicate messages)
        /// </summary>
        Retry
    }

    public class Configuration
    {
        /// <summary>
        /// Maximum amount a message can stay alive before being discard in case of repeated errors.
        /// </summary>
        public TimeSpan MessageTtl = TimeSpan.FromMinutes(1);

        /// <summary>
        /// Strategy in case opf network errors.
        /// </summary>
        public ErrorStrategy ErrorStrategy = ErrorStrategy.Discard;

        /// <summary>
        /// Time slice for batching messages. We wait  that much time at most before processing
        /// a batch of messages.
        /// </summary>
        public TimeSpan BufferingTime = TimeSpan.FromMilliseconds(5000);

        /// <summary>
        /// The compression codec used to compress messages to this cluster.
        /// </summary>
        public CompressionCodec CompressionCodec = CompressionCodec.None;

        /// <summary>
        /// Maximum size of message batches.
        /// </summary>
        public int BatchSize = 200;

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
        public RequiredAcks RequiredAcks = RequiredAcks.Leader;

        /// <summary>
        /// Kafka server side timeout for requests.
        /// </summary>
        public int RequestTimeoutMs = 10000;

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
        public int MaximumConcurrency = -1;

        /// <summary>
        /// Maximum number of messages in the system before blocking send from clients.
        /// By default we never block and the number is unbounded.
        /// </summary>
        public int MaxBufferedMessages = -1;

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
    }
}
