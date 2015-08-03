// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. 
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

using System;
using System.Threading.Tasks;

namespace Kafka.Public
{
    public enum CompressionCodec
    {
        None = 0,
        Gzip = 1,
        Snappy = 2
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
        /// Maximum amount a message can stay alive before being discard in case of pepeated errors.
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
        public TimeSpan BufferingTime = TimeSpan.FromMilliseconds(100);

        /// <summary>
        /// The compression codec used to compress messages to this cluster.
        /// </summary>
        public CompressionCodec CompressionCodec = CompressionCodec.None;

        /// <summary>
        /// Maximum size of message batches.
        /// </summary>
        public int BatchSize = 1000;

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
        public int RequiredAcks = 1;

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
        /// </summary>
        public TaskScheduler TaskScheduler = TaskScheduler.Default;

        /// <summary>
        /// Maximum number of messages in the system before blocking send from clients.
        /// By default we never block and the number is unbounded.
        /// </summary>
        public int MaxBufferedMessages = -1;
    }
}
