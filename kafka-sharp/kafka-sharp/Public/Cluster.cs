// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Kafka.Common;
using Kafka.Protocol;

namespace Kafka.Public
{
    /// <summary>
    /// Some statistics about the cluster.
    /// </summary>
    public class Statistics
    {
        /// <summary>
        /// Number of messages successfully sent (for which we have received an ack)
        /// </summary>
        public long SuccessfulSent { get; internal set; }

        /// <summary>
        /// Number of requests sent to the Kafka cluster
        /// </summary>
        public long RequestSent { get; internal set; }

        /// <summary>
        /// Number of responses received from the Kafka cluster
        /// </summary>
        public long ResponseReceived { get; internal set; }

        /// <summary>
        /// Number of hard errors encountered (network errors or decode errors)
        /// </summary>
        public long Errors { get; internal set; }

        /// <summary>
        ///  Number of times nodes have been marked dead (not the current number of dead nodes)
        /// </summary>
        public long NodeDead { get; internal set; }

        /// <summary>
        /// Number of expired messages.
        /// </summary>
        public long Expired { get; internal set; }

        /// <summary>
        /// Number of discarded messages.
        /// </summary>
        public long Discarded { get; internal set; }

        public long Exit { get; internal set; }

        public override string ToString()
        {
            return string.Format(
                "{{Messages successfully sent: {0} - Requests sent: {1} - Responses received: {2} - Errors: {3} - Dead nodes: {4} - Expired: {5} - Discarded: {6} - Exit: {7}}}",
                SuccessfulSent, RequestSent, ResponseReceived, Errors, NodeDead, Expired, Discarded, Exit);
        }
    }

    /// <summary>
    /// Cluster public interface, so you may mock it.
    /// </summary>
    public interface ICluster : IDisposable
    {
        /// <summary>
        /// Send a string as Kafka message. The string will be UTF8 encoded.
        /// </summary>
        /// <param name="topic">Kafka message topic</param>
        /// <param name="data">Message value</param>
        void Produce(string topic, string data);

        /// <summary>
        /// Send a string as Kafka message with the given key. The string and key will be UTF8 encoded.
        /// </summary>
        /// <param name="topic">Kafka message topic</param>
        /// <param name="key">Message key</param>
        /// <param name="data">Message value</param>
        void Produce(string topic, string key, string data);

        /// <summary>
        /// Send an array of bytes to a Kafka cluster.
        /// </summary>
        /// <param name="topic">Kafka message topic</param>
        /// <param name="data">Message value</param>
        void Produce(string topic, byte[] data);

        /// <summary>
        /// Send an array of bytes to a Kafka Cluster, using the given key.
        /// </summary>
        /// <param name="topic">Kafka message topic</param>
        /// /// <param name="key">Message key</param>
        /// <param name="data">Message value</param>
        void Produce(string topic, byte[] key, byte[] data);

        /// <summary>
        /// Current statistics if the cluster.
        /// </summary>
        Statistics Statistics { get; }

        /// <summary>
        /// Shutdown the cluster connection
        /// </summary>
        Task Shutdown();
    }

    public class Cluster : ICluster
    {
        private readonly Kafka.Cluster.Cluster _cluster;
        private readonly Configuration _configuration;
        private readonly ILogger _logger;

        private static Configuration CloneConfig(Configuration configuration)
        {
            configuration = new Configuration
            {
                BatchSize = configuration.BatchSize,
                BufferingTime = configuration.BufferingTime,
                ClientId = configuration.ClientId,
                CompressionCodec = configuration.CompressionCodec,
                ErrorStrategy = configuration.ErrorStrategy,
                MaxBufferedMessages = configuration.MaxBufferedMessages,
                MessageTtl = configuration.MessageTtl,
                ReceiveBufferSize = configuration.ReceiveBufferSize,
                MaximumConcurrency = configuration.MaximumConcurrency,
                RequestTimeoutMs = configuration.RequestTimeoutMs,
                RequiredAcks = configuration.RequiredAcks,
                Seeds = configuration.Seeds,
                SendBufferSize = configuration.SendBufferSize,
                TaskScheduler = configuration.TaskScheduler
            };
            if (configuration.TaskScheduler == TaskScheduler.Default && configuration.MaximumConcurrency > 0)
            {
                configuration.TaskScheduler = new ActionBlockTaskScheduler(configuration.MaximumConcurrency);
            }
            return configuration;
        }

        public Cluster(Configuration configuration, ILogger logger)
            : this(CloneConfig(configuration), logger, null)
        {
        }

        internal Cluster(Configuration configuration, ILogger logger, Kafka.Cluster.Cluster cluster)
        {
            _configuration = configuration;
            _logger = logger;
            _cluster = cluster ?? new Kafka.Cluster.Cluster(configuration, logger);
            _cluster.InternalError += e => _logger.LogError("Cluster internal error: " + e);
            _cluster.Start();
        }

        public Statistics Statistics
        {
            get { return _cluster.Statistics; }
        }

        public void Produce(string topic, string data)
        {
            Produce(topic, null, data);
        }

        public void Produce(string topic, string key, string data)
        {
            Produce(topic, key == null ? null : Encoding.UTF8.GetBytes(key), Encoding.UTF8.GetBytes(data));
        }

        public void Produce(string topic, byte[] data)
        {
            Produce(topic, null, data);
        }

        private long _sent;

        public void Produce(string topic, byte[] key, byte[] data)
        {
            if (_configuration.MaxBufferedMessages > 0)
            {
                if (_sent - _cluster.PassedThrough >= _configuration.MaxBufferedMessages)
                {
                    SpinWait.SpinUntil(() => _sent - _cluster.PassedThrough < _configuration.MaxBufferedMessages);
                }
                Interlocked.Increment(ref _sent);
            }
            _cluster.ProduceRouter.Route(topic, new Message {Key = key, Value = data}, DateTime.UtcNow.Add(_configuration.MessageTtl));
        }

        public Task Shutdown()
        {
            return _cluster.Stop();
        }

        public void Dispose()
        {
            try
            {
                this.Shutdown().Wait();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex.ToString());
            }
        }
    }
}
