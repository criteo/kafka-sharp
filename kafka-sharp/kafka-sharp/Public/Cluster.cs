// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. 
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

using System;
using System.Text;
using System.Threading;
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
        /// Number of errors encountered
        /// </summary>
        public long Errors { get; internal set; }

        /// <summary>
        ///  Number of times nodes have been marked dead (not the current number of dead nodes)
        /// </summary>
        public long NodeDead { get; internal set; }

        /// <summary>
        /// Number of expired messages (not entirely accurate)
        /// </summary>
        public long Expired { get; internal set; }

        public override string ToString()
        {
            return string.Format(
                "{{Messages successfully sent: {0} - Requests sent: {1} - Responses received: {2} - Errors: {3} - Dead nodes: {4} - Expired: {5}}}",
                SuccessfulSent, RequestSent, ResponseReceived, Errors, NodeDead, Expired);
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
    }

    public class Cluster : ICluster
    {
        private readonly Kafka.Cluster.Cluster _cluster;
        private readonly Configuration _configuration;
        private readonly ILogger _logger;

        public Cluster(Configuration configuration, ILogger logger)
        {
            _configuration = configuration;
            _logger = logger;
            _cluster = new Kafka.Cluster.Cluster(configuration, logger);
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

        public void Produce(string topic, byte[] key, byte[] data)
        {
            if (_configuration.MaxBufferedMessages > 0)
            {
                if (_cluster.Router.WaterLevel >= _configuration.MaxBufferedMessages)
                {
                    SpinWait.SpinUntil(() => _cluster.Router.WaterLevel < _configuration.MaxBufferedMessages);
                }
            }
            _cluster.Router.Route(topic, new Message {Key = key, Value = data}, DateTime.UtcNow.Add(_configuration.MessageTtl));
        }

        public void Dispose()
        {
            try
            {
                _cluster.Stop().Wait();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex.ToString());
            }
        }
    }
}
