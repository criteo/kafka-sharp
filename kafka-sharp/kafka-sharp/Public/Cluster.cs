// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

using System;
using System.Reactive.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Kafka.Common;
using Kafka.Protocol;
using Kafka.Routing;

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

        /// <summary>
        /// Number of produce request that have exited the system either successful, discard or expired.
        /// </summary>
        public long Exit { get; internal set; }

        /// <summary>
        /// Number of received messages.
        /// </summary>
        public long Received { get; internal set; }

        public override string ToString()
        {
            return string.Format(
                "{{Messages successfully sent: {0} - Messages received: {8} - Requests sent: {1} - Responses received: {2} - Errors: {3} - Dead nodes: {4} - Expired: {5} - Discarded: {6} - Exit: {7}}}",
                SuccessfulSent, RequestSent, ResponseReceived, Errors, NodeDead, Expired, Discarded, Exit, Received);
        }
    }

    /// <summary>
    /// Cluster public interface, so you may mock it.
    /// </summary>
    public interface IClusterClient : IDisposable
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
        /// Consume messages from the given topic, from all partitions, and starting from
        /// the oldest available message.
        /// </summary>
        /// <param name="topic"></param>
        void ConsumeFromEarliest(string topic);

        /// <summary>
        /// Consume messages form the given topic, from all partitions, starting from new messages.
        /// </summary>
        /// <param name="topic"></param>
        void ConsumeFromLatest(string topic);

        /// <summary>
        /// Consume messages from the given topic / partition,  starting from
        /// the oldest available message.
        /// </summary>
        /// <param name="topic"></param>
        /// <param name="partition"></param>
        void ConsumeFromEarliest(string topic, int partition);

        /// <summary>
        /// Consume messages from the given topic / partition,  starting from
        /// new messages.
        /// </summary>
        /// <param name="topic"></param>
        /// <param name="partition"></param>
        void ConsumeFromLatest(string topic, int partition);

        /// <summary>
        /// Consume messages from the given topic / partition, starting from the given offset.
        /// </summary>
        /// <param name="topic"></param>
        /// <param name="partition"></param>
        /// <param name="offset"></param>
        void Consume(string topic, int partition, long offset);

        /// <summary>
        /// Stop consuming messages from the given topic, effective immediately.
        /// </summary>
        /// <param name="topic"></param>
        void StopConsume(string topic);

        /// <summary>
        /// Stop consuming messages from the given topic / partition, effective immediately.
        /// </summary>
        /// <param name="topic"></param>
        /// <param name="partition"></param>
        void StopConsume(string topic, int partition);

        /// <summary>
        /// Stop consuming messages from the given topic / partition once the given offset
        /// has been reached.
        /// </summary>
        /// <param name="topic"></param>
        /// <param name="partition"></param>
        /// <param name="offset"></param>
        void StopConsume(string topic, int partition, long offset);

        /// <summary>
        /// Messages received from the brokers.
        /// </summary>
        event Action<KafkaRecord> MessageReceived;

        /// <summary>
        /// The stream of received messages. Use this if you prefer using Reactive Extensions
        /// to manipulate streams of messages.
        /// </summary>
        IObservable<KafkaRecord> Messages { get; }

        /// <summary>
        /// Current statistics if the cluster.
        /// </summary>
        Statistics Statistics { get; }

        /// <summary>
        /// Shutdown the cluster connection
        /// </summary>
        Task Shutdown();
    }

    /// <summary>
    /// The public Kafka cluster representation. All operations goes through this class.
    ///
    /// </summary>
    public class ClusterClient : IClusterClient
    {
        private readonly Cluster.Cluster _cluster;
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

        /// <summary>
        /// This event is raised when a message is consumed. This happen in the context of the
        /// underlying fetch loop, so you can take advantage of that to throttle the whole system.
        /// </summary>
        public event Action<KafkaRecord> MessageReceived = _ => { };

        /// <summary>
        /// The stream of consumed messages as an Rx stream. By default it is observed in the context
        /// of the underlying fetch loop, so you can take advantage of that to throttle the whole system
        /// or you can oberve it on its own scheduler.
        /// </summary>
        public IObservable<KafkaRecord> Messages { get; private set; }

        /// <summary>
        /// Initialize a client to a Kafka cluster.
        /// </summary>
        /// <param name="configuration"></param>
        /// <param name="logger"></param>
        public ClusterClient(Configuration configuration, ILogger logger)
            : this(CloneConfig(configuration), logger, null)
        {
        }

        internal ClusterClient(Configuration configuration, ILogger logger, Cluster.Cluster cluster)
        {
            _configuration = configuration;
            _logger = logger;
            _cluster = cluster ?? new Cluster.Cluster(configuration, logger);
            _cluster.InternalError += e => _logger.LogError("Cluster internal error: " + e);
            _cluster.ConsumeRouter.MessageReceived += kr => MessageReceived(kr);
            Messages = Observable.FromEvent<KafkaRecord>(a => MessageReceived += a, a => MessageReceived -= a);
            _cluster.Start();
        }

        public Statistics Statistics
        {
            get { return _cluster.Statistics; }
        }

        public void ConsumeFromEarliest(string topic)
        {
            _cluster.ConsumeRouter.StartConsume(topic, Partition.All.Id, Offsets.Earliest);
        }

        public void ConsumeFromLatest(string topic)
        {
            _cluster.ConsumeRouter.StartConsume(topic, Partition.All.Id, Offsets.Latest);
        }

        public void ConsumeFromEarliest(string topic, int partition)
        {
            if (partition < Partition.All.Id)
                throw new ArgumentException("Ivalid partition Id", "partition");
            _cluster.ConsumeRouter.StartConsume(topic, partition, Offsets.Earliest);
        }

        public void ConsumeFromLatest(string topic, int partition)
        {
            if (partition < Partition.All.Id)
                throw new ArgumentException("Ivalid partition Id", "partition");
            _cluster.ConsumeRouter.StartConsume(topic, partition, Offsets.Latest);
        }

        public void Consume(string topic, int partition, long offset)
        {
            if (partition < Partition.All.Id)
                throw new ArgumentException("Ivalid partition Id", "partition");
            if (offset < Offsets.Earliest)
                throw new ArgumentException("Invalid offset", "offset");
            _cluster.ConsumeRouter.StartConsume(topic, partition, offset);
        }

        public void StopConsume(string topic)
        {
            _cluster.ConsumeRouter.StopConsume(topic, Partition.All.Id, Offsets.Now);
        }

        public void StopConsume(string topic, int partition)
        {
            if (partition < 0)
                throw new ArgumentException("Partition Ids are always positive.", "partition");
            _cluster.ConsumeRouter.StopConsume(topic, partition, Offsets.Now);
        }

        public void StopConsume(string topic, int partition, long offset)
        {
            if (partition < 0)
                throw new ArgumentException("Partition Ids are always positive.", "partition");
            if (offset < 0)
                throw new ArgumentException("Offsets are always positive.", "offset");
            _cluster.ConsumeRouter.StopConsume(topic, partition, offset);
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
                Shutdown().Wait();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex.ToString());
            }
        }
    }
}
