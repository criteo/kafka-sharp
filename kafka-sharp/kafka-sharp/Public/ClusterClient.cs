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
    /// Cluster public interface, so you may mock it.
    /// </summary>
    public interface IClusterClient : IDisposable
    {
        /// <summary>
        /// Send some data to a Kafka Cluster, with no key.
        /// </summary>
        /// <param name="topic">Kafka message topic</param>
        /// <param name="data">Message value</param>
        void Produce(string topic, object data);

        /// <summary>
        /// Send an array of bytes to a Kafka Cluster, using the given key.
        /// </summary>
        /// <param name="topic">Kafka message topic</param>
        /// <param name="key">Message key</param>
        /// <param name="data">Message value</param>
        void Produce(string topic, object key, object data);

        /// <summary>
        /// Send an array of bytes to a Kafka Cluster, using the given key.
        /// The message will routed to the target partition. This allows
        /// clients to partition data according to a specific scheme.
        /// </summary>
        /// <param name="topic">Kafka message topic</param>
        /// <param name="key">Message key</param>
        /// <param name="data">Message value</param>
        /// <param name="partition">Target partition</param>
        void Produce(string topic, object key, object data, int partition);

        /// <summary>
        /// Consume messages from the given topic, from all partitions, and starting from
        /// the oldest available message.
        /// </summary>
        /// <param name="topic"></param>
        void ConsumeFromEarliest(string topic);

        /// <summary>
        /// Consume messages from the given topic, from all partitions, starting from new messages.
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
        event Action<RawKafkaRecord> MessageReceived;

        /// <summary>
        /// The stream of received messages. Use this if you prefer using Reactive Extensions
        /// to manipulate streams of messages.
        /// </summary>
        IObservable<RawKafkaRecord> Messages { get; }

        /// <summary>
        /// This is raised when a produce message has expired.
        /// </summary>
        event Action<RawKafkaRecord> MessageExpired;

        /// <summary>
        /// Rx observable version of the MessageExpired event.
        /// </summary>
        IObservable<RawKafkaRecord> ExpiredMessages { get; }

        /// <summary>
        /// This is raised when a produce message is discarded due
        /// to a network error or some other Kafka unrecoverable error.
        /// </summary>
        event Action<RawKafkaRecord> MessageDiscarded;

        /// <summary>
        /// Rx observable version of the MessageDiscarded event.
        /// </summary>
        IObservable<RawKafkaRecord> DiscardedMessages { get; }

        /// <summary>
        /// Current statistics if the cluster.
        /// </summary>
        IStatistics Statistics { get; }

        /// <summary>
        /// Shutdown the cluster connection
        /// </summary>
        Task Shutdown();
    }

    /// <summary>
    /// The public Kafka cluster representation. All operations goes through this class.
    /// </summary>
    public sealed class ClusterClient : IClusterClient
    {
        private readonly Cluster.Cluster _cluster;
        private readonly Configuration _configuration;
        private readonly ILogger _logger;

        private static Configuration CloneConfig(Configuration configuration)
        {
            configuration = new Configuration
            {
                ProduceBatchSize = configuration.ProduceBatchSize,
                ProduceBufferingTime = configuration.ProduceBufferingTime,
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
        public event Action<RawKafkaRecord> MessageReceived = _ => { };

        /// <summary>
        /// The stream of consumed messages as an Rx stream. By default it is observed in the context
        /// of the underlying fetch loop, so you can take advantage of that to throttle the whole system
        /// or you can oberve it on its own scheduler.
        /// </summary>
        public IObservable<RawKafkaRecord> Messages { get; private set; }

        /// <summary>
        /// This is raised when a produce message has expired.
        /// The message partition will be set to None and the offset to 0.
        /// </summary>
        public event Action<RawKafkaRecord> MessageExpired = _ => { };

        /// <summary>
        /// Rx observable version of the MessageExpired event.
        /// The message partition will be set to None and the offset to 0.
        /// </summary>
        public IObservable<RawKafkaRecord> ExpiredMessages { get; private set; }

        /// <summary>
        /// This is raised when a produce message is discarded due
        /// to a network error or some other Kafka unrecoverable error.
        /// Note that if you set the cluster in retry mode, it will only
        /// be raised iin case of very strange errors, most of the case
        /// you should get a MessageExpired event instead (cluster will
        /// retry produce on most errors until expiration date comes).
        /// The message partition will be set to None and the offset to 0.
        /// </summary>
        public event Action<RawKafkaRecord> MessageDiscarded = _ => { };

        /// <summary>
        /// Rx observable version of the MessageDiscarded event.
        /// The message partition will be set to None and the offset to 0.
        /// </summary>
        public IObservable<RawKafkaRecord> DiscardedMessages { get; private set; }

        /// <summary>
        /// Initialize a client to a Kafka cluster.
        /// </summary>
        /// <param name="configuration">Kafka configuration.</param>
        /// <param name="logger"></param>
        public ClusterClient(Configuration configuration, ILogger logger)
            : this(configuration, logger, null, null, null)
        {
        }

        /// <summary>
        /// Initialize a client to a Kafka cluster.
        /// </summary>
        /// <param name="configuration">Kafka configuration.</param>
        /// <param name="logger"></param>
        /// <param name="serializationConfig">Serialization configuration if you want to provide custom serializers/deserializers.</param>
        public ClusterClient(Configuration configuration, ILogger logger, SerializationConfig serializationConfig)
            : this(configuration, logger, null, serializationConfig, null)
        {
        }

        /// <summary>
        /// Initialize a client to a Kafka cluster.
        /// </summary>
        /// <param name="configuration">Kafka configuration.</param>
        /// <param name="logger"></param>
        /// <param name="serializationConfig">Serialization configuration if you want to provide custom serializers/deserializers.</param>
        /// <param name="statistics">Provide this if you want to inject custom statistics management.</param>
        public ClusterClient(Configuration configuration, ILogger logger, SerializationConfig serializationConfig, IStatistics statistics)
            : this(CloneConfig(configuration), logger, null, serializationConfig, statistics)
        {
        }

        /// <summary>
        /// Initialize a client to a Kafka cluster.
        /// </summary>
        /// <param name="configuration">Kafka configuration.</param>
        /// <param name="logger"></param>
        /// <param name="statistics">Provide this if you want to inject custom statistics management.</param>
        public ClusterClient(Configuration configuration, ILogger logger, IStatistics statistics)
            : this(CloneConfig(configuration), logger, null, null, statistics)
        {
        }

        internal ClusterClient(Configuration configuration, ILogger logger, Cluster.Cluster cluster)
            : this(configuration, logger, cluster, null, null)
        {
        }

        internal ClusterClient(Configuration configuration, ILogger logger, Cluster.Cluster cluster, SerializationConfig serializationConfig, IStatistics statistics)
        {
            _configuration = configuration;
            _logger = logger;
            _cluster = cluster ?? new Cluster.Cluster(configuration, logger, statistics, serializationConfig);
            _cluster.InternalError += e => _logger.LogError("Cluster internal error: " + e);
            _cluster.ConsumeRouter.MessageReceived += kr => MessageReceived(kr);
            Messages = Observable.FromEvent<RawKafkaRecord>(a => MessageReceived += a, a => MessageReceived -= a);
            _cluster.ProduceRouter.MessageExpired +=
                (t, m) =>
                    MessageExpired(new RawKafkaRecord
                    {
                        Key = m.Key,
                        Value = m.Value,
                        Topic = t,
                        Partition = Partitions.None,
                        Offset = 0
                    });
            ExpiredMessages = Observable.FromEvent<RawKafkaRecord>(a => MessageExpired += a, a => MessageExpired -= a);
            _cluster.ProduceRouter.MessageDiscarded +=
                (t, m) =>
                    MessageDiscarded(new RawKafkaRecord
                    {
                        Key = m.Key,
                        Value = m.Value,
                        Topic = t,
                        Partition = Partitions.None,
                        Offset = 0
                    });
            DiscardedMessages = Observable.FromEvent<RawKafkaRecord>(a => MessageDiscarded += a, a => MessageDiscarded -= a);
            _cluster.Start();
        }

        public IStatistics Statistics
        {
            get { return _cluster.Statistics; }
        }

        public void ConsumeFromEarliest(string topic)
        {
            _cluster.ConsumeRouter.StartConsume(topic, Partitions.All, Offsets.Earliest);
        }

        public void ConsumeFromLatest(string topic)
        {
            _cluster.ConsumeRouter.StartConsume(topic, Partitions.All, Offsets.Latest);
        }

        public void ConsumeFromEarliest(string topic, int partition)
        {
            if (partition < Partitions.All)
                throw new ArgumentException("Ivalid partition Id", "partition");
            _cluster.ConsumeRouter.StartConsume(topic, partition, Offsets.Earliest);
        }

        public void ConsumeFromLatest(string topic, int partition)
        {
            if (partition < Partitions.All)
                throw new ArgumentException("Ivalid partition Id", "partition");
            _cluster.ConsumeRouter.StartConsume(topic, partition, Offsets.Latest);
        }

        public void Consume(string topic, int partition, long offset)
        {
            if (partition < Partitions.All)
                throw new ArgumentException("Ivalid partition Id", "partition");
            if (offset < Offsets.Earliest)
                throw new ArgumentException("Invalid offset", "offset");
            _cluster.ConsumeRouter.StartConsume(topic, partition, offset);
        }

        public void StopConsume(string topic)
        {
            _cluster.ConsumeRouter.StopConsume(topic, Partitions.All, Offsets.Now);
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

        private long _sent;

        public void Produce(string topic, object data)
        {
            Produce(topic, null, data);
        }

        public void Produce(string topic, object key, object data)
        {
            Produce(topic, key, data, Partitions.Any);
        }

        public void Produce(string topic, object key, object data, int partition)
        {
            if (_configuration.MaxBufferedMessages > 0)
            {
                if (_sent - _cluster.PassedThrough >= _configuration.MaxBufferedMessages)
                {
                    SpinWait.SpinUntil(() => _sent - _cluster.PassedThrough < _configuration.MaxBufferedMessages);
                }
                Interlocked.Increment(ref _sent);
            }
            _cluster.ProduceRouter.Route(topic, new Message {Key = key, Value = data}, partition, DateTime.UtcNow.Add(_configuration.MessageTtl));
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
