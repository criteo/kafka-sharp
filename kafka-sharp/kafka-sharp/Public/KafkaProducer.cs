// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

using System;
using System.Collections.Concurrent;
using System.Reactive.Linq;
using System.Reactive.Subjects;

namespace Kafka.Public
{
    /// <summary>
    /// An interface to a typed Kafka producer. It is typically attached to a given topic.
    /// </summary>
    /// <typeparam name="TKey">The type of the messages keys.</typeparam>
    /// <typeparam name="TValue">The type of the values in messages.</typeparam>
    public interface IKafkaProducer<TKey, TValue> : IDisposable
        where TKey : class
        where TValue : class
    {
        /// <summary>
        /// Send a value to a Kafka cluster.
        /// If kafka compatibility mode is set to 0.10+, the timestamp will
        /// be set to Now().
        /// </summary>
        /// <param name="data">Message value</param>
        bool Produce(TValue data);

        /// <summary>
        /// Send a value to a Kafka cluster and a given timestamp.
        /// The timestamp is only used in Kafka compatibility mode 0.10+
        /// </summary>
        /// <param name="data">Message value</param>
        bool Produce(TValue data, DateTime timestamp);

        /// <summary>
        /// Send a value to a Kafka Cluster, using the given key.
        /// If kafka compatibility mode is set to 0.10+, the timestamp will
        /// be set to Now().
        /// </summary>
        /// <param name="key">Message key</param>
        /// <param name="data">Message value</param>
        bool Produce(TKey key, TValue data);

        /// <summary>
        /// Send a value to a Kafka Cluster, using the given key and a given timestamp.
        /// The timestamp is only used in Kafka compatibility mode 0.10+
        /// </summary>
        /// <param name="key">Message key</param>
        /// <param name="data">Message value</param>
        bool Produce(TKey key, TValue data, DateTime timestamp);

        /// <summary>
        /// Send an array of bytes to a Kafka Cluster, using the given key.
        /// The message will routed to the target partition. This allows
        /// clients to partition data according to a specific scheme.
        /// If kafka compatibility mode is set to 0.10+, the timestamp will
        /// be set to Now().
        /// </summary>
        /// <param name="key">Message key</param>
        /// <param name="data">Message value</param>
        /// <param name="partition">Target partition</param>
        bool Produce(TKey key, TValue data, int partition);

        /// <summary>
        /// Send an array of bytes to a Kafka Cluster, using the given key and a given timestamp.
        /// The timestamp is only used in Kafka compatibility mode 0.10+
        /// The message will routed to the target partition. This allows
        /// clients to partition data according to a specific scheme.
        /// </summary>
        /// <param name="key">Message key</param>
        /// <param name="data">Message value</param>
        /// <param name="partition">Target partition</param>
        bool Produce(TKey key, TValue data, int partition, DateTime timestamp);

        /// <summary>
        /// This is raised when a produce message has expired.
        /// </summary>
        event Action<KafkaRecord<TKey, TValue>> MessageExpired;

        /// <summary>
        /// Rx observable version of the MessageExpired event.
        /// </summary>
        IObservable<KafkaRecord<TKey, TValue>> ExpiredMessages { get; }

        /// <summary>
        /// This is raised when a produce message is discarded due
        /// to a network error or some other Kafka unrecoverable error.
        /// </summary>
        event Action<KafkaRecord<TKey, TValue>> MessageDiscarded;

        /// <summary>
        /// Rx observable version of the MessageDiscarded event.
        /// </summary>
        IObservable<KafkaRecord<TKey, TValue>> DiscardedMessages { get; }

        /// <summary>
        /// This is raised when a bunch of messages has been successfully
        /// acknowledged for the producer topic. If you set the acknowledgement
        /// strategy to none, it is never raised.
        /// </summary>
        event Action<int> Acknowledged;

        /// <summary>
        /// Raised when some produce request has been throttled server side.
        /// </summary>
        event Action<int> Throttled;
    }

    /// <summary>
    /// A typed Kafka producer. You can only instanciate one KafkaProducer
    /// per topic/TKey/TValue. You should not forget to setup serializers
    /// in the underlying ClusterClient.
    /// </summary>
    /// <typeparam name="TKey">Messages keys type</typeparam>
    /// <typeparam name="TValue">Messages values type</typeparam>
    public sealed class KafkaProducer<TKey, TValue> : IKafkaProducer<TKey, TValue>
        where TKey : class
        where TValue : class
    {
        private static readonly ConcurrentDictionary<string, KafkaProducer<TKey, TValue>> Producers =
            new ConcurrentDictionary<string, KafkaProducer<TKey, TValue>>();

        private readonly IClusterClient _clusterClient;
        private readonly string _topic;
        private readonly Subject<KafkaRecord<TKey, TValue>> _discarded = new Subject<KafkaRecord<TKey, TValue>>();
        private readonly Subject<KafkaRecord<TKey, TValue>> _expired = new Subject<KafkaRecord<TKey, TValue>>();
        private readonly IDisposable _discardedSub;
        private readonly IDisposable _expiredSub;

        private bool CheckRecord(RawKafkaRecord kr)
        {
            return kr.Topic == _topic && (kr.Key is TKey || kr.Key == null) && kr.Value is TValue;
        }

        private static KafkaRecord<TKey, TValue> ToRecord(RawKafkaRecord kr)
        {
            return new KafkaRecord<TKey, TValue> { Record = kr };
        }

        public KafkaProducer(string topic, IClusterClient clusterClient)
        {
            if (string.IsNullOrEmpty(topic))
            {
                throw new ArgumentException("Topic cannot be null nor empty", "topic");
            }

            if (clusterClient == null)
            {
                throw new ArgumentNullException("clusterClient");
            }

            if (!Producers.TryAdd(topic, this))
            {
                throw new ArgumentException(
                    string.Format("A KafkaProducer already exists for [Topic: {0} TKey: {1} TValue: {2}]", topic,
                        typeof(TKey).Name, typeof(TValue).Name));
            }

            _clusterClient = clusterClient;
            _topic = topic;

            _clusterClient.MessageDiscarded += OnClusterMessageDiscarded;
            _clusterClient.MessageExpired += OnClusterMessageExpired;
            _clusterClient.ProduceAcknowledged += (t, n) =>
            {
                if (t == topic)
                {
                    Acknowledged(n);
                }
            };
            _clusterClient.ProduceThrottled += t => Throttled(t);

            _discardedSub =
                _clusterClient.DiscardedMessages.Where(CheckRecord).Select(ToRecord).Subscribe(_discarded.OnNext);
            _expiredSub = _clusterClient.ExpiredMessages.Where(CheckRecord).Select(ToRecord).Subscribe(_expired.OnNext);
        }

        public bool Produce(TValue data)
        {
            return Produce(null, data);
        }

        public bool Produce(TValue data, DateTime timestamp)
        {
            return Produce(null, data, timestamp);
        }

        public bool Produce(TKey key, TValue data)
        {
            return Produce(key, data, Partitions.Any);
        }

        public bool Produce(TKey key, TValue data, DateTime timestamp)
        {
            return Produce(key, data, Partitions.Any, timestamp);
        }

        public bool Produce(TKey key, TValue data, int partition)
        {
            return !_disposed && _clusterClient.Produce(_topic, key, data, partition);
        }

        public bool Produce(TKey key, TValue data, int partition, DateTime timestamp)
        {
            return !_disposed && _clusterClient.Produce(_topic, key, data, partition, timestamp);
        }

        public event Action<KafkaRecord<TKey, TValue>> MessageExpired = _ => { };

        private void OnClusterMessageExpired(RawKafkaRecord kr)
        {
            if (CheckRecord(kr))
                MessageExpired(ToRecord(kr));
        }

        public IObservable<KafkaRecord<TKey, TValue>> ExpiredMessages
        {
            get { return _expired; }
        }

        public event Action<KafkaRecord<TKey, TValue>> MessageDiscarded = _ => { };

        private void OnClusterMessageDiscarded(RawKafkaRecord kr)
        {
            if (CheckRecord(kr))
                MessageDiscarded(ToRecord(kr));
        }

        public IObservable<KafkaRecord<TKey, TValue>> DiscardedMessages
        {
            get { return _discarded; }
        }

        public event Action<int> Acknowledged = _ => { };
        public event Action<int> Throttled = _ => { };

        private bool _disposed;

        public void Dispose()
        {
            if (_disposed)
                return;

            try
            {
                if (_discardedSub != null ) _discardedSub.Dispose();
                _discarded.OnCompleted();

                if ( _expiredSub != null ) _expiredSub.Dispose();
                _expired.OnCompleted();

                if (_clusterClient != null)
                {
                    _clusterClient.MessageDiscarded -= OnClusterMessageDiscarded;
                    _clusterClient.MessageExpired -= OnClusterMessageExpired;
                }

                if (_topic != null)
                {
                    KafkaProducer<TKey, TValue> dummy;
                    Producers.TryRemove(_topic, out dummy);
                }
            }
            finally
            {
                _disposed = true;
            }
        }
    }
}