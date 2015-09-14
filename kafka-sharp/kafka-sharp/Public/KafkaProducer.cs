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
        /// </summary>
        /// <param name="data">Message value</param>
        void Produce(TValue data);

        /// <summary>
        /// Send a value to a Kafka Cluster, using the given key.
        /// </summary>
        /// <param name="key">Message key</param>
        /// <param name="data">Message value</param>
        void Produce(TKey key, TValue data);

        /// <summary>
        /// Send an array of bytes to a Kafka Cluster, using the given key.
        /// The message will routed to the target partition. This allows
        /// clients to partition data according to a specific scheme.
        /// </summary>
        /// <param name="key">Message key</param>
        /// <param name="data">Message value</param>
        /// <param name="partition">Target partition</param>
        void Produce(TKey key, TValue data, int partition);

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
        private static readonly ConcurrentDictionary<string, KafkaProducer<TKey, TValue>> Producers = new ConcurrentDictionary<string, KafkaProducer<TKey, TValue>>();
        
        private readonly IClusterClient _clusterClient;
        private readonly string _topic;
        private readonly Subject<KafkaRecord<TKey, TValue>> _discarded = new Subject<KafkaRecord<TKey, TValue>>();
        private readonly Subject<KafkaRecord<TKey, TValue>> _expired = new Subject<KafkaRecord<TKey, TValue>>();
        private readonly IDisposable _discardedSub;
        private readonly IDisposable _expiredSub ;

        private bool CheckRecord(RawKafkaRecord kr)
        {
            return kr.Topic == _topic && kr.Key is TKey && kr.Value is TValue;
        }

        private static KafkaRecord<TKey, TValue> ToRecord(RawKafkaRecord kr)
        {
            return new KafkaRecord<TKey, TValue> {Record = kr};
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
                        typeof (TKey).Name, typeof (TValue).Name));
            }

            _clusterClient = clusterClient;
            _topic = topic;

            _clusterClient.MessageDiscarded += OnClusterMessageDiscarded;
            _clusterClient.MessageExpired += OnClusterMessageExpired;

            _discardedSub =
                _clusterClient.DiscardedMessages.Where(CheckRecord).Select(ToRecord).Subscribe(_discarded.OnNext);
            _expiredSub = _clusterClient.ExpiredMessages.Where(CheckRecord).Select(ToRecord).Subscribe(_expired.OnNext);
        }

        public void Produce(TValue data)
        {
            Produce(null, data);
        }

        public void Produce(TKey key, TValue data)
        {
            Produce(key, data, Partitions.Any);
        }

        public void Produce(TKey key, TValue data, int partition)
        {
            if (_disposed) return;
            _clusterClient.Produce(_topic, key, data, partition);
        }

        public event Action<KafkaRecord<TKey, TValue>> MessageExpired = _ => { };

        private void OnClusterMessageExpired(RawKafkaRecord kr)
        {
            if (CheckRecord(kr)) MessageExpired(ToRecord(kr));
        }

        public IObservable<KafkaRecord<TKey, TValue>> ExpiredMessages
        {
            get { return _expired; }
        }

        public event Action<KafkaRecord<TKey, TValue>> MessageDiscarded = _ => { };

        private void OnClusterMessageDiscarded(RawKafkaRecord kr)
        {
            if (CheckRecord(kr)) MessageDiscarded(ToRecord(kr));
        }

        public IObservable<KafkaRecord<TKey, TValue>> DiscardedMessages
        {
            get { return _discarded; }
        }

        private bool _disposed;

        ~KafkaProducer()
        {
            if (!_disposed)
            {
                Dispose();
            }
        }

        public void Dispose()
        {
            if (_disposed) return;

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
            _disposed = true;
            GC.SuppressFinalize(this);
        }
    }
}