// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Threading.Tasks;

namespace Kafka.Public
{
    /// <summary>
    /// An interface to a typed Kafka consumer. It is typically attached to a given topic.
    /// </summary>
    /// <typeparam name="TKey">The type of the messages keys.</typeparam>
    /// <typeparam name="TValue">The type of the values in messages.</typeparam>
    public interface IKafkaConsumer<TKey, TValue> : IDisposable
        where TKey : class
        where TValue : class
    {
        /// <summary>
        /// Messages received from the brokers.
        /// </summary>
        event Action<KafkaRecord<TKey, TValue>> MessageReceived;

        /// <summary>
        /// Signaled when partitions have been assigned by group coordinator
        /// </summary>
        event Action<IDictionary<string, ISet<int>>> PartitionsAssigned;

        /// <summary>
        /// Signaled when partitions have been revoked
        /// </summary>
        event Action PartitionsRevoked;

        /// <summary>
        /// Raised when a fetch request has been throttled server side.
        /// </summary>
        event Action<int> Throttled;

        /// <summary>
        /// The stream of received messages. Use this if you prefer using Reactive Extensions
        /// to manipulate streams of messages.
        /// </summary>
        IObservable<KafkaRecord<TKey, TValue>> Messages { get; }

        /// <summary>
        /// Consume from all partitions, and starting from the oldest available message.
        /// </summary>
        void ConsumeFromEarliest();

        /// <summary>
        /// Consume messages from all partitions, starting from new messages.
        /// </summary>
        void ConsumeFromLatest();

        /// <summary>
        /// Consume messages from the given partition,  starting from
        /// the oldest available message.
        /// </summary>
        /// <param name="partition"></param>
        void ConsumeFromEarliest(int partition);

        /// <summary>
        /// Consume messages from the given partition,  starting from
        /// new messages.
        /// </summary>
        /// <param name="partition"></param>
        void ConsumeFromLatest(int partition);

        /// <summary>
        /// Consume messages from the given partition, starting from the given offset.
        /// </summary>
        /// <param name="partition"></param>
        /// <param name="offset"></param>
        void Consume(int partition, long offset);

        /// <summary>
        /// Stop consuming messages effective immediately.
        /// </summary>
        void StopConsume();

        /// <summary>
        /// Stop consuming messages from the given partition, effective immediately.
        /// </summary>
        /// <param name="partition"></param>
        void StopConsume(int partition);

        /// <summary>
        /// Stop consuming messages from the given partition once the given offset
        /// has been reached.
        /// </summary>
        /// <param name="partition"></param>
        /// <param name="offset"></param>
        void StopConsume(int partition, long offset);

        /// <summary>
        /// Subscribe to a list of topics, using a given consumer group id.
        /// </summary>
        /// <param name="consumerGroupId"></param>
        /// <param name="topics"></param>
        /// <param name="configuration"></param>
        void Subscribe(string consumerGroupId, IEnumerable<string> topics, ConsumerGroupConfiguration configuration);

        /// <summary>
        /// Stop consuming messages from the given topic, effective immediately.
        /// This is the same as 'StopConsume'.
        /// </summary>
        /// <param name="topic"></param>
        void Pause(string topic);

        /// <summary>
        /// Restart consuming from the given topic, starting from where it was stopped.
        /// The topic must have been started first.
        /// </summary>
        /// <param name="topic"></param>
        void Resume(string topic);

        /// <summary>
        /// Commit offsets if linked to a consumer group.
        /// This is a fire and forget commit: the consumer will commit offsets at the next
        /// possible occasion. In particular if this is fired from inside a MessageReceived handler, commit
        /// will occur just after returning from the handler.
        /// Offsets commited are all the the offsets of the messages that have been sent through MessageReceived.
        /// </summary>
        void RequireCommit();

        /// <summary>
        /// Commit the given offset for the given offset/partition. This is a fined grained
        /// asynchronous commit. When this method async returns, given offsets will have
        /// effectively been committed.
        /// </summary>
        /// <param name="topic"></param>
        /// <param name="partition"></param>
        /// <param name="offset"></param>
        /// <returns></returns>
        Task CommitAsync(string topic, int partition, long offset);
    }

    /// <summary>
    /// A typed Kafka consumer. Serializers must have been configured in the underlying cluster client.
    /// Only one instance per Topic/TKey/TValue may be created.
    /// </summary>
    /// <typeparam name="TKey">Messages keys type</typeparam>
    /// <typeparam name="TValue">Messages values type</typeparam>
    public sealed class KafkaConsumer<TKey, TValue> : IKafkaConsumer<TKey, TValue>
        where TKey : class
        where TValue : class
    {
        private static readonly ConcurrentDictionary<string, KafkaConsumer<TKey, TValue>> Consumers = new ConcurrentDictionary<string, KafkaConsumer<TKey, TValue>>();

        private readonly IClusterClient _clusterClient;
        private readonly string _topic;
        private readonly Subject<KafkaRecord<TKey, TValue>> _messages = new Subject<KafkaRecord<TKey, TValue>>();
        private readonly IDisposable _messagesSub;

        private bool CheckRecord(RawKafkaRecord kr)
        {
            return kr.Topic == _topic && (kr.Key is TKey || kr.Key == null) && kr.Value is TValue;
        }

        private static KafkaRecord<TKey, TValue> ToRecord(RawKafkaRecord kr)
        {
            return new KafkaRecord<TKey, TValue> { Record = kr };
        }

        public KafkaConsumer(string topic, IClusterClient clusterClient)
        {
            if (string.IsNullOrEmpty(topic))
            {
                throw new ArgumentException("Topic cannot be null nor empty", "topic");
            }

            if (clusterClient == null)
            {
                throw new ArgumentNullException("clusterClient");
            }

            if (!Consumers.TryAdd(topic, this))
            {
                throw new ArgumentException(string.Format("A KafkaConsumer already exists for [Topic: {0} TKey: {1} TValue: {2}]", topic, typeof(TKey).Name, typeof(TValue).Name));
            }

            _topic = topic;
            _clusterClient = clusterClient;

            _clusterClient.MessageReceived += OnClusterMessage;
            _clusterClient.ConsumeThrottled += t => Throttled(t);
            _clusterClient.PartitionsAssigned += OnPartitionsAssigned;
            _clusterClient.PartitionsRevoked += OnPartitionsRevoked;
            _messagesSub = _clusterClient.Messages.Where(CheckRecord).Select(ToRecord).Subscribe(_messages.OnNext);
        }

        public event Action<KafkaRecord<TKey, TValue>> MessageReceived = _ => { };
        public event Action<int> Throttled = _ => { };

        public event Action<IDictionary<string, ISet<int>>> PartitionsAssigned = x => { };

        public event Action PartitionsRevoked = () => { };

        public IObservable<KafkaRecord<TKey, TValue>> Messages
        {
            get { return _messages; }
        }

        public void ConsumeFromEarliest()
        {
            if (_disposed)
                return;
            _clusterClient.ConsumeFromEarliest(_topic);
        }

        public void ConsumeFromLatest()
        {
            if (_disposed)
                return;
            _clusterClient.ConsumeFromLatest(_topic);
        }

        public void ConsumeFromEarliest(int partition)
        {
            if (_disposed)
                return;
            _clusterClient.ConsumeFromEarliest(_topic, partition);
        }

        public void ConsumeFromLatest(int partition)
        {
            if (_disposed)
                return;
            _clusterClient.ConsumeFromLatest(_topic, partition);
        }

        public void Consume(int partition, long offset)
        {
            if (_disposed)
                return;
            _clusterClient.Consume(_topic, partition, offset);
        }

        public void StopConsume()
        {
            if (_disposed)
                return;
            _clusterClient.StopConsume(_topic);
        }

        public void StopConsume(int partition)
        {
            if (_disposed)
                return;
            _clusterClient.StopConsume(_topic, partition);
        }

        public void StopConsume(int partition, long offset)
        {
            if (_disposed)
                return;
            _clusterClient.StopConsume(_topic, partition, offset);
        }

        public void Subscribe(string consumerGroupId, IEnumerable<string> topics, ConsumerGroupConfiguration configuration)
        {
            if (_disposed)
                return;
            _clusterClient.Subscribe(consumerGroupId, topics, configuration);
        }

        public void Pause(string topic)
        {
            if (_disposed)
                return;
            _clusterClient.Pause(topic);
        }

        public void Resume(string topic)
        {
            if (_disposed)
                return;
            _clusterClient.Resume(topic);
        }

        public void RequireCommit()
        {
            if (_disposed)
                return;
            _clusterClient.RequireCommit();
        }

        public Task CommitAsync(string topic, int partition, long offset)
        {
            if (_disposed)
            {
                var tc = new TaskCompletionSource<bool>();
                tc.SetCanceled();
                return tc.Task;
            }
            return _clusterClient.CommitAsync(topic, partition, offset);
        }

        private void OnClusterMessage(RawKafkaRecord kr)
        {
            if (CheckRecord(kr))
                MessageReceived(ToRecord(kr));
        }

        private void OnPartitionsAssigned(IDictionary<string, ISet<int>> assignedPartitions)
        {
            PartitionsAssigned.Invoke(assignedPartitions);
        }

        private void OnPartitionsRevoked()
        {
            PartitionsRevoked.Invoke();
        }

        private bool _disposed;

        public void Dispose()
        {
            if (_disposed)
                return;

            try
            {
                if (_clusterClient != null)
                {
                    _clusterClient.StopConsume(_topic);
                    _clusterClient.MessageReceived -= OnClusterMessage;
                    _clusterClient.PartitionsRevoked -= OnPartitionsRevoked;
                    _clusterClient.PartitionsAssigned -= OnPartitionsAssigned;
                }

                if (_messagesSub != null) _messagesSub.Dispose();
                _messages.OnCompleted();

                if (_topic != null)
                {
                    KafkaConsumer<TKey, TValue> dummy;
                    Consumers.TryRemove(_topic, out dummy);
                }
            }
            finally
            {
                _disposed = true;
            }
        }
    }
}