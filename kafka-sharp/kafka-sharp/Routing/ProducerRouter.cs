// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. 
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using Kafka.Cluster;
using Kafka.Protocol;
using Kafka.Public;
using ICluster = Kafka.Cluster.ICluster;

namespace Kafka.Routing
{
    using Partitioners = Dictionary<string, IPartitioner>;

    /// <summary>
    /// Acknowledgement class for produce request. Produce requests
    /// are batched by the nodes, so this is an acknowlegement for
    /// multiple messages.
    /// </summary>
    struct ProduceAcknowledgement
    {
        /// <summary>
        /// The response associated to the batch. An empty value means
        /// an error occured.
        /// </summary>
        public ProduceResponse ProduceResponse;

        /// <summary>
        /// The batch of messages that were tried to be sent.
        /// </summary>
        public IEnumerable<IGrouping<string, ProduceMessage>> OriginalBatch;

        /// <summary>
        /// The date time the acknowledgement was "received" by the node
        /// (if a network error occurred, this is the date of the error).
        /// If this is default(DateTime), it means the request was actually
        /// never sent (dead node probably).
        /// </summary>
        public DateTime ReceiveDate;
    }

    interface IProduceRouter
    {
        void ChangeRoutingTable(RoutingTable table);
        void Route(string topic, Message message, DateTime expirationDate);
        void Route(ProduceMessage produceMessage);
        void Acknowledge(ProduceAcknowledgement acknowledgement);
        Task Stop();
        event Action<string> MessageRouted;
        event Action<string> MessageExpired;
        event Action<string, int> MessagesDiscarded;
        event Action<string, int> MessagesSent;
    }

    /// <summary>
    /// Route messages to Kafka nodes according to topic partitioners (default
    /// is to round robin between nodes associated to a topic).
    /// All mechanics is handled by an actor modelized using ActionBlock class.
    /// Most modification of state is done within the actor loop.
    /// </summary>
    class ProduceRouter : IProduceRouter
    {
        /// <summary>
        /// Underlying actor supported messages.
        /// </summary>
        enum RouterMessageType
        {
            PartitionerEvent,
            ProduceEvent,
            CheckPostponed,
            CheckPostponedFollowingRoutingTableChange
        }

        class PartitionerMessage
        {
            public string Topic;
            public object PartitionerInfo;
        }

        private readonly ICluster _cluster;
        private readonly Configuration _configuration;

        private RoutingTable _routingTable = new RoutingTable(new Dictionary<string, Partition[]>());
        private Partitioners _partitioners = new Partitioners();

        private readonly ConcurrentQueue<ProduceMessage> _produceMessages = new ConcurrentQueue<ProduceMessage>();
        private readonly ConcurrentQueue<ProduceAcknowledgement> _produceResponses = new ConcurrentQueue<ProduceAcknowledgement>();
        private readonly ConcurrentQueue<PartitionerMessage> _partitionerMessages = new ConcurrentQueue<PartitionerMessage>();

        private readonly ActionBlock<RouterMessageType> _messages;
        private readonly Dictionary<string, Queue<ProduceMessage>> _postponedMessages = new Dictionary<string, Queue<ProduceMessage>>();
        private Timer _checkPostponedMessages;

        public event Action<string> MessageEnqueued = _ => { };
        public event Action<string> MessageReEnqueued = _ => { };
        public event Action<string> MessageExpired = _ => { };
        public event Action<string, int> MessagesDiscarded = (t, c) => { };
        public event Action<string, int> MessagesSent = (t, c) => { };
        public event Action<string> MessageRouted = _ => { };
        public event Action<string> MessagePostponed = _ => { };
        public event Action RoutingTableRequired = () => { };

        public ProduceRouter(ICluster cluster, Configuration configuration)
        {
            _cluster = cluster;
            _configuration = configuration;
            _messages = new ActionBlock<RouterMessageType>(m => ProcessMessage(m),
                                                           new ExecutionDataflowBlockOptions
                                                               {
                                                                   MaxMessagesPerTask = configuration.BatchSize,
                                                                   TaskScheduler = configuration.TaskScheduler
                                                               });
        }

        /// <summary>
        /// Flushes messages and stop accepting new ones.
        /// </summary>
        public async Task Stop()
        {
            _messages.Complete();
            await _messages.Completion;
        }

        public void ChangeRoutingTable(RoutingTable table)
        {
            Interlocked.Exchange(ref _routingTable, table);
            _messages.Post(RouterMessageType.CheckPostponedFollowingRoutingTableChange);
        }

        public void Route(string topic, Message message,  DateTime expirationDate)
        {
            Route(ProduceMessage.New(topic, message, expirationDate));
        }

        public void Route(ProduceMessage message)
        {
            if (Post(message))
            {
                MessageEnqueued(message.Topic);
            }
        }

        private void ReEnqueue(ProduceMessage message)
        {
            if (message.ExpirationDate < DateTime.UtcNow)
            {
                OnMessageExpired(message);
                return;
            }

            if (Post(message))
            {
                MessageReEnqueued(message.Topic);
            }
        }

        private bool Post(ProduceMessage produceMessage)
        {
            if (_messages.Completion.IsCompleted)
            {
                ProduceMessage.Release(produceMessage);
                return false;
            }
            _produceMessages.Enqueue(produceMessage);
            return _messages.Post(RouterMessageType.ProduceEvent);
        }

        public void Acknowledge(ProduceAcknowledgement acknowledgement)
        {
            _produceResponses.Enqueue(acknowledgement);
            _messages.Post(RouterMessageType.ProduceEvent);
        }

        public void SetPartitioners(Partitioners partitioners)
        {
            if (partitioners == null)
                throw new ArgumentNullException("partitioners");

            Post(new PartitionerMessage {PartitionerInfo = partitioners});
        }

        public void SetPartitioner(string topic, IPartitioner partitioner)
        {
            if (topic == null)
                throw new ArgumentNullException("topic");
            if (partitioner == null)
                throw new ArgumentNullException("partitioner");

            Post(new PartitionerMessage
                {
                    Topic = topic,
                    PartitionerInfo = partitioner
                });
        }

        private void Post(PartitionerMessage partitionerMessage)
        {
            if (_messages.Completion.IsCompleted)
            {
                return;
            }

            _partitionerMessages.Enqueue(partitionerMessage);
            _messages.Post(RouterMessageType.PartitionerEvent);
        }

        private async Task EnsureHasRoutingTable()
        {
            RoutingTableRequired();
            bool hasError = false;
            try
            {
                _routingTable = await _cluster.RequireNewRoutingTable();
            }
            catch
            {
                hasError = true;
            }
            if (hasError)
                await Task.Delay(1000);
        }

        /// <summary>
        /// Actor loop.
        /// </summary>
        private async Task ProcessMessage(RouterMessageType messageType)
        {
            switch (messageType)
            {
                case RouterMessageType.PartitionerEvent:
                    {
                        PartitionerMessage message;
                        if (!_partitionerMessages.TryDequeue(out message))
                        {
                            // Something is messed up, we stop
                            _messages.Complete();
                            return;
                        }
                        HandlePartitionerMessage(message);
                    }
                    break;

                    // In this case we prioritize reading produce responses
                    // to refresh metadata as soon as possible if needed.
                    // Unless batching is set to batches of one message there
                    // should be way less reponses than produce messages and
                    // this should not impact the speed of processing.
                case RouterMessageType.ProduceEvent:
                    {
                        ProduceAcknowledgement response;
                        if (_produceResponses.TryDequeue(out response))
                        {
                            if (response.ProduceResponse.TopicsResponse != null)
                            {
                                await HandleProduceAcknowledgement(response);
                            }
                            else
                            {
                                HandleProduceAcknowledgementNone(response);
                            }
                        }
                        else
                        {
                            ProduceMessage produce;
                            if (!_produceMessages.TryDequeue(out produce))
                            {
                                // Something is messed up, we stop
                                _messages.Complete();
                                return;
                            }
                            await HandleProduceMessage(produce);
                        }
                    }
                    break;

                case RouterMessageType.CheckPostponed:
                    await EnsureHasRoutingTable();
                    goto case RouterMessageType.CheckPostponedFollowingRoutingTableChange;

                case RouterMessageType.CheckPostponedFollowingRoutingTableChange:
                    HandlePostponedMessages();
                    break;
            }
        }

        private void HandlePartitionerMessage(PartitionerMessage message)
        {
            if (message.Topic != null)
            {
                _partitioners[message.Topic] = message.PartitionerInfo as IPartitioner;
            }
            else
            {
                _partitioners = message.PartitionerInfo as Partitioners;
            }
        }

        private async Task HandleProduceMessage(ProduceMessage produceMessage)
        {
            if (produceMessage.ExpirationDate < DateTime.UtcNow)
            {
                OnMessageExpired(produceMessage);
                return;
            }

            var topic = produceMessage.Topic;

            if (IsPostponedTopic(topic))
            {
                PostponeMessage(produceMessage);
                return;
            }

            IPartitioner partitioner;
            if (!_partitioners.TryGetValue(topic, out partitioner))
            {
                partitioner = new DefaultPartitioner();
                _partitioners[topic] = partitioner;
            }

            var partitions = _routingTable.GetPartitions(topic);
            if (partitions.Length == 0)
            {
                await EnsureHasRoutingTable();
                partitions = _routingTable.GetPartitions(topic);
            }

            var partition = partitioner.GetPartition(produceMessage.Message, partitions);

            if (partition == null)
            {
                // Messages for topics with no partition available are postponed.
                // They will be checked again when the routing table is updated.
                PostponeMessage(produceMessage);
                return;
            }

            produceMessage.Partition = partition.Id;
            if (partition.Leader.Produce(produceMessage))
            {
                MessageRouted(topic);
            }
            else
            {
                ReEnqueue(produceMessage);
            }
        }

        private void HandleProduceAcknowledgementNone(ProduceAcknowledgement acknowledgement)
        {
            if (_configuration.ErrorStrategy == ErrorStrategy.Retry || acknowledgement.ReceiveDate == default(DateTime))
            {
                // Repost
                foreach (var message in acknowledgement.OriginalBatch.SelectMany(g => g))
                {
                    ReEnqueue(message);
                }
            }
            else
            {
                // Discard
                foreach (var grouping in acknowledgement.OriginalBatch)
                {
                    MessagesDiscarded(grouping.Key, grouping.Count());
                    foreach (var message in grouping)
                    {
                        ProduceMessage.Release(message);
                    }
                }
            }
        }

        private readonly Dictionary<string, HashSet<int>> _tmpPartitionsInError = new Dictionary<string, HashSet<int>>();
        private readonly Dictionary<string, HashSet<int>> _tmpPartitionsInRecoverableError = new Dictionary<string, HashSet<int>>();

        private static readonly HashSet<int> NullHash = new HashSet<int>();

        private async Task HandleProduceAcknowledgement(ProduceAcknowledgement acknowledgement)
        {
            // The whole point of scanning the response is to search for errors
            var originalBatch = acknowledgement.OriginalBatch;
            var produceResponse = acknowledgement.ProduceResponse;

            // Fill partitions in error caches
            _tmpPartitionsInError.Clear();
            _tmpPartitionsInRecoverableError.Clear();
            foreach (var tr in produceResponse.TopicsResponse)
            {
                bool errors = false;
                foreach (var p in tr.Partitions.Where(p => !Error.IsPartitionOkForProducer(p.ErrorCode)))
                {
                    if (!errors)
                    {
                        errors = true;
                        _tmpPartitionsInError[tr.TopicName] = new HashSet<int>();
                        _tmpPartitionsInRecoverableError[tr.TopicName] = new HashSet<int>();
                    }

                    if (Error.IsPartitionErrorRecoverableForProducer(p.ErrorCode))
                    {
                        _tmpPartitionsInRecoverableError[tr.TopicName].Add(p.Partition);
                    }
                    else
                    {
                        _tmpPartitionsInError[tr.TopicName].Add(p.Partition);
                    }
                }
            }

            // In case of recoverable errors, refresh metadata if they are too old
            if (_tmpPartitionsInRecoverableError.Count > 0 && acknowledgement.ReceiveDate > _routingTable.LastRefreshed)
            {
                await EnsureHasRoutingTable();
            }

            // Scan messages for errors and release memory if needed.
            // Messages associated to recoverable errors are reenqueued for rerouting,
            // messages with irrecoverable errors are discarded and messages with
            // no error are marked sent.
            foreach (var grouping in originalBatch)
            {
                int sent = 0;
                int discarded = 0;
                HashSet<int> errPartitions;
                if (!_tmpPartitionsInError.TryGetValue(grouping.Key, out errPartitions))
                {
                    errPartitions = NullHash;
                }
                HashSet<int> recPartitions;
                if (!_tmpPartitionsInRecoverableError.TryGetValue(grouping.Key, out recPartitions))
                {
                    recPartitions = NullHash;
                }

                foreach (var pm in grouping)
                {
                    if (recPartitions.Contains(pm.Partition))
                    {
                        ReEnqueue(pm);
                    }
                    else
                    {
                        if (errPartitions.Contains(pm.Partition))
                        {
                            ++discarded;
                        }
                        else
                        {
                            ++sent;
                        }
                        ProduceMessage.Release(pm);
                    }
                }
                if (sent > 0)
                {
                    MessagesSent(grouping.Key, sent);
                }
                if (discarded > 0)
                {
                    MessagesDiscarded(grouping.Key, discarded);
                }
            }
        }

        private int _numberOfPostponedMessages;

        /// <summary>
        /// Check all postponed messages:
        /// Reenqueue those with at least one partition available,
        /// check the others for expiration.
        /// </summary>
        private void HandlePostponedMessages()
        {
            foreach (var kv in _postponedMessages)
            {
                var postponed = kv.Value;
                if (_routingTable.GetPartitions(kv.Key).Length > 0)
                {
                    while (postponed.Count > 0)
                    {
                        ReEnqueue(postponed.Dequeue());
                        --_numberOfPostponedMessages;
                    }
                }
                else
                {
                    while (postponed.Count > 0)
                    {
                        if (postponed.Peek().ExpirationDate >= DateTime.UtcNow)
                        {
                            // Messages are queued in order or close enough
                            // so just get out of the loop on the first non expired
                            // message encountered
                            break;
                        }
                        OnMessageExpired(postponed.Dequeue());
                        --_numberOfPostponedMessages;
                    }
                }
            }

            if (_numberOfPostponedMessages == 0 && _checkPostponedMessages != null)
            {
                _checkPostponedMessages.Dispose();
                _checkPostponedMessages = null;
            }
        }

        private bool IsPostponedTopic(string topic)
        {
            Queue<ProduceMessage> postponed;
            return _postponedMessages.TryGetValue(topic, out postponed) && postponed.Count > 0;
        }

        private void PostponeMessage(ProduceMessage produceMessage)
        {
            Queue<ProduceMessage> postponedQueue;
            if (!_postponedMessages.TryGetValue(produceMessage.Topic, out postponedQueue))
            {
                postponedQueue = new Queue<ProduceMessage>();
                _postponedMessages.Add(produceMessage.Topic, postponedQueue);
            }
            postponedQueue.Enqueue(produceMessage);
            ++_numberOfPostponedMessages;
            MessagePostponed(produceMessage.Topic);

            if (_checkPostponedMessages == null)
            {
                _checkPostponedMessages =
                    new Timer(_ => _messages.Post(RouterMessageType.CheckPostponed),
                              null, _configuration.MessageTtl, _configuration.MessageTtl);
            }
        }

        private void OnMessageExpired(ProduceMessage message)
        {
            MessageExpired(message.Topic);
            ProduceMessage.Release(message);
        }
    }
}
