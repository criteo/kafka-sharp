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
    /// Acknowledgement class for produce requests. Produce requests
    /// are batched by the nodes, so this is an acknowlegement for
    /// multiple messages. It contains both the response and the original batch.
    /// It is stamped with the date of reception (or default(DateTime) if
    /// we never sent the batch).
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

    /// <summary>
    /// Interface to the producer router, which routes messages to available nodes.
    /// </summary>
    interface IProduceRouter
    {
        /// <summary>
        /// Provides a new routing table.
        /// </summary>
        /// <param name="table">The new table.</param>
        void ChangeRoutingTable(RoutingTable table);

        /// <summary>
        /// Route a new message to Kafka brokers.
        /// </summary>
        /// <param name="topic">Message topic.</param>
        /// <param name="message">Message Key/Body.</param>
        /// <param name="expirationDate">A date after which the message will not be tried
        /// again in case of errors</param>
        void Route(string topic, Message message, DateTime expirationDate);

        /// <summary>
        /// Route an already created ProduceMessage.
        /// </summary>
        /// <param name="produceMessage"></param>
        void Route(ProduceMessage produceMessage);

        /// <summary>
        /// Acknowledge a response from a Kafka broker.
        /// </summary>
        /// <param name="acknowledgement"></param>
        void Acknowledge(ProduceAcknowledgement acknowledgement);

        /// <summary>
        /// Stop the producer router. Should try to flush all currently pending
        /// messages.
        /// </summary>
        /// <returns></returns>
        Task Stop();

        /// <summary>
        /// Raised when a message has been successfully routed to a node.
        /// </summary>
        event Action<string /* topic */> MessageRouted;

        /// <summary>
        /// Raised when a message has expired and we discarded it.
        /// </summary>
        event Action<string /* topic */> MessageExpired;

        /// <summary>
        /// Raised when a bunch of messages have been discarded due to errors (but not expired).
        /// </summary>
        event Action<string /* topic */, int /* number */> MessagesDiscarded;

        /// <summary>
        /// Raised when a bunch of messages has been successfully acknowledged.
        /// </summary>
        event Action<string /* topic */, int /* number */> MessagesAcknowledged;
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
            /// <summary>
            /// New partitioners have been provided.
            /// Note: partitioner change feature is not exposed yet by the public API.
            /// </summary>
            PartitionerEvent,

            /// <summary>
            /// A produce message is required to be sent or an
            /// acknowledgement has been received. We want to prioritize
            /// acknowledgement treatment over produce routing, so for
            /// implementation reasons we only use one enum slot to handle
            /// the two cases (because ActionBlock does not support priorities).
            /// </summary>
            ProduceEvent,

            /// <summary>
            /// Check for metadata and if postponed messages can be sent again.
            /// This is used by the timer that is set up when messages
            /// are postponed.
            /// </summary>
            CheckPostponed,

            /// <summary>
            /// Check is postponed events can be sent again after the routing
            /// table has been refreshed.
            /// </summary>
            CheckPostponedFollowingRoutingTableChange
        }

        /// <summary>
        /// This is used to describe a partitioner change.
        /// </summary>
        struct PartitionerMessage
        {
            /// <summary>
            /// Topic associated to the new partitioner,
            /// or null to describe a change on multiple topics.
            /// </summary>
            public string Topic;

            /// <summary>
            /// If Topic is not null this is assumed to be a IPartitioner.
            /// If Topic is null this is assumed to be a Dictionary&lt;string, IPartitioner&gt;
            /// </summary>
            public object PartitionerInfo;
        }

        private readonly ICluster _cluster;
        private readonly Configuration _configuration;

        private RoutingTable _routingTable = new RoutingTable(new Dictionary<string, Partition[]>());
        private Partitioners _partitioners = new Partitioners();

        // The queue of produce messages waiting to be routed
        private readonly ConcurrentQueue<ProduceMessage> _produceMessages = new ConcurrentQueue<ProduceMessage>();

        // The queue of received acknowledgements
        private readonly ConcurrentQueue<ProduceAcknowledgement> _produceResponses = new ConcurrentQueue<ProduceAcknowledgement>();

        // The queue of partitioner changes waiting to be processed
        private readonly ConcurrentQueue<PartitionerMessage> _partitionerMessages = new ConcurrentQueue<PartitionerMessage>();

        // The actor used to orchestrate all processing
        private readonly ActionBlock<RouterMessageType> _messages;

        // Postponed messages, stored by topics
        private readonly Dictionary<string, Queue<ProduceMessage>> _postponedMessages = new Dictionary<string, Queue<ProduceMessage>>();

        // Active when there are some postponed messages, it checks regurlarly
        // if they can be sent again
        private Timer _checkPostponedMessages;

        #region events from IProducerRouter

        public event Action<string> MessageRouted = _ => { };
        public event Action<string> MessageExpired = _ => { };
        public event Action<string, int> MessagesDiscarded = (t, c) => { };
        public event Action<string, int> MessagesAcknowledged = (t, c) => { };

        #endregion

        /// <summary>
        /// Raised when a message has been enqueued for routing.
        /// </summary>
        public event Action<string> MessageEnqueued = _ => { };

        /// <summary>
        /// Raised when a message is reenqueued following a recoverable error.
        /// </summary>
        public event Action<string> MessageReEnqueued = _ => { };

        /// <summary>
        /// Raised when a message is postponed following a recoverable error.
        /// </summary>
        public event Action<string> MessagePostponed = _ => { };

        /// <summary>
        /// Raised when we asked for new metadata.
        /// </summary>
        public event Action RoutingTableRequired = () => { };

        /// <summary>
        /// Create a ProducerRouter with the given cluster and configuration.
        /// Will start the internal actor.
        /// </summary>
        /// <param name="cluster"></param>
        /// <param name="configuration"></param>
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

        /// <summary>
        /// Signal a routing table change. We do this using an Interlocked
        /// operation to avoid having to manage yet another prioritized message
        /// in the actor loop. Will also initiate a check of postponed messages.
        /// </summary>
        /// <param name="table"></param>
        public void ChangeRoutingTable(RoutingTable table)
        {
            Interlocked.Exchange(ref _routingTable, table);
            _messages.Post(RouterMessageType.CheckPostponedFollowingRoutingTableChange);
        }

        /// <summary>
        /// Prepare a new message for routing.
        /// </summary>
        /// <param name="topic"></param>
        /// <param name="message"></param>
        /// <param name="expirationDate"></param>
        public void Route(string topic, Message message,  DateTime expirationDate)
        {
            Route(ProduceMessage.New(topic, message, expirationDate));
        }

        /// <summary>
        /// Prepare a message for routing. If we're not in Stop state
        /// the MessageEnqueued event will be raised.
        /// </summary>
        /// <param name="message"></param>
        public void Route(ProduceMessage message)
        {
            if (Post(message))
            {
                MessageEnqueued(message.Topic);
            }
        }

        /// <summary>
        /// Reenqueue a message. Check for expiration date and raise MessageExpired if needed.
        /// Raise the MessageReEnqueued if message effectively reenqueued.
        /// </summary>
        /// <param name="message"></param>
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

        /// <summary>
        /// Inner Post produce implementation. Post a ProduceEvent message
        /// to the inner actor.
        /// </summary>
        /// <param name="produceMessage"></param>
        /// <returns>True if effectiveley posted, false otherwise</returns>
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

        /// <summary>
        /// Accept an acknowledgement request. Post a ProduceEvent message
        /// to the inner actor.
        /// </summary>
        /// <param name="acknowledgement"></param>
        public void Acknowledge(ProduceAcknowledgement acknowledgement)
        {
            _produceResponses.Enqueue(acknowledgement);
            _messages.Post(RouterMessageType.ProduceEvent);
        }

        /// <summary>
        /// Post a partitioner change event for multiple topics.
        /// </summary>
        /// <param name="partitioners"></param>
        public void SetPartitioners(Partitioners partitioners)
        {
            if (partitioners == null)
                throw new ArgumentNullException("partitioners");

            Post(new PartitionerMessage {PartitionerInfo = partitioners});
        }

        /// <summary>
        /// Post a partitioner change event for a single topic.
        /// </summary>
        /// <param name="topic"></param>
        /// <param name="partitioner"></param>
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

        /// <summary>
        /// Request a metadata refresh from the cluster.
        /// </summary>
        /// <returns></returns>
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
        /// Actor loop. In case of produce events we prioritize the acknowledgements.
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

                    // Refresh metadata, then check posponed messages
                case RouterMessageType.CheckPostponed:
                    await EnsureHasRoutingTable();
                    goto case RouterMessageType.CheckPostponedFollowingRoutingTableChange;

                case RouterMessageType.CheckPostponedFollowingRoutingTableChange:
                    HandlePostponedMessages();
                    break;
            }
        }

        // Simply swap partitioners. No need to check the type casts, it has
        // already been taken care of by strictly typed SetPartitioner(s) methods.
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

        /// <summary>
        /// Handle a produce message:
        ///  - check if expired,
        ///  - check if the topic is currenlty postponed, in which case we postpone the message,
        ///  - get the partitioner, using the default one if needed,
        ///  - if no partition is available for the topic we refresh metadata once,
        ///  - if still no partition are available we postpone the message else we route it.
        ///
        /// The MessageRouted event is raised in case of successful routing.
        /// </summary>
        /// <param name="produceMessage"></param>
        /// <returns></returns>
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

        /// <summary>
        /// Handles an acknowlegement in the case there is no associated ProduceResponse.
        /// Messages are reenqueued if they were never sent in the first place or if we
        /// are in Retry mode. They're discarded otherwise, in which case the MessageDiscarded
        /// event is raised.
        /// </summary>
        /// <param name="acknowledgement"></param>
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

        // temp variables used in the following method, avoid reallocating them each time
        private readonly Dictionary<string, HashSet<int>> _tmpPartitionsInError = new Dictionary<string, HashSet<int>>();
        private readonly Dictionary<string, HashSet<int>> _tmpPartitionsInRecoverableError = new Dictionary<string, HashSet<int>>();

        private static readonly HashSet<int> NullHash = new HashSet<int>(); // A sentinel object

        /// <summary>
        /// Handle an acknowledgement with a ProduceResponse. Essentially search for errors and discard/retry
        /// accordingly. The logic is rather painful and boring. We try to be as efficient as possible
        /// in the standard case (i.e. no error).
        /// </summary>
        /// <param name="acknowledgement"></param>
        /// <returns></returns>
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
                    MessagesAcknowledged(grouping.Key, sent);
                }
                if (discarded > 0)
                {
                    MessagesDiscarded(grouping.Key, discarded);
                }
            }
        }

        private int _numberOfPostponedMessages; // The current number of postponed messages

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

        /// <summary>
        /// Postpone a message for later check and start the check timer if needed.
        /// The MessagePostponed event is raised.
        /// </summary>
        /// <param name="produceMessage"></param>
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

        // Raise the MessageExpired event and release a message.
        private void OnMessageExpired(ProduceMessage message)
        {
            MessageExpired(message.Topic);
            ProduceMessage.Release(message);
        }
    }
}
