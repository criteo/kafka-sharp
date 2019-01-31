// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using Kafka.Batching;
using Kafka.Cluster;
using Kafka.Common;
using Kafka.Protocol;
using Kafka.Public;
using Kafka.Routing.PartitionSelection;
using ICluster = Kafka.Cluster.ICluster;

namespace Kafka.Routing
{
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
        public IBatchByTopicByPartition<ProduceMessage> OriginalBatch;

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
        /// Route a new message to Kafka brokers. If target partition is Any
        /// we'll round robin through active partitions.
        /// </summary>
        /// <param name="topic">Message topic.</param>
        /// <param name="message">Message Key/Body.</param>
        /// <param name="partition">Target partition, can be Partitions.Any</param>
        /// <param name="expirationDate">A date after which the message will not be tried
        /// again in case of errors</param>
        void Route(string topic, Message message, int partition, DateTime expirationDate);

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
        event Action<string /* topic */, Message /* message */> MessageExpired;

        /// <summary>
        /// Raised when a bunch of messages have been discarded due to errors (but not expired).
        /// </summary>
        event Action<string /* topic */, Message /* message */> MessageDiscarded;

        /// <summary>
        /// Raised when a bunch of messages has been successfully acknowledged.
        /// </summary>
        event Action<string /* topic */, int /* number */> MessagesAcknowledged;

        /// <summary>
        /// Raised when a timeout error is returned by the broker for a partition
        /// </summary>
        event Action<string /* topic */> BrokerTimeoutError;

        /// <summary>
        /// Raised when a message is reenqueued following a recoverable error
        /// </summary>
        event Action<string /* topic */> MessageReEnqueued;

        /// <summary>
        /// Raised when a message is postponed following a recoverable error.
        /// </summary>
        event Action<string /* topic */> MessagePostponed;

        /// <summary>
        /// Raised when a produce request has been throttled by brokers.
        /// </summary>
        event Action<int> Throttled;
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

        #region Batching

        class ByNodeBatchingStrategy : IBatchStrategy<ProduceMessage>
        {
            public bool Send(INode node, ProduceMessage message)
            {
                return node.Produce(message);
            }

            public void Dispose()
            {
            }
        }

        class GlobalBatchingStrategy : IBatchStrategy<ProduceMessage>
        {
            private readonly AccumulatorByNodeByTopicByPartition<ProduceMessage> _accumulator;
            private Action<INode, IBatchByTopicByPartition<ProduceMessage>> _batch;

            public GlobalBatchingStrategy(Configuration configuration,
                Action<INode, IBatchByTopicByPartition<ProduceMessage>> batch)
            {
                _accumulator = new AccumulatorByNodeByTopicByPartition<ProduceMessage>(pm => pm.Topic,
                    pm => pm.Partition, configuration.ProduceBatchSize, configuration.ProduceBufferingTime);
                _accumulator.NewBatch += batch;
            }

            public bool Send(INode node, ProduceMessage message)
            {
                return _accumulator.Add(Tuple.Create(node, message));
            }

            public void Dispose()
            {
                _accumulator.Dispose();
                _accumulator.NewBatch -= _batch;
                _batch = null;
            }
        }

        #endregion

        private readonly ICluster _cluster;
        private readonly Configuration _configuration;
        private readonly Pool<ReusableMemoryStream> _pool;

        private RoutingTable _routingTable = new RoutingTable();

        private readonly Dictionary<string, PartitionSelector> _partitioners =
            new Dictionary<string, PartitionSelector>();

        private readonly Dictionary<string, Dictionary<int, DateTime>> _filters =
            new Dictionary<string, Dictionary<int, DateTime>>();

        private DateTime _filtersLastChecked;

        private readonly Random _randomGenerator = new Random(Guid.NewGuid().GetHashCode());

        // The queue of produce messages waiting to be routed
        private readonly ConcurrentQueue<ProduceMessage> _produceMessages = new ConcurrentQueue<ProduceMessage>();

        // The queue of received acknowledgements
        private readonly ConcurrentQueue<ProduceAcknowledgement> _produceResponses =
            new ConcurrentQueue<ProduceAcknowledgement>();

        // The actor used to orchestrate all processing
        private readonly ActionBlock<RouterMessageType> _messages;

        // Postponed messages, stored by topics
        private readonly Dictionary<string, Queue<ProduceMessage>> _postponedMessages =
            new Dictionary<string, Queue<ProduceMessage>>();

        // Active when there are some postponed messages, it checks regurlarly
        // if they can be sent again
        private Timer _checkPostponedMessages;

        // Accumulator when in global batching mode
        private readonly IBatchStrategy<ProduceMessage> _batchStrategy;

        #region events from IProducerRouter

        public event Action<string> MessageRouted = _ => { };
        public event Action<string, Message> MessageExpired = (t, m) => { };
        public event Action<string, Message> MessageDiscarded = (t, m) => { };
        public event Action<string, int> MessagesAcknowledged = (t, c) => { };
        public event Action<string> BrokerTimeoutError = _ => { };
        public event Action<string> MessageReEnqueued = _ => { };
        public event Action<string> MessagePostponed = _ => { };
        public event Action<int> Throttled = _ => { };

        #endregion

        /// <summary>
        /// Raised when a message has been enqueued for routing.
        /// </summary>
        public event Action<string> MessageEnqueued = _ => { };

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
        /// <param name="pool">The pool of message buffers (for when SerializeOnProduce is true)</param>
        public ProduceRouter(ICluster cluster, Configuration configuration, Pool<ReusableMemoryStream> pool)
        {
            _cluster = cluster;
            _configuration = configuration;
            _pool = pool;
            _messages = new ActionBlock<RouterMessageType>(m => ProcessMessage(m),
                                                           new ExecutionDataflowBlockOptions
                                                               {
                                                                   MaxMessagesPerTask = configuration.ProduceBatchSize,
                                                                   TaskScheduler = configuration.TaskScheduler
                                                               });
            if (configuration.BatchStrategy == BatchStrategy.Global)
            {
                _batchStrategy = new GlobalBatchingStrategy(
                    configuration,
                    (n, b) =>
                    {
                        if (n.Post(b)) return;
                        foreach (var pm in b.SelectMany(g => g).SelectMany(g => g))
                        {
                            Route(pm);
                        }
                    });
            }
            else
            {
                _batchStrategy = new ByNodeBatchingStrategy();
            }
        }

        /// <summary>
        /// Flushes messages and stop accepting new ones.
        /// </summary>
        public async Task Stop()
        {
            _messages.Complete();
            await _messages.Completion;
            _batchStrategy.Dispose();
        }

        /// <summary>
        /// Signal a routing table change. We do this using an Interlocked
        /// operation to avoid having to manage yet another prioritized message
        /// in the actor loop. Will also initiate a check of postponed messages.
        /// </summary>
        /// <param name="table"></param>
        public void ChangeRoutingTable(RoutingTable table)
        {
            if (_routingTable != null && _routingTable.LastRefreshed > table.LastRefreshed) return;
            if (_configuration.RequiredAcks == RequiredAcks.AllInSyncReplicas)
            {
                Interlocked.Exchange(ref _routingTable, new RoutingTable(table, _configuration.MinInSyncReplicas));
            }
            else
            {
                Interlocked.Exchange(ref _routingTable, new RoutingTable(table));
            }
            _messages.Post(RouterMessageType.CheckPostponedFollowingRoutingTableChange);
        }

        /// <summary>
        /// Prepare a new message for routing.
        /// </summary>
        /// <param name="topic"></param>
        /// <param name="message"></param>
        /// <param name="partition"></param>
        /// <param name="expirationDate"></param>
        public void Route(string topic, Message message, int partition, DateTime expirationDate)
        {
            if (_configuration.SerializationConfig.SerializeOnProduce)
            {
                var serializers = _configuration.SerializationConfig.GetSerializersForTopic(topic);
                message.SerializeKeyValue(_pool.Reserve(), serializers);
            }
            Route(ProduceMessage.New(topic, partition, message, expirationDate));
        }

        /// <summary>
        /// Prepare a message for routing. If we're not in Stop state
        /// the MessageEnqueued event will be raised.
        /// </summary>
        /// <param name="message"></param>
        private void Route(ProduceMessage message)
        {
            if (Post(message))
            {
                MessageEnqueued(message.Topic);
            }
            else
            {
                _cluster.Logger.LogError(
                    string.Format(
                        "[Producer] Failed to route message, discarding message for [topic: {0} / partition: {1}]",
                        message.Topic, message.Partition));
                OnMessageDiscarded(message);
            }
        }

        /// <summary>
        /// Prepare an existing message for routing.
        /// Check for expiration date and raise MessageExpired if needed.
        /// Raise the MessageReEnqueued if message effectively reenqueued.
        /// </summary>
        /// <param name="message"></param>
        private void ReEnqueueMessage(ProduceMessage message)
        {
            if (message.ExpirationDate < DateTime.UtcNow)
            {
                OnMessageExpired(message);
                return;
            }

            message.Partition = Partitions.None; // so that round robined msgs are re robined
            if (Post(message))
            {
                MessageReEnqueued(message.Topic);
            }
            else
            {
                _cluster.Logger.LogWarning(string.Format(
                    "[Producer] Failed to reenqueue message, discarding message for topic '{0}'", message.Topic));
                OnMessageDiscarded(message);
            }
        }

        /// <summary>
        /// Try to send again a message following an error.
        /// Check for retry if needed.
        /// </summary>
        internal void ReEnqueueAfterError(ProduceMessage message)
        {
            if (_configuration.MaxRetry < 0 || ++message.Retried <= _configuration.MaxRetry)
            {
                ReEnqueueMessage(message);
            }
            else
            {
                _cluster.Logger.LogError(string.Format(
                    "[Producer] Not able to reenqueue: too many retry ({1}). Discarding message for topic '{0}'",
                    message.Topic, message.Retried));
                OnMessageDiscarded(message);
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
                return false;
            }
            _produceMessages.Enqueue(produceMessage);
            // There is a race here, since Stop can happen in between the
            // previous and next lines. However this only happen when Stop
            // is required which means we can safely let a few messages in
            // the queue, they will be signaled as discarded to the outer
            // world (Stop is permanent).
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
        /// Request a metadata refresh from the cluster.
        /// </summary>
        /// <returns></returns>
        private async Task EnsureHasRoutingTable()
        {
            RoutingTableRequired();
            try
            {
                var timeout = TimeSpan.FromMilliseconds(_configuration.ClientRequestTimeoutMs);
                var taskFetchingMetadata = _cluster.RequireNewRoutingTable();
                // We are going to wait for either the task to finish, or the timeout.
                // This task is blocking the entire (producer) driver, so we can not allow it to run a long
                // time.
                if (await (Task.WhenAny(taskFetchingMetadata, Task.Delay(timeout))) == taskFetchingMetadata)
                {
                    // The task finished
                    ChangeRoutingTable(taskFetchingMetadata.Result);
                }
                else
                {
                    // The Timeout finished first
                    _cluster.Logger.LogError(
                        $"Could not get routing table! The node we queried took more than {timeout.TotalSeconds} seconds to answer.");
                }
            }
            catch (Exception ex)
            {
                _cluster.Logger.LogError("Could not get routing table! The Kafka cluster is probably having problems answering requests. Exception was: " + ex);
            }
        }

        /// <summary>
        /// Actor loop. In case of produce events we prioritize the acknowledgements.
        /// </summary>
        private async Task ProcessMessage(RouterMessageType messageType)
        {
            try
            {
                switch (messageType)
                {
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
                            UpdatePartitionsBlacklist();
                            if (response.ProduceResponse.ProducePartitionResponse.TopicsResponse != null)
                            {
                                await HandleProduceAcknowledgement(response);
                            }
                            else
                            {
                                HandleProduceAcknowledgementNone(response);
                            }
                            response.OriginalBatch.Dispose();
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
            catch (Exception ex)
            {
                _cluster.Logger.LogError("Unexpected exception in produce router: " + ex);
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

            if (!_partitioners.TryGetValue(topic, out var selector))
            {
                var keySerializer = _configuration.SerializationConfig.GetSerializersForTopic(topic).Item1;
                var partitionSelectionImpl = _configuration.PartitionSelectionConfig.GetPartitionSelectionForTopic(
                    topic, _configuration.NumberOfMessagesBeforeRoundRobin, _randomGenerator.Next(), keySerializer);
                selector = new PartitionSelector(partitionSelectionImpl);
                _partitioners[topic] = selector;
            }

            var partitions = _routingTable.GetPartitions(topic);
            if (partitions.Length == 0)
            {
                await EnsureHasRoutingTable();
                partitions = _routingTable.GetPartitions(topic);
            }

            var partition = selector.GetPartition(produceMessage, partitions, GetFilter(topic));

            if (partition.Id == Partitions.None)
            {
                // Retry without filters because filtered partitions should be valid, we just wanted to avoid
                // spamming them while they're being rebalanced.
                partition = selector.GetPartition(produceMessage, partitions);

                if (partition.Id == Partitions.None)
                {
                    // So now there is really no available partition.
                    // Messages for topics with no partition available are postponed.
                    // They will be checked again when the routing table is updated.
                    PostponeMessage(produceMessage);
                    return;
                }
            }

            produceMessage.Partition = partition.Id;
            if (_batchStrategy.Send(partition.Leader, produceMessage))
            {
                MessageRouted(topic);
            }
            else
            {
                ReEnqueueMessage(produceMessage);
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
            foreach (var message in acknowledgement.OriginalBatch.SelectMany(gt => gt.SelectMany(gp => gp)))
            {
                if (acknowledgement.ReceiveDate == default(DateTime))
                {
                    // Message was not sent
                    ReEnqueueMessage(message);
                }
                else if (_configuration.ErrorStrategy == ErrorStrategy.Retry)
                {
                    ReEnqueueAfterError(message);
                }
                else
                {
                    // Discard
                    OnMessageDiscarded(message);
                }
            }
        }

        // temp variables used in the following method, avoid reallocating them each time
        private readonly Dictionary<string, HashSet<int>> _tmpPartitionsInError = new Dictionary<string, HashSet<int>>();
        private readonly Dictionary<string, HashSet<int>> _tmpPartitionsInRecoverableError = new Dictionary<string, HashSet<int>>();

        private static readonly HashSet<int> NullHash = new HashSet<int>(); // A sentinel object

        /// <summary>
        /// Free message resources
        /// </summary>
        /// <remarks>
        /// Key & Value should not be disposed in case of error as they will be returned in the lost message event
        /// </remarks>
        private void ClearMessage(Message message, bool shouldClearKeyValue = true)
        {
            if (_configuration.SerializationConfig.SerializeOnProduce)
            {
                message.ReleaseSerializedKeyValue();
            }
            else
            {
                if (shouldClearKeyValue)
                {
                    if (message.Key is IDisposable key)
                    {
                        key.Dispose();
                    }
                    if (message.Value is IDisposable value)
                    {
                        value.Dispose();
                    }
                }
            }
        }

        private bool IsPartitionOkForClients(ErrorCode errorCode)
        {
            if (errorCode != ErrorCode.NotEnoughReplicasAfterAppend) return Error.IsPartitionOkForClients(errorCode);

            // If error is NotEnoughReplicasAfterAppend the messages have actually been written on the leader,
            // retyring is a matter of checking our config:
            return !_configuration.RetryIfNotEnoughReplicasAfterAppend;
        }

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
            var produceResponse = acknowledgement.ProduceResponse.ProducePartitionResponse;
            var throttled = acknowledgement.ProduceResponse.ThrottleTime;

            if (throttled > 0)
            {
                Throttled(throttled);
            }

            // Fill partitions in error caches
            _tmpPartitionsInError.Clear();
            _tmpPartitionsInRecoverableError.Clear();
            foreach (var tr in produceResponse.TopicsResponse)
            {
                bool errors = false;
                foreach (var p in tr.PartitionsData.Where(p => !IsPartitionOkForClients(p.ErrorCode)))
                {
                    if (!errors)
                    {
                        errors = true;
                        _tmpPartitionsInError[tr.TopicName] = new HashSet<int>();
                        _tmpPartitionsInRecoverableError[tr.TopicName] = new HashSet<int>();
                    }

                    if (Error.IsPartitionErrorRecoverableForProducer(p.ErrorCode))
                    {

                        _cluster.Logger.LogWarning(
                            string.Format("Recoverable error detected: [topic: {0} - partition: {1} - error: {2}]",
                                tr.TopicName, p.Partition, p.ErrorCode));
                        _tmpPartitionsInRecoverableError[tr.TopicName].Add(p.Partition);
                        if (p.ErrorCode != ErrorCode.InvalidMessageSize && p.ErrorCode != ErrorCode.InvalidMessage)
                        {
                            _cluster.Logger.LogWarning(
                                string.Format("Will ignore [topic: {0} / partition: {1}] for a time", tr.TopicName,
                                    p.Partition));
                            BlacklistPartition(tr.TopicName, p.Partition);

                            if (p.ErrorCode == ErrorCode.RequestTimedOut)
                                BrokerTimeoutError(tr.TopicName);
                        }
                    }
                    else
                    {
                        _cluster.Logger.LogError(
                            string.Format("Irrecoverable error detected: [topic: {0} - partition: {1} - error: {2}]",
                                tr.TopicName, p.Partition, p.ErrorCode));
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

                foreach (var pm in grouping.SelectMany(g => g))
                {
                    if (recPartitions.Contains(pm.Partition))
                    {
                        ReEnqueueAfterError(pm);
                    }
                    else
                    {
                        if (errPartitions.Contains(pm.Partition))
                        {
                            _cluster.Logger.LogError(
                                string.Format(
                                    "[Producer] Irrecoverable error, discarding message for [topic: {0} / partition: {1}]",
                                    pm.Topic, pm.Partition));
                            OnMessageDiscarded(pm);
                        }
                        else
                        {
                            ++sent;
                            ClearMessage(pm.Message);
                        }
                    }
                }
                if (sent > 0)
                {
                    MessagesAcknowledged(grouping.Key, sent);
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
                        ReEnqueueAfterError(postponed.Dequeue());
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
            if (_numberOfPostponedMessages >= _configuration.MaxPostponedMessages)
            {
                _cluster.Logger.LogError(
                    string.Format(
                        "[Producer] Too many postponed messages, discarding message for [topic: {0} / partition: {1}]",
                        produceMessage.Topic, produceMessage.RequiredPartition));
                OnMessageDiscarded(produceMessage);
                return;
            }

            Queue<ProduceMessage> postponedQueue;
            if (!_postponedMessages.TryGetValue(produceMessage.Topic, out postponedQueue))
            {
                postponedQueue = new Queue<ProduceMessage>();
                _postponedMessages.Add(produceMessage.Topic, postponedQueue);
            }
            postponedQueue.Enqueue(produceMessage);
            if (postponedQueue.Count == 1)
            {
                if (produceMessage.RequiredPartition >= 0)
                {
                    _cluster.Logger.LogError(
                        string.Format("[Producer] No node available for [topic: {0} / partition: {1}], postponing messages.",
                            produceMessage.Topic, produceMessage.RequiredPartition));
                }
                else
                {
                    _cluster.Logger.LogError(string.Format(
                        "[Producer] No partition available for topic {0}, postponing messages.", produceMessage.Topic));
                }
            }
            ++_numberOfPostponedMessages;
            MessagePostponed(produceMessage.Topic);

            if (_checkPostponedMessages == null)
            {
                _checkPostponedMessages =
                    new Timer(_ => _messages.Post(RouterMessageType.CheckPostponed),
                              null, _configuration.MessageTtl, _configuration.MessageTtl);
            }
        }

        /// <summary>
        /// Mark a partition as temporary not usable.
        /// </summary>
        /// <param name="topic"></param>
        /// <param name="partition"></param>
        private void BlacklistPartition(string topic, int partition)
        {
            if (_configuration.MinimumTimeBetweenRefreshMetadata == default(TimeSpan))
            {
                return;
            }

            Dictionary<int, DateTime> filter;
            if (!_filters.TryGetValue(topic, out filter))
            {
                filter = new Dictionary<int, DateTime>();
                _filters.Add(topic, filter);
            }
            filter[partition] = DateTime.UtcNow.Add(_configuration.TemporaryIgnorePartitionTime);
        }

        private readonly List<int> _tmpFiltered = new List<int>();

        /// <summary>
        /// Check and remove old partition filters
        /// </summary>
        private void UpdatePartitionsBlacklist()
        {
            var now = DateTime.UtcNow;
            if (_filtersLastChecked.Add(_configuration.MinimumTimeBetweenRefreshMetadata) <= now)
            {
                _filtersLastChecked = now;
                foreach (var filter in _filters)
                {
                    _tmpFiltered.Clear();
                    foreach (var pt in filter.Value.Where(kv => kv.Value <= now))
                    {
                        _tmpFiltered.Add(pt.Key);
                    }
                    foreach (var partition in _tmpFiltered)
                    {
                        _cluster.Logger.LogInformation(string.Format("Unfiltering partition {0} for topic {1}", partition, filter.Key));
                        filter.Value.Remove(partition);
                    }
                }
            }
        }

        private Dictionary<int, DateTime> GetFilter(string topic)
        {
            Dictionary<int, DateTime> filter;
            return _filters.TryGetValue(topic, out filter) ? filter : null;
        }

        // Raise the MessageExpired event and release a message.
        private void OnMessageExpired(ProduceMessage message)
        {
            _cluster.Logger.LogError(string.Format(
                "[Producer] Not able to send message before reaching TTL for [topic: {0} / partition: {1}], message expired.",
                message.Topic, message.RequiredPartition));

            ClearMessage(message.Message, shouldClearKeyValue: false);
            MessageExpired(message.Topic, message.Message);
        }

        // Raise the MessageDiscarded event and release a message.
        private void OnMessageDiscarded(ProduceMessage message)
        {
            ClearMessage(message.Message, shouldClearKeyValue: false);
            MessageDiscarded(message.Topic, message.Message);
        }
    }
}
