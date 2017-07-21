// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using Kafka.Batching;
using Kafka.Cluster;
using Kafka.Common;
using Kafka.Protocol;
using Kafka.Public;
using ICluster = Kafka.Cluster.ICluster;

namespace Kafka.Routing
{
    /// <summary>
    /// Expose messages consumed from topics.
    /// </summary>
    interface IConsumeRouter
    {
        /// <summary>
        /// Provides a new routing table.
        /// </summary>
        /// <param name="table">The new table.</param>
        void ChangeRoutingTable(RoutingTable table);

        /// <summary>
        /// Consume a given partition of given topic, starting from the given offset.
        /// </summary>
        /// <param name="topic">Topic required to be consumed.</param>
        /// <param name="partition">The partition to consume. The special value -1 means consume
        /// all partitions.</param>
        /// <param name="offset">Offset to start consuming from. There are two special values:
        /// -1 means start from last available offset, -2 means start from first available offset</param>
        void StartConsume(string topic, int partition, long offset);

        /// <summary>
        /// Start consuming from a consumer group subscription.
        /// </summary>
        /// <param name="group"></param>
        /// <param name="topics"></param>
        void StartConsumeSubscription(IConsumerGroup group, IEnumerable<string> topics);

        /// <summary>
        /// Stop consuming a given topic / partition once the given offset is reached.
        /// </summary>
        /// <param name="topic">Topic to stop consuming from.</param>
        /// <param name="partition">Partition to stop consuming from. -1 means all.</param>
        /// <param name="offset">Stop consuming once this offset is reached. Offsets.Now (aka -42) means now.</param>
        void StopConsume(string topic, int partition, long offset);

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

        /// <summary>
        /// Acknowledge a response from a fetch request.
        /// </summary>
        /// <param name="fetchResponse">a fetch response from a Kafka broker.</param>
        void Acknowledge(CommonAcknowledgement<FetchResponse> fetchResponse);

        /// <summary>
        /// Acknowledge a response from an offset request.
        /// </summary>
        /// <param name="offsetResponse">an offset response from a Kafka broker.</param>
        void Acknowledge(CommonAcknowledgement<CommonResponse<OffsetPartitionResponse>> offsetResponse);

        /// <summary>
        /// Stop the consumer router.
        /// </summary>
        /// <returns></returns>
        Task Stop();

        /// <summary>
        /// Emit a received kafka message.
        /// </summary>
        event Action<RawKafkaRecord> MessageReceived;

        /// <summary>
        /// Signaled when a fetch response has been throttled
        /// </summary>
        event Action<int> Throttled;

        /// <summary>
        /// Signaled when partitions have been assigned by group coordinator
        /// </summary>
        event Action<IDictionary<string, ISet<PartitionOffset>>> PartitionsAssigned;

        /// <summary>
        /// Signaled when partitions have been revoked
        /// </summary>
        event Action PartitionsRevoked;
    }

    // Magic values for Offset APIs. Some from Kafka protocol, some special to us
    static class Offsets
    {
        public const long Some = 0; // From Kafka protocol
        public const long Latest = -1; // From Kafka protocol
        public const long Earliest = -2; // From Kafka protocol
        public const long Now = -42;
        public const long None = -43;
        public const long Never = -44;
    }

    /// <summary>
    /// Representation of a Kafka fetch unit.
    /// A Kafka FetchRequest will typically contain multiple fetch units.
    /// </summary>
    struct FetchMessage
    {
        public string Topic;  // Required topic
        public long Offset;   // Fetch from this offset
        public int Partition; // Fetch from this partition
        public int MaxBytes;  // Maximum number of bytes to return from this partition
    }

    /// <summary>
    /// Representation of a Kafka Offset unit.
    /// A Kafka OffsetRequest will typically contain multiple offset units.
    /// This request type is very confusing. Basically you use it to get starting
    /// offsets for segments in partition, but segments are an implementation detail
    /// of the Kafka broker. An external client will probably only use the special
    /// values for Time.
    /// </summary>
    struct OffsetMessage
    {
        // Topic to get offsets from.
        public string Topic;

        // From the Kafka Protocol: "Used to ask for all messages before a certain time (ms).
        // There are two special values. Specify -1 to receive the latest offset (i.e. the
        // offset of the next coming message) and -2 to receive the earliest available offset.
        // Note that because offsets are pulled in descending order, asking for the earliest
        // offset will always return you a single element."
        // We will probably only use the special values.
        public long Time;

        // Partition to get offsets from.
        public int Partition;

        // The maximum number of offsets to return. We will probably use 1.
        public int MaxNumberOfOffsets;
    }

    class CommonAcknowledgement<TResponse> where TResponse : IMemoryStreamSerializable, new()
    {
        public TResponse Response;
        public DateTime ReceivedDate;
    }

    /// <summary>
    /// Consumer implementation. For now partitions handled by the consumer must be specified,
    /// the offset fetch/commit API is not supported.
    /// </summary>
    class ConsumeRouter : IConsumeRouter
    {
        #region Batching

        class ByNodeFetchBatchingStrategy : IBatchStrategy<FetchMessage>
        {
            public bool Send(INode node, FetchMessage message)
            {
                return node.Fetch(message);
            }

            public void Dispose()
            {
            }
        }

        class GlobalFetchBatchingStrategy : IBatchStrategy<FetchMessage>
        {
            private readonly AccumulatorByNodeByTopic<FetchMessage> _accumulator;
            private Action<INode, IBatchByTopic<FetchMessage>> _batch;

            public GlobalFetchBatchingStrategy(Configuration configuration, Action<INode, IBatchByTopic<FetchMessage>> batch)
            {
                _accumulator = new AccumulatorByNodeByTopic<FetchMessage>(
                    m => m.Topic,
                    configuration.ConsumeBatchSize,
                    configuration.ConsumeBufferingTime);
                _accumulator.NewBatch += batch;
            }

            public bool Send(INode node, FetchMessage message)
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

        class ByNodeOffsetBatchingStrategy : IBatchStrategy<OffsetMessage>
        {
            public bool Send(INode node, OffsetMessage message)
            {
                return node.Offset(message);
            }

            public void Dispose()
            {
            }
        }

        class GlobalOffsetBatchingStrategy : IBatchStrategy<OffsetMessage>
        {
            private readonly AccumulatorByNodeByTopic<OffsetMessage> _accumulator;
            private Action<INode, IBatchByTopic<OffsetMessage>> _batch;

            public GlobalOffsetBatchingStrategy(Configuration configuration, Action<INode, IBatchByTopic<OffsetMessage>> batch)
            {
                _accumulator = new AccumulatorByNodeByTopic<OffsetMessage>(
                    m => m.Topic,
                    configuration.ConsumeBatchSize,
                    configuration.ConsumeBufferingTime);
                _accumulator.NewBatch += batch;
            }

            public bool Send(INode node, OffsetMessage message)
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

        #region Messages

        private enum ConsumerMessageType
        {
            FetchResponse,
            OffsetResponse,
            CheckPostponed,
            CheckPostponedAfterRoutingTableChange,
            Start,
            Stop,
            StartSubscription,
            Commit,
            Heartbeat,
        }

        private struct ConsumerMessage
        {
            public ConsumerMessageType MessageType;
            public ConsumerMessageValue MessageValue;
        }

        class TopicOnOff
        {
            public string Topic;
            public long Offset;
            public int Partition;
        }

        class CommitMsg
        {
            public TaskCompletionSource<bool> CommitPromise;
            public string Topic;
            public int Partition;
            public long Offset;
        }

        [StructLayout(LayoutKind.Explicit)]
        struct ConsumerMessageValue
        {
            [FieldOffset(0)]
            public CommonAcknowledgement<FetchResponse> FetchResponse;

            [FieldOffset(0)]
            public CommonAcknowledgement<CommonResponse<OffsetPartitionResponse>> OffsetResponse;

            [FieldOffset(0)]
            public IEnumerable<string> Subscription;

            [FieldOffset(0)]
            public TopicOnOff Topic;

            [FieldOffset(0)]
            public CommitMsg CommitMsg;
        }

        #endregion

        private class PartitionState
        {
            public long NextOffset;
            public long StopAt = Offsets.Never;
            public bool Active;
            public bool Postponed;
        }

        private readonly Dictionary<string, Dictionary<int, PartitionState>> _partitionsStates =
            new Dictionary<string, Dictionary<int, PartitionState>>();

        private readonly ICluster _cluster;
        private readonly Configuration _configuration;
        private readonly ActionBlock<ConsumerMessage> _innerActor;

        private RoutingTable _routingTable = new RoutingTable();
        private IEnumerable<string> _subscription;
        private IDictionary<string, ISet<PartitionOffset>> _partitionAssignments = new Dictionary<string, ISet<PartitionOffset>>();
        private IConsumerGroup _consumerGroup;
        private Timer _heartbeatTimer;
        private Timer _commitTimer;
        private volatile bool _commitRequired;

        // Active when there are some postponed topic/partition, it checks regurlarly
        // if they can be fetched again
        private Timer _checkPostponedPartitions;

        // Used in test mode to scale the waiting times when fetch metadata fails
        private readonly int _resolution;

        // Batch strategies
        private readonly IBatchStrategy<FetchMessage> _fetchBatchStrategy;
        private readonly IBatchStrategy<OffsetMessage> _offsetBatchStrategy;

        public event Action<RawKafkaRecord> MessageReceived = r => { };

        public event Action<int> Throttled = t => { };

        public event Action<IDictionary<string, ISet<PartitionOffset>>> PartitionsAssigned = x => { };

        public event Action PartitionsRevoked = () => {};

        /// <summary>
        /// Raised in case of unexpected internal error.
        /// </summary>
        public event Action<Exception> InternalError = e => { };

        /// <summary>
        /// Raised when fetches are postponed following cluster errors.
        /// </summary>
        public event Action<string, int> FetchPostponed = (t, p) => { };

        /// <summary>
        /// Raised when we asked for new metadata.
        /// </summary>
        public event Action RoutingTableRequired = () => { };

        public ConsumeRouter(ICluster cluster, Configuration configuration, int resolution = 1000)
        {
            _resolution = resolution;
            _cluster = cluster;
            _configuration = configuration;
            _innerActor = new ActionBlock<ConsumerMessage>(m => ProcessMessage(m),
                new ExecutionDataflowBlockOptions {TaskScheduler = configuration.TaskScheduler});
            if (configuration.BatchStrategy == BatchStrategy.ByNode)
            {
                _fetchBatchStrategy = new ByNodeFetchBatchingStrategy();
                _offsetBatchStrategy = new ByNodeOffsetBatchingStrategy();
            }
            else
            {
                _fetchBatchStrategy = new GlobalFetchBatchingStrategy(configuration, (n, b) =>
                {
                    if (n.Post(b))
                        return;
                    foreach (var fm in b.SelectMany(g => g))
                    {
                        Postpone(fm.Topic, fm.Partition);
                    }
                });
                _offsetBatchStrategy = new GlobalOffsetBatchingStrategy(configuration, (n, b) =>
                {
                    if (n.Post(b))
                        return;
                    foreach (var om in b.SelectMany(g => g))
                    {
                        Postpone(om.Topic, om.Partition);
                    }
                });
            }
        }

        public async Task Stop()
        {
            _innerActor.Complete();
            await _innerActor.Completion;
            _fetchBatchStrategy.Dispose();
            _offsetBatchStrategy.Dispose();
            if (_consumerGroup != null)
            {
                PartitionsRevoked();
                await CommitAll();
                await _consumerGroup.LeaveGroup();
                _heartbeatTimer.Dispose();
                if (_commitTimer != null)
                {
                    _commitTimer.Dispose();
                }
            }
        }

        /// <summary>
        /// Acknowledge a response from a fetch request. Will post a corresponding
        /// message to the inner actor.
        /// </summary>
        /// <param name="fetchResponse">a fetch response from a Kafka broker.</param>
        public void Acknowledge(CommonAcknowledgement<FetchResponse> fetchResponse)
        {
            _innerActor.Post(new ConsumerMessage
            {
                MessageType = ConsumerMessageType.FetchResponse,
                MessageValue = new ConsumerMessageValue
                {
                    FetchResponse = fetchResponse
                }
            });
        }

        /// <summary>
        /// Acknowledge a response from an offset request. Will post a corresponding
        /// message to the inner actor.
        /// </summary>
        /// <param name="offsetResponse">an offset response from a Kafka broker.</param>
        public void Acknowledge(CommonAcknowledgement<CommonResponse<OffsetPartitionResponse>> offsetResponse)
        {
            _innerActor.Post(new ConsumerMessage
            {
                MessageType = ConsumerMessageType.OffsetResponse,
                MessageValue = new ConsumerMessageValue
                {
                    OffsetResponse = offsetResponse
                }
            });
        }

        /// <summary>
        /// Consume a given partition of given topic, starting from the given offset.
        /// </summary>
        /// <param name="topic">Topic required to be consumed.</param>
        /// <param name="partition">The partition to consume. The special value -1 means consume
        /// all partitions.</param>
        /// <param name="offset">Offset to start consuming from. There are two special values:
        /// -1 means start from last available offset, -2 means start from first available offset</param>
        public void StartConsume(string topic, int partition, long offset)
        {
            _innerActor.Post(new ConsumerMessage
            {
                MessageType = ConsumerMessageType.Start,
                MessageValue =
                    new ConsumerMessageValue
                    {
                        Topic = new TopicOnOff {Topic = topic, Offset = offset, Partition = partition}
                    }
            });
        }

        public void StartConsumeSubscription(IConsumerGroup group, IEnumerable<string> topics)
        {
            if (_consumerGroup != null)
            {
                throw new ArgumentException(string.Format("A subscription was already started for this consumer (using group id {0})", _consumerGroup.GroupId));
            }
            _consumerGroup = group;
            _innerActor.Post(new ConsumerMessage
            {
                MessageType = ConsumerMessageType.StartSubscription,
                MessageValue = new ConsumerMessageValue { Subscription = topics.ToArray() }
            });
        }

        /// <summary>
        /// Stop consuming a given topic / partition once the given offset is reached.
        /// </summary>
        /// <param name="topic">Topic to stop consuming from.</param>
        /// <param name="partition">Topic to stop consuming from. -1 means all.</param>
        /// <param name="offset">Stop consuming once this offset is reached. Offsets.Now (aka -42) means now.</param>
        public void StopConsume(string topic, int partition, long offset)
        {
            _innerActor.Post(new ConsumerMessage
            {
                MessageType = ConsumerMessageType.Stop,
                MessageValue =
                    new ConsumerMessageValue
                    {
                        Topic = new TopicOnOff { Topic = topic, Offset = offset, Partition = partition }
                    }
            });
        }

        /// <summary>
        /// Signal a routing table change. We do this using an Interlocked
        /// operation to avoid having to manage yet another prioritized message
        /// in the actor loop.
        /// </summary>
        /// <param name="table"></param>
        public void ChangeRoutingTable(RoutingTable table)
        {
            if (_routingTable != null && _routingTable.LastRefreshed >= table.LastRefreshed) return;
            Interlocked.Exchange(ref _routingTable, new RoutingTable(table));
            _innerActor.Post(new ConsumerMessage
            {
                MessageType = ConsumerMessageType.CheckPostponedAfterRoutingTableChange
            });
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
                ChangeRoutingTable(await _cluster.RequireNewRoutingTable());
            }
            catch
            {
                hasError = true;
            }
            if (hasError)
                await Task.Delay(_resolution /* == 1000 * (_resolution / 1000) == */);
        }

        /// <summary>
        /// Inner actor message routing to their handling methods.
        /// </summary>
        /// <param name="message"></param>
        /// <returns></returns>
        private async Task ProcessMessage(ConsumerMessage message)
        {
            try
            {
                switch (message.MessageType)
                {
                    case ConsumerMessageType.Start:
                        await HandleStart(message.MessageValue.Topic);
                        break;

                    case ConsumerMessageType.Stop:
                        HandleStop(message.MessageValue.Topic);
                        break;

                    case ConsumerMessageType.FetchResponse:
                        await HandleFetchResponse(message.MessageValue.FetchResponse);
                        break;

                    case ConsumerMessageType.OffsetResponse:
                        await HandleOffsetResponse(message.MessageValue.OffsetResponse);
                        break;

                    case ConsumerMessageType.CheckPostponed:
                        await EnsureHasRoutingTable();
                        goto case ConsumerMessageType.CheckPostponedAfterRoutingTableChange;

                    case ConsumerMessageType.CheckPostponedAfterRoutingTableChange:
                        CheckPostponed();
                        break;

                    case ConsumerMessageType.StartSubscription:
                        await HandleStartSubscription(message.MessageValue.Subscription);
                        break;

                    case ConsumerMessageType.Heartbeat:
                        await HandleHeartbeat();
                        break;

                    case ConsumerMessageType.Commit:
                        await CheckCommit(message.MessageValue.CommitMsg);
                        break;
                }
            }
            catch (Exception ex)
            {
                InternalError(ex);
            }
        }

        private async Task HandleStartSubscription(IEnumerable<string> subscription)
        {
            _subscription = subscription;

            await JoinGroup();

            _heartbeatTimer =
                new Timer(_ => _innerActor.Post(new ConsumerMessage { MessageType = ConsumerMessageType.Heartbeat }),
                    null, TimeSpan.FromMilliseconds(_consumerGroup.Configuration.SessionTimeoutMs / 2.0),
                    TimeSpan.FromMilliseconds(_consumerGroup.Configuration.SessionTimeoutMs / 2.0));
            if (_consumerGroup.Configuration.AutoCommitEveryMs > 0)
            {
                _commitTimer = new Timer(_ => RequireCommit(), null,
                    TimeSpan.FromMilliseconds(_consumerGroup.Configuration.AutoCommitEveryMs),
                    TimeSpan.FromMilliseconds(_consumerGroup.Configuration.AutoCommitEveryMs));
            }
        }

        /// <summary>
        /// (Re)join a consumer group, starting all new assigned partitions
        /// </summary>
        /// <returns></returns>
        private async Task JoinGroup()
        {
            try
            {
                var joined = await _consumerGroup.Join(_subscription);

                foreach (var assignment in joined.Assignments)
                {
                    foreach (var p in assignment.Value)
                    {
                        if (!IsAssigned(assignment.Key, p.Partition))
                        {
                            StartConsume(assignment.Key, p.Partition, p.Offset);
                        }
                    }
                }

                PartitionsAssigned(joined.Assignments);

                _partitionAssignments = joined.Assignments;
            }
            catch (Exception ex)
            {
                _cluster.Logger.LogError(string.Format("Some exception occured while trying to join group {0} - {1}",
                    _consumerGroup.GroupId, ex));
                _partitionAssignments = new Dictionary<string, ISet<PartitionOffset>>();
            }
        }

        private readonly int[] _tmpPartitions = new int[1];

        /// <summary>
        /// Manage beginning of fetch on given topic/partition.
        /// If the required offset is positive (i.e. client required a specific known offset
        /// to begin from) we issue a Fetch request. If the required offset is Earliest or Latest,
        /// we issue an Offset request to discover the offset values before beginning fetch requests.
        ///
        /// This method initializes the PartitionState associated to this topic / partition.
        /// </summary>
        /// <param name="topicOnOff"></param>
        /// <returns></returns>
        private async Task HandleStart(TopicOnOff topicOnOff)
        {
            int[] partitions = null;
            if (topicOnOff.Partition >= 0)
            {
                partitions = _tmpPartitions;
                partitions[0] = topicOnOff.Partition;
            }
            else
            {
                if (_consumerGroup != null)
                {
                    // (Re)start all assigned partitions
                    partitions = _partitionAssignments[topicOnOff.Topic].Select(po => po.Partition).ToArray();
                }
                else
                    while (partitions == null)
                    {
                        try
                        {
                            partitions = await _cluster.RequireAllPartitionsForTopic(topicOnOff.Topic);
                        }
                        catch
                        {
                            // Means the cluster is kind of dead, ignore and retry, there's not much else
                            // to do anyway since we won't be able to send more fetch requests until
                            // some node is available.
                            // TODO: be verbose?
                        }
                    }
            }

            foreach (int partition in partitions)
            {
                // Initialize PartitionState if needed
                Dictionary<int, PartitionState> states;
                if (!_partitionsStates.TryGetValue(topicOnOff.Topic, out states))
                {
                    states = new Dictionary<int, PartitionState>();
                    _partitionsStates[topicOnOff.Topic] = states;
                }
                PartitionState state;
                if (!states.TryGetValue(partition, out state))
                {
                    state = new PartitionState { NextOffset = topicOnOff.Offset, Active = false, Postponed = false };
                    states[partition] = state;
                }

                // Already active partitions are not (re)started
                if (!state.Active)
                {
                    await HandleStartPartition(partition, state, topicOnOff);
                }
                else // But we change back their StopAt mark
                {
                    state.StopAt = Offsets.Never;
                }
            }
        }

        private async Task HandleStartPartition(int partition, PartitionState state, TopicOnOff startInfo)
        {
            // Activate partition
            state.Active = true;
            state.StopAt = Offsets.Never;

            // Known offset => FetchRequest
            if (startInfo.Offset >= 0)
            {
                state.NextOffset = startInfo.Offset;
                await Fetch(startInfo.Topic, partition, startInfo.Offset);
            }
            // Restart from last known offset
            else if (startInfo.Offset == Offsets.Now)
            {
                // last known offset was unknown => OffsetRequest
                if (state.NextOffset < 0)
                {
                    await Offset(startInfo.Topic, partition, state.NextOffset);
                }
                else
                {
                    await Fetch(startInfo.Topic, partition, state.NextOffset);
                }
            }
            // Unknown offset => OffsetRequest
            else
            {
                await Offset(startInfo.Topic, partition, startInfo.Offset);
            }
        }

        private async Task Offset(string topic, int partition, long time)
        {
            await NodeFetch(topic, partition,
                l =>
                    _offsetBatchStrategy.Send(l,
                        (new OffsetMessage {Topic = topic, Partition = partition, MaxNumberOfOffsets = 1, Time = time})));
        }

        /// <summary>
        /// Set the stop mark for a given topic. Depending on the value and the last seen
        /// offset we either disable the topic/partition now or mark it for later check.
        /// </summary>
        /// <param name="topicOnOff"></param>
        private void HandleStop(TopicOnOff topicOnOff)
        {
            if (!_partitionsStates.ContainsKey(topicOnOff.Topic))
            {
                return;
            }
            foreach (
                var partition in
                    topicOnOff.Partition == Partitions.All
                        ? _partitionsStates[topicOnOff.Topic].Keys
                        : Enumerable.Repeat(topicOnOff.Partition, 1))
            {
                if (!IsAssigned(topicOnOff.Topic, partition))
                {
                    // Skip non assigned partitions in case of consumer group usage
                    continue;
                }

                PartitionState state;
                try
                {
                    state = _partitionsStates[topicOnOff.Topic][partition];
                }
                catch (Exception exception)
                {
                    // Topic / partition was probably never started
                    InternalError(exception);
                    continue;
                }

                if (topicOnOff.Offset == Offsets.Now || (topicOnOff.Offset >= 0 && state.NextOffset > topicOnOff.Offset))
                {
                    if (state.Postponed)
                    {
                        state.Postponed = false;
                        state.Active = false;
                    }
                    else
                    {
                        state.StopAt = topicOnOff.Offset == Offsets.Now ? state.NextOffset - 1 : topicOnOff.Offset;
                        if (state.StopAt < 0)
                            state.StopAt = Offsets.Now;
                    }
                }
                else if (topicOnOff.Offset < 0)
                {
                    // nothing to do
                }
                else
                {
                    state.StopAt = topicOnOff.Offset;
                }
            }
        }

        private static readonly Task DoneTask = Task.FromResult(true);

        private DateTime _lastHeartBeat;

        private Task CheckHeartbeat()
        {
            if (_consumerGroup != null
                && (DateTime.UtcNow - _lastHeartBeat).TotalMilliseconds
                    > _consumerGroup.Configuration.SessionTimeoutMs / 2.0)
            {
                return HandleHeartbeat();
            }
            return DoneTask;
        }

        private Task CheckCommit(CommitMsg commit)
        {
            return commit != null ? CommitOne(commit) : _commitRequired ? CommitAll() : DoneTask;
        }

        private async Task CommitOne(CommitMsg commit)
        {
            try
            {
                await
                    _consumerGroup.Commit(
                        Enumerable.Repeat(
                            new TopicData<OffsetCommitPartitionData>
                            {
                                TopicName = commit.Topic,
                                PartitionsData =
                                    Enumerable.Repeat(
                                        new OffsetCommitPartitionData
                                        {
                                            Partition = commit.Partition,
                                            Offset = commit.Offset,
                                            Metadata = ""
                                        }, 1)
                            }, 1));
                commit.CommitPromise.SetResult(true);
            }
            catch (Exception ex)
            {
                _cluster.Logger.LogError("Exception caught while trying to commit offsets. " + ex);
                commit.CommitPromise.SetException(ex);
            }
        }

        private async Task CommitAll()
        {
            try
            {
                await
                    _consumerGroup.Commit(
                        _partitionsStates.Select(
                            tp =>
                                new TopicData<OffsetCommitPartitionData>
                                {
                                    TopicName = tp.Key,
                                    PartitionsData =
                                        tp.Value.Where(ps => ps.Value.Active)
                                            .Select(
                                                ps =>
                                                    new OffsetCommitPartitionData
                                                    {
                                                        Partition = ps.Key,
                                                        Offset = ps.Value.NextOffset,
                                                        Metadata = ""
                                                    })
                                }));
            }
            catch(Exception ex)
            {
                _cluster.Logger.LogError("Exception caught while trying to commit offsets. " + ex);
            }
            _commitRequired = false;
        }

        public void RequireCommit()
        {
            _commitRequired = true;
            _innerActor.Post(new ConsumerMessage { MessageType = ConsumerMessageType.Commit });
        }

        public Task CommitAsync(string topic, int partition, long offset)
        {
            var promise = new TaskCompletionSource<bool>();
            _innerActor.Post(new ConsumerMessage
            {
                MessageType = ConsumerMessageType.Commit,
                MessageValue =
                    new ConsumerMessageValue
                    {
                        CommitMsg =
                            new CommitMsg
                            {
                                CommitPromise = promise,
                                Offset = offset,
                                Partition = partition,
                                Topic = topic
                            }
                    }
            });
            return promise.Task;
        }

        // Send an heartbeat request. In case of errors, will start a
        // join group procedure. If the error was RebalanceInProgress, will
        // also commit offsets before rejoining.
        private async Task HandleHeartbeat()
        {
            bool mustRejoin = false;
            try
            {
                var result = await _consumerGroup.Heartbeat();
                _lastHeartBeat = DateTime.UtcNow;
                if (result != ErrorCode.NoError)
                {
                    _cluster.Logger.LogInformation(
                        string.Format("Consumer group \"{0}\" rebalance required (heartbeat returned {1})",
                            _consumerGroup.GroupId, result));
                    mustRejoin = true;
                    if (result == ErrorCode.RebalanceInProgress)
                    {
                        PartitionsRevoked();

                        // Save offsets before rejoining
                        await CommitAll();
                    }
                }
            }
            catch (Exception)
            {
                _cluster.Logger.LogError(
                    string.Format("Consumer group \"{0}\" rebalance required (coordinator node dead).",
                        _consumerGroup.GroupId));
                mustRejoin = true;
            }

            if (mustRejoin)
            {
                await JoinGroup();
            }
        }

        // Utility function, statically allocated closure
        private static bool IsPartitionOkForClients(FetchPartitionResponse fr)
        {
            return Error.IsPartitionOkForClients(fr.ErrorCode);
        }

        // Utility function, statically allocated closure
        private static bool IsPartitionOkForClients(OffsetPartitionResponse or)
        {
            return Error.IsPartitionOkForClients(or.ErrorCode);
        }

        /// <summary>
        /// Iterate over all partition responses. We refresh the metadata when a partition
        /// is in error.
        /// </summary>
        private async Task HandleResponse<TPartitionResponse>(CommonResponse<TPartitionResponse> response, DateTime receivedDate,
            Func<string, TPartitionResponse, Task> responseHandler, Func<TPartitionResponse, bool> partitionChecker)
            where TPartitionResponse : IMemoryStreamSerializable, new()
        {
            bool gotMetadata = false;
            foreach (var r in response.TopicsResponse)
            {
                foreach (var pr in r.PartitionsData)
                {
                    // Refresh metadata in case of error. We do this only once to avoid
                    // metadata request bombing.
                    if (!gotMetadata && receivedDate > _routingTable.LastRefreshed && !partitionChecker(pr))
                    {
                        // TODO: factorize that with ProducerRouter code?
                        await EnsureHasRoutingTable();
                        gotMetadata = true;
                    }
                    await responseHandler(r.TopicName, pr);
                }
            }
        }

        private Task HandleOffsetResponse(CommonAcknowledgement<CommonResponse<OffsetPartitionResponse>> offsetResponse)
        {
            return HandleResponse(offsetResponse.Response, offsetResponse.ReceivedDate, HandleOffsetPartitionResponse, IsPartitionOkForClients);
        }

        // Initiate appropriate fetch request following offset update, or retry offset request
        // when nothing has been received.
        private async Task HandleOffsetPartitionResponse(string topic, OffsetPartitionResponse partitionResponse)
        {
            var state = _partitionsStates[topic][partitionResponse.Partition];

            if (!CheckActivity(state, topic, partitionResponse.Partition))
            {
                return;
            }

            // TODO: check size is 0 or 1 only
            if (partitionResponse.Offsets.Length == 0)
            {
                // there was probably an error, in any case retry
                if (partitionResponse.ErrorCode != ErrorCode.NoError)
                {
                    _cluster.Logger.LogError(string.Format(
                        "Error on ListOffsets request for topic {0} / partition {1} [{2}].", topic,
                        partitionResponse.Partition, partitionResponse.ErrorCode));
                }
                await Offset(topic, partitionResponse.Partition, state.NextOffset);
            }
            else
            {
                // start Fetch loop
                var nextOffset = partitionResponse.Offsets[0];

                if (state.StopAt == Offsets.Never || nextOffset <= state.StopAt)
                {
                    state.NextOffset = nextOffset;
                    await Fetch(topic, partitionResponse.Partition, state.NextOffset);
                }
                else
                {
                    state.Active = false;
                }
            }
        }

        private Task HandleFetchResponse(CommonAcknowledgement<FetchResponse> fetchResponse)
        {
            if (fetchResponse.Response.ThrottleTime > 0)
            {
                Throttled(fetchResponse.Response.ThrottleTime);
            }
            return HandleResponse(fetchResponse.Response.FetchPartitionResponse, fetchResponse.ReceivedDate, HandleFetchPartitionResponse, IsPartitionOkForClients);
        }

        private bool CheckActivity(PartitionState state, string topic, int partition)
        {
            if (!IsAssigned(topic, partition))
            {
                state.Active = false;
            }

            return state.Active;
        }

        /// <summary>
        /// Handle a partition response for a given topic: filter out of range messages
        /// (can happen when receiving compressed messages), update the partitions states for the
        /// topic, emit all received messages on the dispatcher, and issue a new Fetch request if needed.
        /// </summary>
        /// <param name="topic"></param>
        /// <param name="partitionResponse"></param>
        /// <returns></returns>
        private async Task HandleFetchPartitionResponse(string topic, FetchPartitionResponse partitionResponse)
        {
            var state = _partitionsStates[topic][partitionResponse.Partition];

            if (CheckActivity(state, topic, partitionResponse.Partition))
            {
                // Filter messages under required offset, this may happen when receiving
                // compressed messages. The Kafka protocol specifies it's up to the client
                // to filter out those messages. We also filter messages with offset greater
                // than the required stop offset if set.
                long firstRequired = state.NextOffset;
                foreach (var message in
                    partitionResponse.Messages.Where(
                        message =>
                            message.Offset >= firstRequired
                                && (state.StopAt < 0 || message.Offset <= state.StopAt)))
                {
                    MessageReceived(new RawKafkaRecord
                    {
                        Topic = topic,
                        Key = message.Message.Key,
                        Value = message.Message.Value,
                        Offset = message.Offset,
                        Lag = partitionResponse.HighWatermarkOffset - message.Offset,
                        Partition = partitionResponse.Partition,
                        Timestamp = Timestamp.FromUnixTimestamp(message.Message.TimeStamp)
                    });
                    state.NextOffset = message.Offset + 1;

                    await CheckHeartbeat();
                    await CheckCommit(null);

                    // Recheck status (may have changed if heartbeat triggered rebalance)
                    // TODO: is it really useful to check for heartbeat/commit after each message ?
                    // It's merely a defense versus bad message handler, maybe we don't care and
                    // just better check only at the beginning of partition processing.
                    if (!CheckActivity(state, topic, partitionResponse.Partition))
                    {
                        break;
                    }
                }
            }

            ResponseMessageListPool.Release(partitionResponse.Messages);

            await CheckCommit(null);

            // Stop if we have seen the last required offset
            // TODO: what if we never see it?
            if (!state.Active || (state.StopAt != Offsets.Never && state.NextOffset > state.StopAt))
            {
                state.Active = false;
                return;
            }

            // Loop fetch request
            await Fetch(topic, partitionResponse.Partition, state.NextOffset);
        }

        private async Task Fetch(string topic, int partition, long offset)
        {
            await NodeFetch(topic, partition,
                l =>
                    _fetchBatchStrategy.Send(l, new FetchMessage
                    {
                        Topic = topic,
                        Partition = partition,
                        Offset = offset,
                        MaxBytes = _configuration.FetchMessageMaxBytes
                    }));
        }

        // Code factorization for Fetch/Offset requests
        private async Task NodeFetch(string topic, int partition, Func<INode, bool> fetchAction)
        {
            var leader = GetNodeForPartition(topic, partition);
            if (leader == null)
            {
                await EnsureHasRoutingTable();
                leader = GetNodeForPartition(topic, partition);
            }

            if (leader == null || !fetchAction(leader))
            {
                Postpone(topic, partition);
            }
        }

        private void Postpone(string topic, int partition)
        {
            _cluster.Logger.LogError(
                string.Format("[Consumer] No node available for [topic: {0} / partition: {1}]",
                    topic, partition));
            _partitionsStates[topic][partition].Postponed = true;
            FetchPostponed(topic, partition);
            CheckStartTimer();
        }

        /// <summary>
        /// Iterate over all postponed {topic, partition} and issue Fetch/Offset requests
        /// when a node has become available for it.
        /// </summary>
        private void CheckPostponed()
        {
            if (_checkPostponedPartitions != null)
            {
                _checkPostponedPartitions.Dispose();
                _checkPostponedPartitions = null;
            }

            foreach (var kv in _partitionsStates)
            {
                foreach (var kv2 in kv.Value.Where(p => p.Value.Postponed))
                {
                    var leader = GetNodeForPartition(kv.Key, kv2.Key);
                    if (leader == null)
                    {
                        CheckStartTimer();
                        continue;
                    }
                    if (kv2.Value.NextOffset >= 0)
                    {
                        if (_fetchBatchStrategy.Send(leader,
                            new FetchMessage
                            {
                                Topic = kv.Key,
                                Partition = kv2.Key,
                                Offset = kv2.Value.NextOffset
                            }))
                        {
                            kv2.Value.Postponed = false;
                        }
                    }
                    else
                    {
                        if (_offsetBatchStrategy.Send(leader,
                            new OffsetMessage
                            {
                                Topic = kv.Key,
                                Partition = kv2.Key,
                                MaxNumberOfOffsets = 1,
                                Time = kv2.Value.NextOffset
                            }))
                        {
                            kv2.Value.Postponed = false;
                        }
                    }
                    
                }
            }
        }

        private void CheckStartTimer()
        {
            if (_checkPostponedPartitions == null)
            {
                _checkPostponedPartitions =
                    new Timer(
                        _ => _innerActor.Post(new ConsumerMessage { MessageType = ConsumerMessageType.CheckPostponed }),
                        null, TimeSpan.FromSeconds(10), TimeSpan.FromMilliseconds(-1));
            }
        }

        private INode GetNodeForPartition(string topic, int partition)
        {
            return _routingTable.GetLeaderForPartition(topic, partition);
        }

        bool IsAssigned(string topic, int partition)
        {
            if (_consumerGroup == null)
            {
                // No consumer group => partitions are always assigned to us
                return true;
            }

            ISet<PartitionOffset> partitions;
            if (_partitionAssignments.TryGetValue(topic, out partitions))
            {
                return partitions.Contains(new PartitionOffset { Partition = partition });
            }
            return false;
        }
    }
}