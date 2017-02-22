// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using Kafka.Batching;
using Kafka.Cluster;
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
        /// Stop consuming a given topic / partition once the given offset is reached.
        /// </summary>
        /// <param name="topic">Topic to stop consuming from.</param>
        /// <param name="partition">Partition to stop consuming from. -1 means all.</param>
        /// <param name="offset">Stop consuming once this offset is reached. Offsets.Now (aka -42) means now.</param>
        void StopConsume(string topic, int partition, long offset);

        /// <summary>
        /// Acknowledge a response from a fetch request.
        /// </summary>
        /// <param name="fetchResponse">a fetch response from a Kafka broker.</param>
        void Acknowledge(CommonAcknowledgement<FetchPartitionResponse> fetchResponse);

        /// <summary>
        /// Acknowledge a response from an offset request.
        /// </summary>
        /// <param name="offsetResponse">an offset response from a Kafka broker.</param>
        void Acknowledge(CommonAcknowledgement<OffsetPartitionResponse> offsetResponse);

        /// <summary>
        /// Stop the consumer router.
        /// </summary>
        /// <returns></returns>
        Task Stop();

        /// <summary>
        /// Emit a received kafka message.
        /// </summary>
        event Action<RawKafkaRecord> MessageReceived;
    }

    // Magic values for Offset APIs. Some from Kafka protocol, some special to us
    static class Offsets
    {
        public const long Some = 0; // From Kafka protocol
        public const long Latest = -1; // From Kafka protocol
        public const long Earliest = -2; // From Kafka protocol
        public const long Now = -42;
        public const long None = -42;
        public const long Never = -42;
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

    struct CommonAcknowledgement<TResponse> where TResponse : IMemoryStreamSerializable, new()
    {
        public CommonResponse<TResponse> Response;
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
            Stop
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

        [StructLayout(LayoutKind.Explicit)]
        struct ConsumerMessageValue
        {
            [FieldOffset(0)]
            public CommonAcknowledgement<FetchPartitionResponse> FetchResponse;

            [FieldOffset(0)]
            public CommonAcknowledgement<OffsetPartitionResponse> OffsetResponse;

            [FieldOffset(0)]
            public TopicOnOff Topic;
        }

        #endregion

        private class PartitionState
        {
            public long LastOffsetSeen;
            public long NextOffsetExpected;
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

        // Active when there are some postponed topic/partition, it checks regurlarly
        // if they can be fetched again
        private Timer _checkPostponedPartitions;

        // Used in test mode to scale the waiting times when fetch metadata fails
        private readonly int _resolution;

        // Batch strategies
        private readonly IBatchStrategy<FetchMessage> _fetchBatchStrategy;
        private readonly IBatchStrategy<OffsetMessage> _offsetBatchStrategy;

        public event Action<RawKafkaRecord> MessageReceived = r => { };

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
                _fetchBatchStrategy = new GlobalFetchBatchingStrategy(configuration, (n, b) => n.Post(b));
                _offsetBatchStrategy = new GlobalOffsetBatchingStrategy(configuration, (n, b) => n.Post(b));
            }
        }

        public async Task Stop()
        {
            _innerActor.Complete();
            await _innerActor.Completion;
            _fetchBatchStrategy.Dispose();
            _offsetBatchStrategy.Dispose();
        }

        /// <summary>
        /// Acknowledge a response from a fetch request. Will post a corresponding
        /// message to the inner actor.
        /// </summary>
        /// <param name="fetchResponse">a fetch response from a Kafka broker.</param>
        public void Acknowledge(CommonAcknowledgement<FetchPartitionResponse> fetchResponse)
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
        public void Acknowledge(CommonAcknowledgement<OffsetPartitionResponse> offsetResponse)
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
                }
            }
            catch (Exception ex)
            {
                InternalError(ex);
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
                var state = new PartitionState
                {
                    LastOffsetSeen = Offsets.None,
                    NextOffsetExpected = topicOnOff.Offset,
                    Active = true,
                    Postponed = false
                };
                states[partition] = state;

                // Known offset => FetchRequest
                if (topicOnOff.Offset >= 0)
                {
                    await Fetch(topicOnOff.Topic, partition, topicOnOff.Offset);
                }
                // Unknown offset => OffsetRequest
                else
                {
                    await Offset(topicOnOff.Topic, partition, topicOnOff.Offset);
                }
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
            foreach (
                var partition in
                    topicOnOff.Partition == Partitions.All
                        ? _partitionsStates[topicOnOff.Topic].Keys
                        : Enumerable.Repeat(topicOnOff.Partition, 1))
            {
                PartitionState state;
                try
                {
                    state = _partitionsStates[topicOnOff.Topic][partition];
                }
                catch (Exception exception)
                {
                    // Topic / partition was probably not started
                    InternalError(exception);
                    continue;
                }

                if (topicOnOff.Offset == Offsets.Now ||
                    (topicOnOff.Offset >= 0 && state.LastOffsetSeen > topicOnOff.Offset))
                {
                    state.Active = false;
                    state.Postponed = false;
                    continue;
                }

                if (topicOnOff.Offset < 0)
                {
                    continue;
                }

                state.StopAt = topicOnOff.Offset;
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
        private async Task HandleResponse<TPartitionResponse>(CommonAcknowledgement<TPartitionResponse> acknowledgement,
            Func<string, TPartitionResponse, Task> responseHandler, Func<TPartitionResponse, bool> partitionChecker)
            where TPartitionResponse : IMemoryStreamSerializable, new()
        {
            bool gotMetadata = false;
            foreach (var r in acknowledgement.Response.TopicsResponse)
            {
                foreach (var pr in r.PartitionsData)
                {
                    // Refresh metadata in case of error. We do this only once to avoid
                    // metadata request bombing.
                    if (!gotMetadata && acknowledgement.ReceivedDate > _routingTable.LastRefreshed && !partitionChecker(pr))
                    {
                        // TODO: factorize that with ProducerRouter code?
                        await EnsureHasRoutingTable();
                        gotMetadata = true;
                    }
                    await responseHandler(r.TopicName, pr);
                }
            }
        }

        private Task HandleOffsetResponse(CommonAcknowledgement<OffsetPartitionResponse> offsetResponse)
        {
            return HandleResponse(offsetResponse, HandleOffsetPartitionResponse, IsPartitionOkForClients);
        }

        // Initiate appropriate fetch request following offset update, or retry offset request
        // when nothing has been received.
        private async Task HandleOffsetPartitionResponse(string topic, OffsetPartitionResponse partitionResponse)
        {
            var state = _partitionsStates[topic][partitionResponse.Partition];
            if (!state.Active)
            {
                return;
            }

            // TODO: check size is 0 or 1 only
            if (partitionResponse.Offsets.Length == 0)
            {
                // there was an error, retry
                await Offset(topic, partitionResponse.Partition, state.NextOffsetExpected);
            }
            else
            {
                // start Fetch loop
                var nextOffset = partitionResponse.Offsets[0];
                if (state.StopAt < 0 || nextOffset <= state.StopAt)
                {
                    state.NextOffsetExpected = nextOffset;
                    await Fetch(topic, partitionResponse.Partition, state.NextOffsetExpected);
                }
                else
                {
                    state.Active = false;
                }
            }
        }

        private Task HandleFetchResponse(CommonAcknowledgement<FetchPartitionResponse> fetchResponse)
        {
            return HandleResponse(fetchResponse, HandleFetchPartitionResponse, IsPartitionOkForClients);
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
            if (!state.Active)
            {
                return;
            }

            // Filter messages under required offset, this may happen when receiving
            // compressed messages. The Kafka protocol specifies it's up to the client
            // to filter out those messages. We also filter messages with offset greater
            // than the required stop offset if set.
            foreach (
                var message in
                    partitionResponse.Messages.Where(
                        message =>
                            message.Offset >= state.NextOffsetExpected &&
                            (state.StopAt < 0 || message.Offset <= state.StopAt)))
            {
                state.LastOffsetSeen = message.Offset;
                MessageReceived(new RawKafkaRecord
                    {
                        Topic = topic,
                        Key = message.Message.Key,
                        Value = message.Message.Value,
                        Offset = message.Offset,
                        Partition = partitionResponse.Partition
                    });
            }

            ResponseMessageListPool.Release(partitionResponse.Messages);

            // Stop if we have seen the last required offset
            // TODO: what if we never see it?
            if (state.StopAt != Offsets.Never && state.LastOffsetSeen >= state.StopAt)
            {
                state.Active = false;
                return;
            }

            // Loop fetch request
            if (state.LastOffsetSeen >= Offsets.Some)
            {
                state.NextOffsetExpected = state.LastOffsetSeen + 1;
            }
            await Fetch(topic, partitionResponse.Partition, state.NextOffsetExpected);
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
        private async Task NodeFetch(string topic, int partition, Action<INode> fetchAction)
        {
            var leader = GetNodeForPartition(topic, partition);
            if (leader == null)
            {
                await EnsureHasRoutingTable();
                leader = GetNodeForPartition(topic, partition);
            }

            if (leader == null)
            {
                Postpone(topic, partition);
                return;
            }

            fetchAction(leader);
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
                    if (kv2.Value.NextOffsetExpected >= 0)
                    {
                        _fetchBatchStrategy.Send(leader, new FetchMessage
                        {
                            Topic = kv.Key,
                            Partition = kv2.Key,
                            Offset = kv2.Value.NextOffsetExpected
                        });
                    }
                    else
                    {
                        _offsetBatchStrategy.Send(leader, new OffsetMessage
                        {
                            Topic = kv.Key,
                            Partition = kv2.Key,
                            MaxNumberOfOffsets = 1,
                            Time = kv2.Value.NextOffsetExpected
                        });
                    }
                    kv2.Value.Postponed = false;
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
    }
}