// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using Kafka.Network;
using Kafka.Protocol;
using Kafka.Public;
using Kafka.Public.Loggers;
using Kafka.Routing;

namespace Kafka.Cluster
{
    using ProducerFactory = Func<IProduceRouter>;
    using ConsumerFactory = Func<IConsumeRouter>;
    using NodeFactory = Func<string, int, INode>;

    /// <summary>
    /// Interface to a physical Kafka cluster.
    /// </summary>
    interface ICluster
    {
        /// <summary>
        /// Ask for the current routing table.
        /// </summary>
        Task<RoutingTable> RequireNewRoutingTable();

        /// <summary>
        /// Ask for the partitions of a given topic.
        /// </summary>
        Task<int[]> RequireAllPartitionsForTopic(string topic);

        /// <summary>
        /// Ask for the partitions of given topics.
        /// </summary>
        Task<IDictionary<string, int[]>> RequireAllPartitionsForTopics(IEnumerable<string> topics);

        /// <summary>
        /// Send an offset request to node, restricted to a single topic and partition,
        /// to obtain the earliest available offset for this topic / partition.
        /// </summary>
        /// <param name="topic"></param>
        /// <param name="partition"></param>
        /// <returns></returns>
        Task<long> GetEarliestOffset(string topic, int partition);

        /// <summary>
        /// Send an offset request to node, restricted to a single topic and partition,
        /// to obtain the latest available offset for this topic / partition.
        /// </summary>
        /// <param name="topic"></param>
        /// <param name="partition"></param>
        /// <returns></returns>
        Task<long> GetLatestOffset(string topic, int partition);

        /// <summary>
        /// Get the node responsible for managing the given group
        /// </summary>
        /// <param name="group"></param>
        /// <returns></returns>
        Task<INode> GetGroupCoordinator(string group);

        /// <summary>
        /// Get the current statistics of the cluster.
        /// </summary>
        IStatistics Statistics { get; }

        /// <summary>
        /// The logger used for feedback.
        /// </summary>
        ILogger Logger { get; }
    }

    class Cluster : ICluster
    {
        /// <summary>
        /// Message types of the inner actor.
        /// </summary>
        enum MessageType
        {
            Metadata,
            TopicMetadata,
            NodeEvent,
            SeenTopic,
            GroupCoordinator
        }

        [StructLayout(LayoutKind.Explicit)]
        struct MessageValue
        {
            [FieldOffset(0)]
            public TaskCompletionSource<RoutingTable> Promise;

            [FieldOffset(0)]
            public Tuple<TaskCompletionSource<IDictionary<string, int[]>>, IEnumerable<string>> TopicPromise;

            [FieldOffset(0)]
            public Func<Task> NodeEventProcessing;

            [FieldOffset(0)]
            public string SeenTopic;

            [FieldOffset(0)]
            public Tuple<TaskCompletionSource<INode>, string> CoordinatorPromise;
        }

        struct ClusterMessage
        {
            public MessageType MessageType;
            public MessageValue MessageValue;
            // TODO: add timestamp and use it to avoid send a request when result is more recent
        }

        private readonly Pools _pools;
        private readonly Configuration _configuration;
        private readonly NodeFactory _nodeFactory;
        private readonly Dictionary<INode, BrokerMeta> _nodes = new Dictionary<INode, BrokerMeta>();
        private readonly Dictionary<int, INode> _nodesById = new Dictionary<int, INode>();
        private readonly Dictionary<string, INode> _nodesByHostPort = new Dictionary<string, INode>();
        private readonly ActionBlock<ClusterMessage> _agent; // inner actor
        private readonly Random _random = new Random((int)(DateTime.Now.Ticks & 0xffffffff));
        private string _seeds; // Addresses of the nodes to use for bootstrapping the cluster
        private readonly Func<string> _seedsGetter; // Dynamic alternative to _seeds
        private readonly TimeoutScheduler _timeoutScheduler; // Timeout checker

        private HashSet<string> _seenTopics = new HashSet<string>(); // Gather seen topics for feedback filtering
        private Timer _refreshMetadataTimer; // Timer for periodic checking of metadata
        private RoutingTable _routingTable; // The current routing table
        private bool _started; // Cluster is active

        private double _resolution = 1000.0;
        private long _entered;
        private long _exited;

        public IStatistics Statistics { get; private set; }

        public IProduceRouter ProduceRouter { get; private set; }
        public IConsumeRouter ConsumeRouter { get; private set; }
        public ILogger Logger { get; private set; }

        public event Action<Exception> InternalError = _ => { };
        internal event Action<RoutingTable> RoutingTableChange = _ => { };

        internal long Entered
        {
            get { return Interlocked.Read(ref _entered); }
        }

        internal long PassedThrough
        {
            get { return Interlocked.Read(ref _exited); }
        }

        public Cluster() : this(new Configuration(), new DevNullLogger(), null, null, null)
        {
        }

        public Cluster(Configuration configuration, ILogger logger, IStatistics statistics = null)
            : this(configuration, logger, null, null, null, statistics)
        {
        }

        public Cluster(Configuration configuration, ILogger logger, NodeFactory nodeFactory, ProducerFactory producerFactory, ConsumerFactory consumerFactory, IStatistics statistics = null)
        {
            _configuration = configuration;
            _seeds = configuration.Seeds;
            _seedsGetter = configuration.SeedsGetter;
            Logger = logger;
            Statistics = statistics ?? new Statistics();
            _timeoutScheduler = new TimeoutScheduler(configuration.ClientRequestTimeoutMs / 2);

            _pools = InitPools(Statistics, configuration, logger);

            // Producer init
            ProduceRouter = producerFactory != null ? producerFactory() : new ProduceRouter(this, configuration, _pools.MessageBuffersPool);
            ProduceRouter.MessageExpired += (t, m) =>
            {
                Statistics.UpdateExpired();
                UpdateExited(1);
            };
            ProduceRouter.MessagesAcknowledged += (t, c) =>
            {
                Statistics.UpdateSuccessfulSent(c);
                UpdateExited(c);
                SignalSeenTopic(t);
            };
            ProduceRouter.MessageDiscarded += (t, m) =>
            {
                Statistics.UpdateDiscarded();
                UpdateExited(1);
            };
            RoutingTableChange += ProduceRouter.ChangeRoutingTable;
            ProduceRouter.BrokerTimeoutError += Statistics.UpdateBrokerTimeoutError;
            ProduceRouter.MessageReEnqueued += Statistics.UpdateMessageRetry;
            ProduceRouter.MessagePostponed += Statistics.UpdateMessagePostponed;

            // Consumer init
            ConsumeRouter = consumerFactory != null ? consumerFactory() : new ConsumeRouter(this, configuration);
            ConsumeRouter.MessageReceived += UpdateConsumerMessageStatistics;
            if (ConsumeRouter is ConsumeRouter)
            {
                (ConsumeRouter as ConsumeRouter).InternalError +=
                    ex => Logger.LogError("An unexpected error occured in the consumer: " + ex);
            }
            RoutingTableChange += ConsumeRouter.ChangeRoutingTable;

            // Node factory
            var clientId = Encoding.UTF8.GetBytes(configuration.ClientId);
            var serializer = new Node.Serialization(configuration.SerializationConfig, configuration.Compatibility, _pools.RequestsBuffersPool, clientId, configuration.RequiredAcks, configuration.RequestTimeoutMs,
                                                 configuration.CompressionCodec, configuration.FetchMinBytes, configuration.FetchMaxWaitTime);
            _nodeFactory = nodeFactory ??
                           ((h, p) =>
                            new Node(string.Format("[{0}:{1}]", h, p),
                                     () =>
                                     new Connection(h, p, ep => new RealSocket(ep), _pools.SocketBuffersPool, _pools.RequestsBuffersPool, configuration.SendBufferSize, configuration.ReceiveBufferSize),
                                     serializer,
                                     configuration,
                                     _timeoutScheduler,
                                     _resolution));
            _nodeFactory = DecorateFactory(_nodeFactory);

            // Inner actor
            _agent = new ActionBlock<ClusterMessage>(r => ProcessMessage(r),
                new ExecutionDataflowBlockOptions { TaskScheduler = configuration.TaskScheduler });

            // Bootstrap
            BuildNodesFromSeeds();
            if (_nodes.Count == 0)
            {
                var message = _seedsGetter != null ? "Invalid seeds Getter" : "Invalid seeds: " + _seeds;
                throw new ArgumentException(message);
            }
        }

        private void UpdateConsumerMessageStatistics(RawKafkaRecord kr)
        {
            Statistics.UpdateReceived();
            Statistics.UpdateConsumerLag(kr.Topic, kr.Lag);
        }

        public Cluster SetResolution(double resolution)
        {
            _resolution = resolution;
            return this;
        }

        private static Pools InitPools(IStatistics statistics, Configuration configuration, ILogger logger)
        {
            var pools = new Pools(statistics, logger);
            pools.InitSocketBuffersPool(Math.Max(configuration.SendBufferSize, configuration.ReceiveBufferSize));
            pools.InitRequestsBuffersPool();
            var limit = configuration.SerializationConfig.MaxPooledMessages;
            if (configuration.MaxBufferedMessages > 0 && limit > configuration.MaxBufferedMessages)
            {
                limit = configuration.MaxBufferedMessages;
            }
            pools.InitMessageBuffersPool(limit, configuration.SerializationConfig.MaxMessagePoolChunkSize);

            return pools;
        }

        internal void UpdateEntered()
        {
            Statistics.UpdateEntered();
            Interlocked.Increment(ref _entered);
        }

        private void UpdateExited(long nb)
        {
            Statistics.UpdateExited(nb);
            Interlocked.Add(ref _exited, nb);
        }

        private void SignalSeenTopic(string topic)
        {
            if (!_seenTopics.Contains(topic))
            {
                _agent.Post(new ClusterMessage
                {
                    MessageType = MessageType.SeenTopic,
                    MessageValue = new MessageValue { SeenTopic = topic }
                });
            }
        }

        private void RefreshMetadata()
        {
            _agent.Post(new ClusterMessage { MessageType = MessageType.Metadata });
        }

        private NodeFactory DecorateFactory(NodeFactory nodeFactory)
        {
            return (h, p) => ObserveNode(nodeFactory(h, p));
        }

        private void NodeMaxRequestReached(INode node)
        {
            // It is expected to reach the maximum capacity only when producing by burst
            // (i.e. strategy block until messages are sent).
            if (_configuration.ErrorStrategy == ErrorStrategy.Discard)
            {
                Logger.LogWarning(
                    string.Format(
                        "[Node] Maximum number of parallel request ({0}) to broker {1} reached.",
                        _configuration.MaxInFlightRequests, node.Name));
            }
        }

        // Connect all the INode events.
        private INode ObserveNode(INode node)
        {
            node.Dead += n => OnNodeEvent(() => ProcessDeadNode(n));
            node.ConnectionError += (n, e) => OnNodeEvent(() => ProcessNodeError(n, e));
            node.DecodeError += (n, e) => OnNodeEvent(() => ProcessDecodeError(n, e));
            node.InternalError += (n, e) => OnNodeEvent(() => ProcessNodeError(n, e));
            node.RequestSent += _ => Statistics.UpdateRequestSent();
            node.ResponseReceived += (n, l) => Statistics.UpdateResponseReceived(GetNodeId(n), l);
            node.ProduceBatchSent += (_, c, s) =>
            {
                Statistics.UpdateRawProduced(c);
                Statistics.UpdateRawProducedBytes(s);
            };
            node.FetchResponseReceived += (_, c, s) =>
            {
                Statistics.UpdateRawReceived(c);
                Statistics.UpdateRawReceivedBytes(s);
            };
            node.Connected +=
                n => OnNodeEvent(() => Logger.LogInformation(string.Format("Connected to {0}", GetNodeName(n))));
            node.ProduceAcknowledgement += (n, ack) => ProduceRouter.Acknowledge(ack);
            node.FetchAcknowledgement += (n, r) => ConsumeRouter.Acknowledge(r);
            node.OffsetAcknowledgement += (n, r) => ConsumeRouter.Acknowledge(r);
            node.NoMoreRequestSlot += n => NodeMaxRequestReached(n);
            node.RequestTimeout += n => Statistics.UpdateRequestTimeout(GetNodeId(n));
            return node;
        }

        // Events processing are serialized on to the internal actor
        // to avoid concurrency management.
        private void OnNodeEvent(Func<Task> processing)
        {
            _agent.Post(new ClusterMessage
            {
                MessageType = MessageType.NodeEvent,
                MessageValue = new MessageValue { NodeEventProcessing = processing }
            });
        }

        // Events processing are serialized on to the internal actor
        // to avoid concurrency management.
        private void OnNodeEvent(Action processing)
        {
            _agent.Post(new ClusterMessage
            {
                MessageType = MessageType.NodeEvent,
                MessageValue = new MessageValue { NodeEventProcessing = () =>
                {
                    processing();
                    return Task.CompletedTask;
                } }
            });
        }

        private const string UnknownNode = "[Unknown]";

        private string GetNodeName(INode node)
        {
            BrokerMeta bm;
            return _nodes.TryGetValue(node, out bm) ? bm.ToString() : UnknownNode;
        }

        private int GetNodeId(INode node)
        {
            BrokerMeta bm;
            return _nodes.TryGetValue(node, out bm) ? bm.Id : -1;
        }

        private void ProcessDecodeError(INode node, Exception exception)
        {
            Statistics.UpdateErrors();
            Logger.LogError(string.Format("A response could not be decoded for the node {0}: {1}", GetNodeName(node), exception));
        }

        private void ProcessNodeError(INode node, Exception exception)
        {
            Statistics.UpdateErrors();
            var transportException = exception as TransportException;
            var n = GetNodeName(node);
            if (transportException != null)
            {
                switch (transportException.Error)
                {
                    case TransportError.ConnectError:
                        Logger.LogWarning(string.Format("Failed to connect to {0}, retrying.", n));
                        break;

                    // Brokers very often close their connexions unilateraly,
                    // so we consider transport errors as "almost normal". "Real" errors
                    // will be logged with the proper severity level in case of
                    // dead nodes (see 'ProcessDeadNode')
                    case TransportError.ReadError:
                    case TransportError.WriteError:
                        if (transportException.InnerException is SocketException se && se.SocketErrorCode == SocketError.Success)
                        {
                            // It means we the connection was closed by remote
                            Logger.LogInformation($"{n} remotely closed idle connection.");
                        }
                        else
                        {
                            Logger.LogWarning($"Transport error to {n}: {transportException}");
                        }
                        break;

                    // We cannot get there, but just in case and because dumb
                    // static checkers like Sonar are complaining:
                    default:
                        Logger.LogError("Unknown transport error");
                        InternalError(transportException);
                        break;
                }
            }
            else if (exception is TimeoutException)
            {
                Logger.LogWarning(string.Format("Timeout in node {0}: {1}", n, exception));
            }
            else
            {
                Logger.LogError(string.Format("An error occured in node {0}: {1}", n, exception));
            }
        }

        private static string BuildKey(string host, int port)
        {
            return host + ':' + port;
        }

        private void BuildNodesFromSeeds()
        {
            _seeds = _seedsGetter?.Invoke() ?? _seeds;
            foreach (var seed in _seeds.Split(new[] { ',' }, StringSplitOptions.RemoveEmptyEntries))
            {
                var hostPort = seed.Split(':');
                var broker = new BrokerMeta { Host = hostPort[0], Port = int.Parse(hostPort[1]) };
                var node = _nodeFactory(broker.Host, broker.Port);
                _nodes[node] = broker;
                _nodesByHostPort[BuildKey(broker.Host, broker.Port)] = node;
            }
        }

        public void Start()
        {
            Logger.LogInformation("Bootstraping with " + _seeds);
            Logger.LogInformation("Compatibility with " + ((_configuration.Compatibility == Compatibility.V0_10_1) ? "Kafka 0.10+" : "Kafka 0.8.2"));
            Logger.LogInformation(
                string.Format("Configuration: {0} - {1} - {2} - max before overflow: {3} - produce batch size: {4} - client timeout: {5} ms",
                _configuration.OverflowStrategy == OverflowStrategy.Block ? "blocking" : "discard",
                _configuration.BatchStrategy == BatchStrategy.Global ? "global" : "by node",
                _configuration.ErrorStrategy == ErrorStrategy.Discard ? "discard on error" : "retry on error",
                _configuration.MaxBufferedMessages.ToString(),
                _configuration.ProduceBatchSize.ToString(),
                _configuration.ClientRequestTimeoutMs.ToString()));

            // log full configuration to make the difference between kafka drivers
            Logger.LogInformation(
                string.Format("               message Ttl: {0} s - max retry: {1} - refresh metadata interval {2} s",
                _configuration.MessageTtl.TotalSeconds.ToString(),
                _configuration.MaxRetry.ToString(),
                _configuration.RefreshMetadataInterval.TotalSeconds.ToString()
                ));
            Logger.LogInformation(
                string.Format("               min time between refresh metadata: {0} s - temporary ignore partition time: {1} s - produce buffering time: {2} ms",
                _configuration.MinimumTimeBetweenRefreshMetadata.TotalSeconds.ToString(),
                _configuration.TemporaryIgnorePartitionTime.TotalSeconds.ToString(),
                _configuration.ProduceBufferingTime.TotalMilliseconds.ToString()
                ));
            Logger.LogInformation(
                string.Format("               compression codec: {0} - receive buffer size: {1} - send buffer size: {2}",
                _configuration.CompressionCodec == CompressionCodec.Gzip ? "gzip" :
                    _configuration.CompressionCodec == CompressionCodec.Snappy ? "snappy" : "none",
                _configuration.ReceiveBufferSize.ToString(),
                _configuration.SendBufferSize.ToString()
                ));
            Logger.LogInformation(
                string.Format("               require Acks: {0} - min in sync replicas: {1} - retry if not enough replicas after append: {2}",
                _configuration.RequiredAcks == RequiredAcks.AllInSyncReplicas ? "all in sync replicas" :
                    _configuration.RequiredAcks == RequiredAcks.Leader ? "leader" : "none",
                _configuration.MinInSyncReplicas.ToString(),
                _configuration.RetryIfNotEnoughReplicasAfterAppend ? "yes" : "no"
                ));
            Logger.LogInformation(
                string.Format("               request time out: {0} ms - max in flight requests: {1} - max concurrency: {2}",
                _configuration.RequestTimeoutMs.ToString(),
                _configuration.MaxInFlightRequests.ToString(),
                _configuration.MaximumConcurrency.ToString()
                ));
            Logger.LogInformation(
                string.Format("               max postpone messages: {0} - max successive node errors: {1} - fetch max wait time: {2}",
                _configuration.MaxPostponedMessages.ToString(),
                _configuration.MaxSuccessiveNodeErrors.ToString(),
                _configuration.FetchMaxWaitTime.ToString()
                ));
            Logger.LogInformation(
                string.Format("               fetch min bytes: {0} - fetch message max bytes: {1} - consume batch size: {2}",
                _configuration.FetchMinBytes.ToString(),
                _configuration.FetchMessageMaxBytes.ToString(),
                _configuration.ConsumeBatchSize.ToString()
                ));


            RefreshMetadata();
            _refreshMetadataTimer = new Timer(
                _ => RefreshMetadata(), null,
                _configuration.RefreshMetadataInterval,
                _configuration.RefreshMetadataInterval);
            _started = true;
        }

        public async Task Stop()
        {
            if (!_started) return;
            _timeoutScheduler.Dispose();
            _refreshMetadataTimer.Dispose();
            await ConsumeRouter.Stop();
            await ProduceRouter.Stop();
            _agent.Complete();
            await _agent.Completion;
            await Task.WhenAll(_nodes.Keys.Select(n => n.Stop()));
            _started = false;
        }

        public Task<RoutingTable> RequireNewRoutingTable()
        {
            var promise = new TaskCompletionSource<RoutingTable>();
            if (!_agent.Post(new ClusterMessage
            {
                MessageType = MessageType.Metadata,
                MessageValue = new MessageValue { Promise = promise }
            }))
            {
                Logger.LogDebug("Agent failed to trigger metadata reload");
                promise.SetCanceled();
            }
            return promise.Task;
        }

        public async Task<long> GetEarliestOffset(string topic, int partition)
        {
            var rt = await RequireNewRoutingTable();
            return await rt.GetLeaderForPartition(topic, partition).GetEarliestOffset(topic, partition);
        }

        public async Task<long> GetLatestOffset(string topic, int partition)
        {
            var rt = await RequireNewRoutingTable();
            return await rt.GetLeaderForPartition(topic, partition).GetLatestOffset(topic, partition);
        }

        public async Task<int[]> RequireAllPartitionsForTopic(string topic)
        {
            try
            {
                return (await RequireAllPartitionsForTopics(new[] { topic }))[topic];
            }
            catch (KeyNotFoundException)
            {
                Logger.LogError(string.Format("Topic {0} cannot be found.", topic));
                throw;
            }
        }

        public Task<IDictionary<string, int[]>> RequireAllPartitionsForTopics(IEnumerable<string> topics)
        {
            var promise = new TaskCompletionSource<IDictionary<string, int[]>>();
            foreach (var topic in topics)
            {
                SignalSeenTopic(topic);
            }
            _agent.Post(new ClusterMessage
            {
                MessageType = MessageType.TopicMetadata,
                MessageValue = new MessageValue { TopicPromise = Tuple.Create(promise, topics) }
            });
            return promise.Task;
        }

        public Task<INode> GetGroupCoordinator(string group)
        {
            var promise = new TaskCompletionSource<INode>();
            _agent.Post(new ClusterMessage
            {
                MessageType = MessageType.GroupCoordinator,
                MessageValue = new MessageValue { CoordinatorPromise = Tuple.Create(promise, group) }
            });
            return promise.Task;
        }

        private async Task CheckNoMoreNodes()
        {
            if (_nodes.Count == 0)
            {
                Logger.LogError("All nodes are dead, retrying from bootstrap seeds.");
                BuildNodesFromSeeds();
            }

            var random = new Random();
            while (_nodes.Count == 0)
            {
                var timeToWaitMs = random.Next(20_000);
                Logger.LogError($"Bootstrapping from seeds failed. Retrying in {timeToWaitMs} ms.");
                await Task.Delay(TimeSpan.FromMilliseconds(timeToWaitMs));
                Logger.LogError("All nodes are dead, retrying from bootstrap seeds.");
                BuildNodesFromSeeds();
            }
        }

        // Remove the node from current nodes and refresh the metadata.
        private async Task ProcessDeadNode(INode deadNode)
        {
            Statistics.UpdateNodeDead(GetNodeId(deadNode));
            BrokerMeta m;
            if (!_nodes.TryGetValue(deadNode, out m))
            {
                Logger.LogWarning(string.Format("Kafka unknown node dead, the node makes itself known as: {0}.",
                                              deadNode.Name));
                return;
            }
            Logger.LogWarning(string.Format("Kafka node {0} is dead, refreshing metadata.", GetNodeName(deadNode)));
            if (_routingTable != null)
            {
                _routingTable = new RoutingTable(_routingTable, deadNode);
                RoutingTableChange(_routingTable);
            }
            _nodes.Remove(deadNode);
            deadNode.Stop();
            _nodesByHostPort.Remove(BuildKey(m.Host, m.Port));
            _nodesById.Remove(m.Id);
            await CheckNoMoreNodes();
            RefreshMetadata();
        }

        private async Task ProcessMessage(ClusterMessage message)
        {
            try
            {
                switch (message.MessageType)
                {
                    // Event occured on a node
                    case MessageType.NodeEvent:
                        await message.MessageValue.NodeEventProcessing();
                        break;

                    // Single topic metadata, this is generally for the consumer
                    case MessageType.TopicMetadata:
                        await ProcessTopicMetadata(message.MessageValue.TopicPromise);
                        break;

                    // Full metadata required
                    case MessageType.Metadata:
                        await ProcessFullMetadata(message.MessageValue.Promise);
                        break;

                    // New topic seen
                    case MessageType.SeenTopic:
                        var seen = new HashSet<string>(_seenTopics) { message.MessageValue.SeenTopic };
                        _seenTopics = seen;
                        break;

                    // Group coordinator
                    case MessageType.GroupCoordinator:
                        await ProcessGroupCoordinator(message.MessageValue.CoordinatorPromise);
                        break;

                    default:
                        throw new ArgumentOutOfRangeException("message", "Invalid message type");
                }
            }
            catch (Exception ex)
            {
                InternalError(ex);
            }
        }

        private string TopicInfo(TopicMeta tm)
        {
            var buffer = new StringBuilder("[Metadata][Topic] ");
            buffer.Append(tm.TopicName).Append(":").Append(tm.ErrorCode);
            return tm.Partitions.Aggregate(
                buffer,
                (b, pm) => b.Append(" ").Append(string.Join(":", pm.Id, pm.Leader, pm.ErrorCode, pm.Replicas.Length, pm.Isr.Length))
                ).ToString();
        }

        private INode ChooseRefreshNode()
        {
            return _nodes.Keys.ElementAt(_random.Next(_nodes.Count));
        }

        private async Task ProcessTopicMetadata(Tuple<TaskCompletionSource<IDictionary<string, int[]>>, IEnumerable<string>> topicPromise)
        {
            var node = ChooseRefreshNode();
            try
            {
                var promise = topicPromise.Item1;
                var topics = new HashSet<string>(topicPromise.Item2);
                Logger.LogInformation(string.Format("Fetching metadata for topics '{1}' from {0}...", node.Name, string.Join(", ", topics)));
                var response = await node.FetchMetadata(topics);
                var result = new Dictionary<string, int[]>();
                foreach (var tm in response.TopicsMeta.Where(t => topics.Contains(t.TopicName)))
                {
                    result.Add(tm.TopicName, tm.Partitions.Select(p => p.Id).ToArray());
                    Logger.LogInformation(TopicInfo(tm));
                }
                promise.SetResult(result);
            }
            catch (OperationCanceledException ex)
            {
                topicPromise.Item1.SetException(ex);
            }
            catch (TransportException ex)
            {
                topicPromise.Item1.SetException(ex);
            }
            catch (TimeoutException ex)
            {
                Logger.LogError(string.Format("Timeout while fetching metadata from {0}!", node.Name));
                topicPromise.Item1.SetException(ex);
            }
            catch (Exception ex)
            {
                topicPromise.Item1.SetCanceled();
                InternalError(ex);
            }
        }

        private async Task ProcessFullMetadata(TaskCompletionSource<RoutingTable> promise)
        {
            if (_routingTable == null || _routingTable.LastRefreshed + _configuration.MinimumTimeBetweenRefreshMetadata <= DateTime.UtcNow)
            {
                var node = ChooseRefreshNode();
                try
                {
                    Logger.LogInformation(string.Format("Fetching metadata from {0}...", node.Name));
                    var response = await node.FetchMetadata();
                    Logger.LogInformation("[Metadata][Brokers] " +
                                          string.Join("/", response.BrokersMeta.Select(bm => bm.ToString())));
                    if (response.BrokersMeta.Length == 0)
                    {
                        Logger.LogError($"The metadata that node {node.Name} gave us was empty. We need to fetch metadata again.");
                        throw new Exception("Metadata response with empty list of brokers");
                    }
                    foreach (var tm in response.TopicsMeta.Where(tm => _seenTopics.Contains(tm.TopicName)))
                    {
                        Logger.LogInformation(TopicInfo(tm));
                    }
                    ResponseToTopology(response);
                    ResponseToRoutingTable(response);
                    if (promise != null)
                    {
                        promise.SetResult(_routingTable);
                    }
                    RoutingTableChange(_routingTable);
                    await CheckNoMoreNodes();
                }
                catch (OperationCanceledException ex)
                {
                    if (promise != null)
                    {
                        promise.SetException(ex);
                    }
                }
                catch (TransportException ex)
                {
                    if (promise != null)
                    {
                        promise.SetException(ex);
                    }
                    RefreshMetadata();
                }
                catch (TimeoutException ex)
                {
                    Logger.LogError(string.Format("Timeout while fetching metadata from {0}!", node.Name));
                    if (promise != null)
                    {
                        promise.SetException(ex);
                    }
                    RefreshMetadata();
                }
                catch (Exception ex)
                {
                    if (promise != null)
                    {
                        promise.SetCanceled();
                    }
                    RefreshMetadata();
                    InternalError(ex);
                }
            }
            else
            {
                Logger.LogDebug("ProcessFullMetadata: no need to refresh the routing table");
                if (promise != null)
                {
                    promise.SetResult(_routingTable);
                }
            }
        }

        private async Task ProcessGroupCoordinator(Tuple<TaskCompletionSource<INode>, string> coordinatorPromise)
        {
            var node = ChooseRefreshNode();
            try
            {
                var promise = coordinatorPromise.Item1;
                var group = coordinatorPromise.Item2;
                Logger.LogInformation(string.Format("Getting coordinator for consumer group '{1}' from {0}...",
                    node.Name, group));
                var response = await node.GetGroupCoordinator(group);
                switch (response.ErrorCode)
                {
                    case ErrorCode.GroupAuthorizationFailed:
                        throw new KafkaGroupCoordinatorAuthorizationException { GroupId = group };

                    case ErrorCode.NoError:
                        promise.SetResult(_nodesById[response.CoordinatorId]);
                        break;

                    default:
                        promise.SetResult(null);
                        break;
                }
            }
            catch (OperationCanceledException ex)
            {
                coordinatorPromise.Item1.SetException(ex);
            }
            catch (TransportException ex)
            {
                coordinatorPromise.Item1.SetException(ex);
            }
            catch (TimeoutException ex)
            {
                Logger.LogError(string.Format("Timeout while fetching group coordinator from {0}!", node.Name));
                coordinatorPromise.Item1.SetException(ex);
            }
            catch (KeyNotFoundException)
            {
                // Node not found, refresh metadata
                RefreshMetadata();
                coordinatorPromise.Item1.SetCanceled();
            }
            catch (Exception
                ex)
            {
                coordinatorPromise.Item1.SetCanceled();
                InternalError(ex);
            }
        }

        private readonly
            HashSet<string> _tmpNewNodes = new HashSet<string>();
        private readonly HashSet<int> _tmpNewNodeIds = new HashSet<int>();

        private void ResponseToTopology(MetadataResponse response)
        {
            // New stuff
            foreach (var bm in response.BrokersMeta)
            {
                var hostPort = BuildKey(bm.Host, bm.Port);
                _tmpNewNodes.Add(hostPort);
                _tmpNewNodeIds.Add(bm.Id);
                INode node;
                if (!_nodesByHostPort.TryGetValue(hostPort, out node))
                {
                    node = _nodeFactory(bm.Host, bm.Port);
                    _nodesByHostPort[hostPort] = node;
                }
                if (!_nodes.ContainsKey(node))
                {
                    _nodes[node] = bm;
                }
                _nodes[node].Id = bm.Id;
                _nodesById[bm.Id] = node;
            }

            // Clean old
            var idToClean = _nodesById.Keys.Where(id => !_tmpNewNodeIds.Contains(id)).ToList();
            foreach (var id in idToClean)
            {
                _nodesById.Remove(id);
            }

            var hostToClean = _nodesByHostPort.Keys.Where(host => !_tmpNewNodes.Contains(host)).ToList();
            foreach (var host in hostToClean)
            {
                var node = _nodesByHostPort[host];
                _nodesByHostPort.Remove(host);
                _nodes.Remove(node);
                node.Stop();
            }

            _tmpNewNodes.Clear();
            _tmpNewNodeIds.Clear();
        }

        private void ResponseToRoutingTable(MetadataResponse response)
        {
            var routes = new Dictionary<string, Partition[]>();
            foreach (var tm in response.TopicsMeta.Where(_ => Error.IsPartitionOkForClients(_.ErrorCode)))
            {
                routes[tm.TopicName] = tm.Partitions.Where(
                    _ => Error.IsPartitionOkForClients(_.ErrorCode)
                         && _.Leader >= 0)
                    .Select(_ => new Partition { Id = _.Id, Leader = _nodesById[_.Leader], NbIsr = _.Isr.Length }).OrderBy(p => p).ToArray();
            }
            _routingTable = new RoutingTable(routes);
        }
    }
}
