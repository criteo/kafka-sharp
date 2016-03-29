// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using Kafka.Network;
using Kafka.Protocol;
using Kafka.Public;
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
        /// Get the current statistics of the cluster.
        /// </summary>
        IStatistics Statistics { get; }

        /// <summary>
        /// The logger used for feedback.
        /// </summary>
        ILogger Logger { get; }
    }

    class DevNullLogger : ILogger
    {
        public void LogInformation(string message)
        {
        }

        public void LogWarning(string message)
        {
        }

        public void LogError(string message)
        {
        }
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
            SeenTopic
        }

        [StructLayout(LayoutKind.Explicit)]
        struct MessageValue
        {
            [FieldOffset(0)]
            public TaskCompletionSource<RoutingTable> Promise;

            [FieldOffset(0)]
            public Tuple<TaskCompletionSource<int[]>, string> TopicPromise;

            [FieldOffset(0)]
            public Action NodeEventProcessing;

            [FieldOffset(0)]
            public string SeenTopic;
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
        private readonly string _seeds; // Addresses of the nodes to use for bootstrapping the cluster
        private readonly TimeoutScheduler _timeoutScheduler; // Timeout checker

        private HashSet<string> _seenTopics = new HashSet<string>(); // Gather seen topics for feedback filtering
        private Timer _refreshMetadataTimer; // Timer for periodic checking of metadata
        private RoutingTable _routingTable; // The current routing table
        private bool _started; // Cluster is active

        private double _resolution = 1000.0;

        public IStatistics Statistics { get; private set; }

        public IProduceRouter ProduceRouter { get; private set; }
        public IConsumeRouter ConsumeRouter { get; private set; }
        public ILogger Logger { get; private set; }

        public event Action<Exception> InternalError = _ => { };
        internal event Action<RoutingTable> RoutingTableChange = _ => { };

        internal long PassedThrough
        {
            get { return Statistics.Exited; }
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
            Logger = logger;
            Statistics = statistics ?? new Statistics();
            _timeoutScheduler = new TimeoutScheduler(configuration.ClientRequestTimeoutMs / 2);

            _pools = InitPools(Statistics, configuration);

            // Producer init
            ProduceRouter = producerFactory != null ? producerFactory() : new ProduceRouter(this, configuration, _pools.MessageBuffersPool);
            ProduceRouter.MessageExpired += (t, m) =>
            {
                Statistics.UpdateExpired();
                Statistics.UpdateExited();
            };
            ProduceRouter.MessagesAcknowledged += (t, c) =>
            {
                Statistics.UpdateSuccessfulSent(c);
                Statistics.UpdateExited(c);
                SignalSeenTopic(t);
            };
            ProduceRouter.MessageDiscarded += (t, m) =>
            {
                Statistics.UpdateDiscarded();
                Statistics.UpdateExited();
            };
            RoutingTableChange += ProduceRouter.ChangeRoutingTable;

            // Consumer init
            ConsumeRouter = consumerFactory != null ? consumerFactory() : new ConsumeRouter(this, configuration);
            ConsumeRouter.MessageReceived += _ => Statistics.UpdateReceived();
            if (ConsumeRouter is ConsumeRouter)
            {
                (ConsumeRouter as ConsumeRouter).InternalError +=
                    ex => Logger.LogError("An unexpected error occured in the consumer: " + ex);
            }
            RoutingTableChange += ConsumeRouter.ChangeRoutingTable;

            // Node factory
            var clientId = Encoding.UTF8.GetBytes(configuration.ClientId);
            var serializer = new Node.Serialization(configuration.SerializationConfig, _pools.RequestsBuffersPool, clientId, configuration.RequiredAcks, configuration.RequestTimeoutMs,
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
                throw new ArgumentException("Invalid seeds: " + _seeds);
            }
        }

        public Cluster SetResolution(double resolution)
        {
            _resolution = resolution;
            return this;
        }

        private static Pools InitPools(IStatistics statistics, Configuration configuration)
        {
            var pools = new Pools(statistics);
            pools.InitSocketBuffersPool(Math.Max(configuration.SendBufferSize, configuration.ReceiveBufferSize));
            pools.InitRequestsBuffersPool();
            var limit = configuration.MaxBufferedMessages;
            if (limit <= 0)
            {
                // Try to be smart
                if (configuration.BatchStrategy == BatchStrategy.Global)
                {
                    limit = 10 * configuration.ProduceBatchSize;
                }
                else
                {
                    limit = 100 * configuration.ProduceBatchSize;
                }
            }
            pools.InitMessageBuffersPool(limit, configuration.SerializationConfig.MaxMessagePoolChunkSize);

            return pools;
        }

        private void SignalSeenTopic(string topic)
        {
            if (!_seenTopics.Contains(topic))
            {
                _agent.Post(new ClusterMessage
                {
                    MessageType = MessageType.SeenTopic,
                    MessageValue = new MessageValue {SeenTopic = topic}
                });
            }
        }

        private void RefreshMetadata()
        {
            _agent.Post(new ClusterMessage {MessageType = MessageType.Metadata});
        }

        private NodeFactory DecorateFactory(NodeFactory nodeFactory)
        {
            return (h, p) => ObserveNode(nodeFactory(h, p));
        }

        // Connect all the INode events.
        private INode ObserveNode(INode node)
        {
            node.Dead += n => OnNodeEvent(() => ProcessDeadNode(n));
            node.ConnectionError += (n, e) => OnNodeEvent(() => ProcessNodeError(n, e));
            node.DecodeError += (n, e) => OnNodeEvent(() => ProcessDecodeError(n, e));
            node.RequestSent += _ => Statistics.UpdateRequestSent();
            node.ResponseReceived += _ => Statistics.UpdateResponseReceived();
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
            return node;
        }

        // Events processing are serialized on to the internal actor
        // to avoid concurrency management.
        private void OnNodeEvent(Action processing)
        {
            _agent.Post(new ClusterMessage
            {
                MessageType = MessageType.NodeEvent,
                MessageValue = new MessageValue { NodeEventProcessing = processing }
            });
        }

        private const string UnknownNode = "[Unknown]";

        private string GetNodeName(INode node)
        {
            BrokerMeta bm;
            return _nodes.TryGetValue(node, out bm) ? bm.ToString() : UnknownNode;
        }

        private void ProcessDecodeError(INode node, Exception exception)
        {
            Statistics.UpdateErrors();
            Logger.LogError(string.Format("A response could not be decoded for the node {0}: {1}", GetNodeName(node), exception));
        }

        private void ProcessNodeError(INode node, Exception exception)
        {
            Statistics.UpdateErrors();
            var ex = exception as TransportException;
            var n = GetNodeName(node);
            if (ex != null)
            {
                switch (ex.Error)
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
                        Logger.LogWarning(string.Format("Transport error to {0}: {1}", n, ex));
                        break;

                        // We cannot get there, but just in case and because dumb
                        // static checkers like Sonar are complaining:
                    default:
                        Logger.LogError("Unknown transport error");
                        InternalError(ex);
                        break;
                }
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
            foreach (var seed in _seeds.Split(new []{','}, StringSplitOptions.RemoveEmptyEntries))
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
                    MessageValue = new MessageValue {Promise = promise}
                }))
            {
                promise.SetCanceled();
            }
            return promise.Task;
        }

        public Task<int[]> RequireAllPartitionsForTopic(string topic)
        {
            var promise = new TaskCompletionSource<int[]>();
            SignalSeenTopic(topic);
            _agent.Post(new ClusterMessage
            {
                MessageType = MessageType.TopicMetadata,
                MessageValue = new MessageValue { TopicPromise = Tuple.Create(promise, topic) }
            });
            return promise.Task;
        }

        private void CheckNoMoreNodes()
        {
            if (_nodes.Count == 0)
            {
                Logger.LogError("All nodes are dead, retrying from bootstrap seeds.");
                BuildNodesFromSeeds();
            }
        }

        // Remove the node from current nodes and refresh the metadata.
        private void ProcessDeadNode(INode deadNode)
        {
            Statistics.UpdateNodeDead();
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
            CheckNoMoreNodes();
            RefreshMetadata();
        }

        private async Task ProcessMessage(ClusterMessage message)
        {
            switch (message.MessageType)
            {
                // Event occured on a node
                case MessageType.NodeEvent:
                    message.MessageValue.NodeEventProcessing();
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
                    var seen = new HashSet<string>(_seenTopics) {message.MessageValue.SeenTopic};
                    _seenTopics = seen;
                    break;

                default:
                    throw new ArgumentOutOfRangeException("message", "Invalid message type");
            }
        }

        private string TopicInfo(TopicMeta tm)
        {
            var buffer = new StringBuilder("[Metadata][Topic] ");
            buffer.Append(tm.TopicName).Append(":").Append(tm.ErrorCode);
            return tm.Partitions.Aggregate(
                buffer,
                (b, pm) => b.Append(" ").Append(string.Join(":", pm.Id, pm.Leader, pm.ErrorCode))
                ).ToString();
        }

        private INode ChooseRefreshNode()
        {
            return _nodes.Keys.ElementAt(_random.Next(_nodes.Count));
        }

        private async Task ProcessTopicMetadata(Tuple<TaskCompletionSource<int[]>, string> topicPromise)
        {
            var node = ChooseRefreshNode();
            try
            {
                var promise = topicPromise.Item1;
                var topic = topicPromise.Item2;
                Logger.LogInformation(string.Format("Fetching metadata for topic '{1}' from {0}...", node.Name, topic));
                var response = await node.FetchMetadata(topic);
                var tm = response.TopicsMeta.First(t => t.TopicName == topic);
                Logger.LogInformation(TopicInfo(tm));
                promise.SetResult(tm.Partitions.Select(p => p.Id).ToArray());
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
            if (_routingTable == null || _routingTable.LastRefreshed + _configuration.MinimumTimeBetweenRefreshMetadata < DateTime.UtcNow)
            {
                var node = ChooseRefreshNode();
                try
                {
                    Logger.LogInformation(string.Format("Fetching metadata from {0}...", node.Name));
                    var response = await node.FetchMetadata();
                    Logger.LogInformation("[Metadata][Brokers] " +
                                          string.Join("/", response.BrokersMeta.Select(bm => bm.ToString())));
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
                    CheckNoMoreNodes();
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
                }
                catch (TimeoutException ex)
                {
                    Logger.LogError(string.Format("Timeout while fetching metadata from {0}!", node.Name));
                    if (promise != null)
                    {
                        promise.SetException(ex);
                    }
                }
                catch (Exception ex)
                {
                    if (promise != null)
                    {
                        promise.SetCanceled();
                    }
                    InternalError(ex);
                }
            }
            else
            {
                if (promise != null)
                {
                    promise.SetResult(_routingTable);
                }
            }
        }

        private readonly HashSet<string> _tmpNewNodes = new HashSet<string>();
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
                routes[tm.TopicName] = tm.Partitions.Where(_ => Error.IsPartitionOkForClients(_.ErrorCode) && _.Leader >= 0).Select(_ => new Partition {Id = _.Id, Leader = _nodesById[_.Leader]}).OrderBy(p => p).ToArray();
            }
            _routingTable = new RoutingTable(routes);
        }
    }
}
