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
using Kafka.Common;
using Kafka.Network;
using Kafka.Protocol;
using Kafka.Public;
using Kafka.Routing;

namespace Kafka.Cluster
{
    using ConnectionFactory = Func<IConnection>;
    using OffsetCommitResponse = CommonResponse<PartitionCommitData>;
    using OffsetFetchResponse = CommonResponse<PartitionOffsetData>;

    /// <summary>
    /// Interface to the cluster nodes. Nodes are responsible for batching requests,
    /// serializing them, deserializing responses, and passing responses to whoever
    /// wants them. They're responsible for maintaining a connection to a corresponding
    /// Kafka broker.
    /// </summary>
    interface INode
    {
        /// <summary>
        /// A name associated to this node.
        /// </summary>
        string Name { get; }

        /// <summary>
        /// Feed a produce request to the node for batching.
        /// </summary>
        /// <param name="message">A produce message to batch on the node.</param>
        /// <returns>False if the request is refused due to the node to be dead.</returns>
        bool Produce(ProduceMessage message);

        /// <summary>
        /// Post a batch of messages to be produced.
        /// </summary>
        /// <param name="batch"></param>
        bool Post(IBatchByTopicByPartition<ProduceMessage> batch);

        /// <summary>
        /// Feed a fetch request to the node for batching.
        /// </summary>
        /// <param name="message">Fetch request to batch.</param>
        /// <returns>False if the node is dead.</returns>
        bool Fetch(FetchMessage message);

        /// <summary>
        /// Post a batch of fetch requests to the node.
        /// </summary>
        /// <param name="batch"></param>
        bool Post(IBatchByTopic<FetchMessage> batch);

        /// <summary>
        /// Feed an offset request to the node for batching.
        /// </summary>
        /// <param name="message">Offset request to batch.</param>
        /// <returns>False if the node is dead.</returns>
        bool Offset(OffsetMessage message);

        /// <summary>
        /// Post a batch of offset requests to the node.
        /// </summary>
        /// <param name="batch"></param>
        bool Post(IBatchByTopic<OffsetMessage> batch);

        /// <summary>
        /// Send a fetch metadata request to the node.
        /// </summary>
        /// <returns>Metadata for all topics.</returns>
        Task<MetadataResponse> FetchMetadata();

        /// <summary>
        /// Send a fetch metadata request to node, restricted to a single topic.
        /// </summary>
        /// <param name="topic">Topic to fetch metadata for.</param>
        /// <returns>Metadata for a single topic.</returns>
        Task<MetadataResponse> FetchMetadata(IEnumerable<string> topic);

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
        /// Sends a GroupCoordinator request to the node, for the given group id.
        /// </summary>
        /// <param name="groupId"></param>
        /// <returns></returns>
        Task<GroupCoordinatorResponse> GetGroupCoordinator(string groupId);

        /// <summary>
        /// Sends a Heartbeat request to the node, using given group id, member id and generation.
        /// </summary>
        /// <param name="groupId"></param>
        /// <param name="generationId"></param>
        /// <param name="memberId"></param>
        /// <returns></returns>
        Task<ErrorCode> Heartbeat(string groupId, int generationId, string memberId);
        
        /// <summary>
        /// Sends a JoinGroup request to the node useing the given consumer group id.
        /// </summary>
        /// <param name="groupId"></param>
        /// <param name="memberId"></param>
        /// <param name="sessionTimeout"></param>
        /// <param name="rebalanceTimeout"></param>
        /// <param name="subscription"></param>
        /// <returns></returns>
        Task<JoinConsumerGroupResponse> JoinConsumerGroup(string groupId, string memberId, int sessionTimeout,
            int rebalanceTimeout, IEnumerable<string> subscription);

        /// <summary>
        /// Send a SyncGroup request to the node.
        /// </summary>
        /// <param name="groupId"></param>
        /// <param name="memberId"></param>
        /// <param name="generation"></param>
        /// <param name="assignments"></param>
        /// <returns></returns>
        Task<SyncConsumerGroupResponse> SyncConsumerGroup(string groupId, string memberId, int generation, IEnumerable<ConsumerGroupAssignment> assignments);

        /// <summary>
        /// Send a LeaveGroup request for the given consumer group.
        /// </summary>
        /// <param name="groupId"></param>
        /// <param name="memberId"></param>
        /// <returns></returns>
        Task<ErrorCode> LeaveGroup(string groupId, string memberId);

        /// <summary>
        /// Commit given offsets for the given topics / partitions.
        /// </summary>
        /// <param name="retentionTime"></param>
        /// <param name="topicsData"></param>
        /// <param name="groupId"></param>
        /// <param name="generation"></param>
        /// <param name="memberId"></param>
        /// <returns></returns>
        Task<OffsetCommitResponse> Commit(string groupId, int generation, string memberId, long retentionTime, IEnumerable<TopicData<OffsetCommitPartitionData>> topicsData);

        /// <summary>
        /// Fetch current offsets for the given topics / partitions.
        /// </summary>
        /// <param name="groupId"></param>
        /// <param name="topicsData"></param>
        /// <returns></returns>
        Task<OffsetFetchResponse> FetchOffsets(string groupId, IEnumerable<TopicData<PartitionAssignment>> topicsData);

        /// <summary>
        /// Stop all activities on the node (effectively marking it dead).
        /// </summary>
        /// <returns></returns>
        Task Stop();

        /// <summary>
        /// Some request has been sent on the wire.
        /// </summary>
        event Action<INode> RequestSent;

        /// <summary>
        /// A batch of produce message has been sent over the wire.
        /// </summary>
        event Action<INode, long /* batch # of messages */, long /* batch size in byte */> ProduceBatchSent;

        /// <summary>
        /// A fetch response has been received.
        /// </summary>
        event Action<INode, long /* # of messages */, long /* batch size in byte */> FetchResponseReceived;

        /// <summary>
        /// Some response has been received from the wire.
        /// </summary>
        event Action<INode, double /* # request latency in ms*/> ResponseReceived;

        /// <summary>
        /// Some error occured on the underlying connection.
        /// </summary>
        event Action<INode, Exception> ConnectionError;

        /// <summary>
        /// A response could not be deserialized.
        /// </summary>
        event Action<INode, Exception> DecodeError;

        /// <summary>
        /// An error occured inside the node
        /// </summary>
        event Action<INode, Exception> InternalError;

        /// <summary>
        /// The node is dead.
        /// </summary>
        event Action<INode> Dead;

        /// <summary>
        /// The node successfuly connected to the underlying broker.
        /// </summary>
        event Action<INode> Connected;

        /// <summary>
        /// No acknowledgement received for the request : time out
        /// </summary>
        event Action<INode> RequestTimeout;

        /// <summary>
        /// An acknowledgement for a produce request has been received.
        /// </summary>
        event Action<INode, ProduceAcknowledgement> ProduceAcknowledgement;

        /// <summary>
        /// An acknowledgement for a fetch request has been received.
        /// </summary>
        event Action<INode, CommonAcknowledgement<FetchResponse>> FetchAcknowledgement;

        /// <summary>
        /// An acknowledgement for an offset request has been received.
        /// </summary>
        event Action<INode, CommonAcknowledgement<CommonResponse<OffsetPartitionResponse>>> OffsetAcknowledgement;

        /// <summary>
        /// The node reached its maximum number of concurrent requests.
        /// </summary>
        event Action<INode> NoMoreRequestSlot;

    }

    /// <summary>
    /// Send/Receive requests to/from a Kafka node.
    /// Produce messages are buffered before sending.
    /// Buffering is handling with a Rx stream.
    /// Request sending is handled through an ActionBlock. Metadata requests are prioritized.
    /// Responses are handled through another ActionBlock thus allowing
    /// for full pipelining fo requests/response on one underlying connection.
    /// State change shared between send and receive actors is kept minimal (mainly correlation ids matching).
    /// Connection setup is always handled in the Send actor.
    /// </summary>
    sealed class Node : INode
    {
        #region Serialization / Deserialization

        /// <summary>
        /// This is pretty much just to allow injection for testing. This is a bit awkward
        /// not to encapsulate that into a wider "connection" object but we want
        /// to keep deserialization out of IO completion threads without introducing
        /// too much complexity.
        /// Anyway, remember it's for testing without having to code broker side ser/deser.
        /// </summary>
        internal interface ISerialization
        {
            ReusableMemoryStream SerializeProduceBatch(int correlationId, IEnumerable<IGrouping<string, IGrouping<int, ProduceMessage>>> batch);
            ReusableMemoryStream SerializeMetadataAllRequest(int correlationId);
            ReusableMemoryStream SerializeFetchBatch(int correlationId, IEnumerable<IGrouping<string, FetchMessage>> batch);
            ReusableMemoryStream SerializeOffsetBatch(int correlationId, IEnumerable<IGrouping<string, OffsetMessage>> batch);
            ReusableMemoryStream SerializeRequest(int correlationId, ISerializableRequest request, Basics.ApiVersion version);

            TResponse DeserializeResponse<TResponse>(int correlationId, ReusableMemoryStream data, Basics.ApiVersion version) where TResponse : IMemoryStreamSerializable, new();

            MetadataResponse DeserializeMetadataResponse(int correlationId, ReusableMemoryStream data);
            CommonResponse<TPartitionResponse> DeserializeCommonResponse<TPartitionResponse>(int correlationId,
                ReusableMemoryStream data, Basics.ApiVersion version) where TPartitionResponse : IMemoryStreamSerializable, new();
        }

        /// <summary>
        /// The Serialization for the real Kafka protocol.
        /// </summary>
        internal class Serialization : ISerialization
        {
            private readonly byte[] _clientId;
            private readonly short _requiredAcks;
            private readonly int _timeoutInMs;
            private readonly int _minBytes;
            private readonly int _maxWait;
            private readonly CompressionCodec _compressionCodec;
            private readonly SerializationConfig _serializationConfig;
            private readonly Pool<ReusableMemoryStream> _requestPool;
            private readonly Compatibility _compatibility;

            public Serialization(SerializationConfig serializationConfig, Compatibility compatibility, Pool<ReusableMemoryStream> requestPool, byte[] clientId, RequiredAcks requiredAcks, int timeoutInMs, CompressionCodec compressionCodec, int minBytes, int maxWait)
            {
                _clientId = clientId;
                _requiredAcks = (short) requiredAcks;
                _timeoutInMs = timeoutInMs;
                _minBytes = minBytes;
                _maxWait = maxWait;
                _compressionCodec = compressionCodec;
                _serializationConfig = serializationConfig ?? new SerializationConfig();
                _requestPool = requestPool;
                _compatibility = compatibility;
            }

            public ReusableMemoryStream SerializeMetadataAllRequest(int correlationId)
            {
                return new TopicRequest().Serialize(_requestPool.Reserve(), correlationId, _clientId, null, Basics.ApiVersion.Ignored);
            }

            public ReusableMemoryStream SerializeProduceBatch(int correlationId, IEnumerable<IGrouping<string, IGrouping<int, ProduceMessage>>> batch)
            {
                var produceRequest = new ProduceRequest
                {
                    RequiredAcks = _requiredAcks,
                    Timeout = _timeoutInMs,
                    TopicsData = batch.Select(gt => new TopicData<PartitionData>
                    {
                        TopicName = gt.Key,
                        PartitionsData = gt.Select(gp => new PartitionData
                        {
                            Partition = gp.Key,
                            Messages = gp.Select(pm => pm.Message),
                            CompressionCodec = _compressionCodec,
                        })
                    })
                };
                return produceRequest.Serialize(_requestPool.Reserve(), correlationId, _clientId, _serializationConfig, _compatibility == Compatibility.V0_8_2 ? Basics.ApiVersion.V0 : Basics.ApiVersion.V2);
            }

            public ReusableMemoryStream SerializeFetchBatch(int correlationId, IEnumerable<IGrouping<string, FetchMessage>> batch)
            {
                var fetchRequest = new FetchRequest
                {
                    MaxWaitTime = _maxWait,
                    MinBytes = _minBytes,
                    TopicsData = batch.Select(gt => new TopicData<FetchPartitionData>
                    {
                        TopicName = gt.Key,
                        PartitionsData = gt.Select(fm => new FetchPartitionData
                        {
                            Partition = fm.Partition,
                            FetchOffset = fm.Offset,
                            MaxBytes = fm.MaxBytes
                        })
                    })
                };
                return fetchRequest.Serialize(_requestPool.Reserve(), correlationId, _clientId, null, _compatibility == Compatibility.V0_8_2 ? Basics.ApiVersion.V0 : Basics.ApiVersion.V2);
            }

            public ReusableMemoryStream SerializeOffsetBatch(int correlationId, IEnumerable<IGrouping<string, OffsetMessage>> batch)
            {
                var offsetRequest = new OffsetRequest
                {
                    TopicsData = batch.Select(gt => new TopicData<OffsetPartitionData>
                    {
                        TopicName = gt.Key,
                        PartitionsData = gt.Select(om => new OffsetPartitionData
                        {
                            Partition = om.Partition,
                            Time = om.Time,
                            MaxNumberOfOffsets = om.MaxNumberOfOffsets
                        })
                    })
                };
                return offsetRequest.Serialize(_requestPool.Reserve(), correlationId, _clientId, null, _compatibility == Compatibility.V0_8_2 ? Basics.ApiVersion.V0 : Basics.ApiVersion.V1);
            }

            public ReusableMemoryStream SerializeRequest(int correlationId, ISerializableRequest request, Basics.ApiVersion version)
            {
                return request.Serialize(_requestPool.Reserve(), correlationId, _clientId, null, version);
            }

            public TResponse DeserializeResponse<TResponse>(int correlationId, ReusableMemoryStream data, Basics.ApiVersion version) where TResponse : IMemoryStreamSerializable, new()
            {
                var response = new TResponse();
                response.Deserialize(data, _serializationConfig, version);
                return response;
            }

            public MetadataResponse DeserializeMetadataResponse(int notUsed, ReusableMemoryStream data)
            {
                return MetadataResponse.Deserialize(data, null);
            }

            public CommonResponse<TPartitionResponse> DeserializeCommonResponse<TPartitionResponse>(int correlationId,
                ReusableMemoryStream data, Basics.ApiVersion version) where TPartitionResponse : IMemoryStreamSerializable, new()
            {
                var response = new CommonResponse<TPartitionResponse>();
                response.Deserialize(data, _serializationConfig, version);
                return response;
            }
        }

        #endregion

        #region Requests

        internal struct Requested<TRequest, TPromised> where TRequest : class
        {
            public TRequest Request;
            public TaskCompletionSource<TPromised> Promise;
        }

        [StructLayout(LayoutKind.Explicit)]
        internal struct RequestValue
        {
            [FieldOffset(0)]
            public IBatchByTopic<FetchMessage> FetchBatchRequest;

            [FieldOffset(0)]
            public IBatchByTopic<OffsetMessage> OffsetBatchRequest;

            [FieldOffset(0)]
            public IBatchByTopicByPartition<ProduceMessage> ProduceBatchRequest;

            [FieldOffset(0)]
            public Requested<IEnumerable<string>, MetadataResponse> MetadataRequest;

            [FieldOffset(0)]
            public Requested<IBatchByTopic<OffsetMessage>, long> OneOffsetRequest;

            [FieldOffset(0)]
            public Requested<GroupCoordinatorRequest, GroupCoordinatorResponse> GroupCoordinatorRequest;

            [FieldOffset(0)]
            public Requested<HeartbeatRequest, ErrorCode> HeartbeatRequest;

            [FieldOffset(0)]
            public Requested<LeaveGroupRequest, ErrorCode> LeaveGroupRequest;

            [FieldOffset(0)]
            public Requested<JoinConsumerGroupRequest, JoinConsumerGroupResponse> JoinConsumerGroupRequest;

            [FieldOffset(0)]
            public Requested<SyncConsumerGroupRequest, SyncConsumerGroupResponse> SyncConsumerGroupRequest;

            [FieldOffset(0)]
            public Requested<OffsetCommitRequest, OffsetCommitResponse> OffsetCommitRequest;

            [FieldOffset(0)]
            public Requested<OffsetFetchRequest, OffsetFetchResponse> OffsetFetchRequest;
        }

        /// <summary>
        /// Message type of the underlying send actor.
        /// </summary>
        internal enum RequestType
        {
            BatchedFetch,
            BatchedOffset,
            BatchedProduce,
            Metadata,
            SingleOffset,
            GroupCoordinator,
            OffsetCommit,
            OffsetFetch,
            Heartbeat,
            JoinConsumerGroup,
            SyncConsumerGroup,
            LeaveGroup
        }

        internal struct Request
        {
            public RequestType RequestType;
            public RequestValue RequestValue;

            public static Request CreateFetchRequest(IBatchByTopic<FetchMessage> value)
            {
                return new Request
                {
                    RequestType = RequestType.BatchedFetch,
                    RequestValue = new RequestValue { FetchBatchRequest = value }
                };
            }

            public static Request CreateOffsetRequest(IBatchByTopic<OffsetMessage> value)
            {
                return new Request
                {
                    RequestType = RequestType.BatchedOffset,
                    RequestValue = new RequestValue { OffsetBatchRequest = value }
                };
            }

            public static Request CreateProduceRequest(IBatchByTopicByPartition<ProduceMessage> value)
            {
                return new Request
                {
                    RequestType = RequestType.BatchedProduce,
                    RequestValue = new RequestValue { ProduceBatchRequest = value }
                };
            }

            public static Request CreateMetadataRequest(Requested<IEnumerable<string>, MetadataResponse> value)
            {
                return new Request
                {
                    RequestType = RequestType.Metadata,
                    RequestValue = new RequestValue { MetadataRequest = value }
                };
            }

            public static Request CreateOneOffsetRequest(Requested<IBatchByTopic<OffsetMessage>, long> value)
            {
                return new Request
                {
                    RequestType = RequestType.SingleOffset,
                    RequestValue = new RequestValue { OneOffsetRequest = value }
                };
            }

            public static Request CreateGroupCoordinatorRequest(string groupId, TaskCompletionSource<GroupCoordinatorResponse> promise)
            {
                return new Request
                {
                    RequestType = RequestType.GroupCoordinator,
                    RequestValue =
                        new RequestValue
                        {
                            GroupCoordinatorRequest =
                                new Requested<GroupCoordinatorRequest, GroupCoordinatorResponse>
                                {
                                    Request = new GroupCoordinatorRequest { GroupId = groupId },
                                    Promise = promise
                                }
                        }
                };
            }

            public static Request CreateHeartbeatRequest(string groupId, int generationId, string memberId,
                TaskCompletionSource<ErrorCode> promise)
            {
                return new Request
                {
                    RequestType = RequestType.Heartbeat,
                    RequestValue =
                        new RequestValue
                        {
                            HeartbeatRequest =
                                new Requested<HeartbeatRequest, ErrorCode>
                                {
                                    Request =
                                        new HeartbeatRequest
                                        {
                                            GroupId = groupId,
                                            MemberId = memberId,
                                            GenerationId = generationId
                                        },
                                    Promise = promise
                                }
                        }
                };
            }

            public static Request CreateLeaveGroupRequest(string groupId, string memberId,
                TaskCompletionSource<ErrorCode> promise)
            {
                return new Request
                {
                    RequestType = RequestType.LeaveGroup,
                    RequestValue =
                        new RequestValue
                        {
                            LeaveGroupRequest =
                                new Requested<LeaveGroupRequest, ErrorCode>
                                {
                                    Request =
                                        new LeaveGroupRequest
                                        {
                                            GroupId = groupId,
                                            MemberId = memberId
                                        },
                                    Promise = promise
                                }
                        }
                };
            }

            public static Request CreateJoinGroupRequest(string groupId, string memberId, int sessionTimeout, int rebalanceTimeout,
                IEnumerable<string> subscription, TaskCompletionSource<JoinConsumerGroupResponse> promise)
            {
                return new Request
                {
                    RequestType = RequestType.JoinConsumerGroup,
                    RequestValue = new RequestValue
                    {
                        JoinConsumerGroupRequest = new Requested<JoinConsumerGroupRequest, JoinConsumerGroupResponse>
                        {
                            Request = new JoinConsumerGroupRequest
                            {
                                GroupId = groupId,
                                SessionTimeout = sessionTimeout,
                                RebalanceTimeout = rebalanceTimeout,
                                MemberId = memberId,
                                Subscription = subscription
                            },
                            Promise = promise
                        }
                    }
                };
            }

            public static Request CreateSyncGroupRequest(string groupId, string memberId, int generation,
                IEnumerable<ConsumerGroupAssignment> assignments, TaskCompletionSource<SyncConsumerGroupResponse> promise)
            {
                return new Request
                {
                    RequestType = RequestType.SyncConsumerGroup,
                    RequestValue = new RequestValue
                    {
                        SyncConsumerGroupRequest = new Requested<SyncConsumerGroupRequest, SyncConsumerGroupResponse>
                        {
                            Request = new SyncConsumerGroupRequest
                            {
                                GroupId = groupId,
                                MemberId = memberId,
                                GenerationId = generation,
                                GroupAssignment = assignments
                            },
                            Promise = promise
                        }
                    }
                };
            }

            public static Request CreateOffsetCommitRequest(string consumerGroupId, int generation, string consumerId, long retentionTime,
                IEnumerable<TopicData<OffsetCommitPartitionData>> commit, TaskCompletionSource<OffsetCommitResponse> promise)
            {
                return new Request
                {
                    RequestType = RequestType.OffsetCommit,
                    RequestValue =
                        new RequestValue
                        {
                            OffsetCommitRequest =
                                new Requested<OffsetCommitRequest, OffsetCommitResponse>
                                {
                                    Promise = promise,
                                    Request = new OffsetCommitRequest
                                    {
                                        ConsumerGroupId = consumerGroupId,
                                        ConsumerGroupGenerationId = generation,
                                        ConsumerId = consumerId,
                                        RetentionTime = retentionTime,
                                        TopicsData = commit
                                    }
                                }
                        }
                };
            }

            public static Request CreateOffsetFetchRequest(string consumerGroupId, IEnumerable<TopicData<PartitionAssignment>> fetch,
                TaskCompletionSource<OffsetFetchResponse> promise)
            {
                return new Request
                {
                    RequestType = RequestType.OffsetFetch,
                    RequestValue =
                        new RequestValue
                        {
                            OffsetFetchRequest =
                                new Requested<OffsetFetchRequest, OffsetFetchResponse>
                                {
                                    Promise = promise,
                                    Request =
                                        new OffsetFetchRequest { ConsumerGroupId = consumerGroupId, TopicsData = fetch }
                                }
                        }
                };
            }
        }

        // Dummy class to signal request messages incoming.
        private struct Ping
        {
        }

        #endregion

        #region Responses

        struct ResponseData
        {
            public ReusableMemoryStream Data;
            public int CorrelationId;
        }

        struct ResponseException
        {
            public Exception Exception;
        }

        [StructLayout(LayoutKind.Explicit)]
        struct ResponseValue
        {
            [FieldOffset(0)]
            public ResponseException ResponseException;

            [FieldOffset(0)]
            public ResponseData ResponseData;
        }

        /// <summary>
        /// Message types for the underlying response handler actor.
        /// </summary>
        enum ResponseType
        {
            Data,
            Stop,
            Exception
        }

        struct Response
        {
            public ResponseType ResponseType;
            public IConnection Connection;
            public ResponseValue ResponseValue;
        }

        #endregion

        // Correlation id generator. Global is better for debugging.
        private static int _correlationId;

        private readonly ConnectionFactory _connectionFactory;
        private readonly Accumulator<ProduceMessage> _produceMessages; // incoming stream of produce messages
        private readonly Accumulator<FetchMessage> _fetchMessages; // incoming stream of fetch requests
        private readonly Accumulator<OffsetMessage> _offsetMessages; // incoming stream of offset requests
        private readonly ConcurrentQueue<Request> _metadata = new ConcurrentQueue<Request>(); // queue for metadata requests
        private readonly ConcurrentQueue<Request> _nonMetadata = new ConcurrentQueue<Request>(); // queue for all other batched requests

        private readonly ActionBlock<Ping> _requestQueue; // incoming request actor
        private readonly ActionBlock<Response> _responseQueue; // incoming response actor
        private readonly ISerialization _serialization;
        private readonly Configuration _configuration;
        private readonly TimeoutScheduler _timeoutScheduler;

        struct Pending
        {
            public int CorrelationId;
            public Request Request;
            public DateTime TimeStamp;
        }

        private readonly ConcurrentDictionary<IConnection, ConcurrentQueue<Pending>> _pendings = new ConcurrentDictionary<IConnection, ConcurrentQueue<Pending>>();

        private int _pendingsCount;
        private TaskCompletionSource<bool> _noMorePending;

        private IConnection _connection; // underlying connection to the broker
        private long _successiveErrors; // used to decide when a node is dead
        private readonly double _resolution;

        private void IncrementPendings()
        {
            Interlocked.Increment(ref _pendingsCount);
        }

        private void DecrementPendings()
        {
            if (Interlocked.Decrement(ref _pendingsCount) != 0) return;
            lock (_pendings)
            {
                if (_noMorePending != null)
                {
                    _noMorePending.TrySetResult(true);
                }
            }
        }

        private Accumulator<ProduceMessage> InitProduceSubject(int bufferingCount, TimeSpan bufferingTime)
        {
            var accumulator = new AccumulatorByTopicByPartition<ProduceMessage>(pm => pm.Topic, pm => pm.Partition, bufferingCount, bufferingTime);
            accumulator.NewBatch += b => Post(b);
            return accumulator;
        }

        private Accumulator<FetchMessage> InitFetchSubject(int bufferingCount, TimeSpan bufferingTime)
        {
            var accumulator = new AccumulatorByTopic<FetchMessage>(fm => fm.Topic, bufferingCount, bufferingTime);
            accumulator.NewBatch += b => Post(b);
            return accumulator;
        }

        private Accumulator<OffsetMessage> InitOffsetSubject(int bufferingCount, TimeSpan bufferingTime)
        {
            var accumulator = new AccumulatorByTopic<OffsetMessage>(om => om.Topic, bufferingCount, bufferingTime);
            accumulator.NewBatch += b => Post(b);
            return accumulator;
        }

        public string Name { get; internal set; }

        // For tests (no timeout)
        internal Node(string name, ConnectionFactory connectionFactory, ISerialization serialization,
            Configuration configuration, double resolution = 1000.0)
            : this(name, connectionFactory, serialization, configuration, new TimeoutScheduler(), resolution)
        {
        }

        public Node(string name, ConnectionFactory connectionFactory, ISerialization serialization,
            Configuration configuration, TimeoutScheduler timeoutScheduler, double resolution = 1000.0)
        {
            Name = name ?? "[Unknown]";
            _connectionFactory = connectionFactory;
            var options = new ExecutionDataflowBlockOptions
            {
                MaxMessagesPerTask = 1,
                TaskScheduler = configuration.TaskScheduler
            };
            _requestQueue = new ActionBlock<Ping>(r => ProcessRequest(r), options);
            _responseQueue = new ActionBlock<Response>(r => ProcessResponse(r), options);
            _resolution = resolution;
            if (configuration.BatchStrategy == BatchStrategy.ByNode)
            {
                _produceMessages = InitProduceSubject(configuration.ProduceBatchSize, configuration.ProduceBufferingTime);
                _fetchMessages = InitFetchSubject(configuration.ConsumeBatchSize, configuration.ConsumeBufferingTime);
                _offsetMessages = InitOffsetSubject(configuration.ConsumeBatchSize, configuration.ConsumeBufferingTime);
            }
            _serialization = serialization;
            _configuration = configuration;
            _timeoutScheduler = timeoutScheduler;
            timeoutScheduler.Register(this, CheckForTimeout);
        }

        public bool Produce(ProduceMessage message)
        {
            return !IsDead() && _produceMessages.Add(message);
        }

        public bool Fetch(FetchMessage message)
        {
            return !IsDead() && _fetchMessages.Add(message);
        }

        public bool Offset(OffsetMessage message)
        {
            return !IsDead() && _offsetMessages.Add(message);
        }

        public Task<MetadataResponse> FetchMetadata()
        {
            return FetchMetadata(null);
        }

        public Task<MetadataResponse> FetchMetadata(IEnumerable<string> topic)
        {
            var promise = new TaskCompletionSource<MetadataResponse>();

            if (!Post(Request.CreateMetadataRequest(new Requested<IEnumerable<string>, MetadataResponse> { Request = topic, Promise = promise })))
            {
                promise.SetCanceled();
            }

            return promise.Task;
        }

        public Task<long> GetEarliestOffset(string topic, int partition)
        {
            return GetOffset(topic, partition, Offsets.Earliest);
        }

        public Task<long> GetLatestOffset(string topic, int partition)
        {
            return GetOffset(topic, partition, Offsets.Latest);
        }

        private Task<long> GetOffset(string topic, int partition, long firstOrLast)
        {
            var promise = new TaskCompletionSource<long>();
            var batch = BatchByTopic<OffsetMessage>.New();
            batch.Add(topic, new OffsetMessage {MaxNumberOfOffsets = 1, Partition = partition, Time = firstOrLast, Topic = topic});
            if (!Post(Request.CreateOneOffsetRequest(new Requested<IBatchByTopic<OffsetMessage>, long> { Request = batch, Promise = promise })))
            {
                promise.SetCanceled();
            }

            return promise.Task;
        }

        public Task<GroupCoordinatorResponse> GetGroupCoordinator(string groupId)
        {
            var promise = new TaskCompletionSource<GroupCoordinatorResponse>();
            if (!Post(Request.CreateGroupCoordinatorRequest(groupId, promise)))
            {
                promise.SetCanceled();
            }
            return promise.Task;
        }

        public Task<ErrorCode> Heartbeat(string groupId, int generationId, string memberId)
        {
            var promise = new TaskCompletionSource<ErrorCode>();
            if (!Post(Request.CreateHeartbeatRequest(groupId, generationId, memberId, promise)))
            {
                promise.SetCanceled();
            }
            return promise.Task;
        }

        public Task<JoinConsumerGroupResponse> JoinConsumerGroup(string groupId, string memberId, int sessionTimeout,
            int rebalanceTimeout, IEnumerable<string> subscription)
        {
            var promise = new TaskCompletionSource<JoinConsumerGroupResponse>();
            if (!Post(Request.CreateJoinGroupRequest(groupId, memberId, sessionTimeout, rebalanceTimeout, subscription, promise)))
            {
                promise.SetCanceled();
            }
            return promise.Task;
        }

        public Task<SyncConsumerGroupResponse> SyncConsumerGroup(string groupId, string memberId, int generation,
            IEnumerable<ConsumerGroupAssignment> assignments)
        {
            var promise = new TaskCompletionSource<SyncConsumerGroupResponse>();
            if (!Post(Request.CreateSyncGroupRequest(groupId, memberId, generation, assignments, promise)))
            {
                promise.SetCanceled();
            }
            return promise.Task;
        }

        public Task<ErrorCode> LeaveGroup(string groupId, string memberId)
        {
            var promise = new TaskCompletionSource<ErrorCode>();
            if (!Post(Request.CreateLeaveGroupRequest(groupId, memberId, promise)))
            {
                promise.SetCanceled();
            }
            return promise.Task;
        }

        public Task<OffsetCommitResponse> Commit(string groupId, int generation, string memberId, long retentionTime, IEnumerable<TopicData<OffsetCommitPartitionData>> topicsData)
        {
            var promise = new TaskCompletionSource<OffsetCommitResponse>();
            if (!Post(Request.CreateOffsetCommitRequest(groupId, generation, memberId, retentionTime, topicsData, promise)))
            {
                promise.SetCanceled();
            }
            return promise.Task;
        }

        public Task<OffsetFetchResponse> FetchOffsets(string groupId, IEnumerable<TopicData<PartitionAssignment>> topicsData)
        {
            var promise = new TaskCompletionSource<OffsetFetchResponse>();
            if (!Post(Request.CreateOffsetFetchRequest(groupId, topicsData, promise)))
            {
                promise.SetCanceled();
            }
            return promise.Task;
        }

        private int _stopped;

        public async Task Stop()
        {
            if (Interlocked.Increment(ref _stopped) > 1) return; // already stopped

            // Flush accumulators
            if (_configuration.BatchStrategy == BatchStrategy.ByNode)
            {
                _produceMessages.Dispose();
                _fetchMessages.Dispose();
                _offsetMessages.Dispose();
            }

            // Complete the incoming queue (pending batches have all been posted)
            // and wait for everything to be sent.
            _requestQueue.Complete();
            await _requestQueue.Completion;

            // Now we have to wait for pending requests to be acknowledged or
            // canceled.
            lock (_pendings)
            {
                if (_pendingsCount != 0)
                {
                    _noMorePending = new TaskCompletionSource<bool>();
                }
            }
            if (_noMorePending != null)
            {
                await Task.WhenAny(_noMorePending.Task, Task.Delay(3 * _configuration.ClientRequestTimeoutMs));
            }

            _responseQueue.Complete();
            await _responseQueue.Completion;

            // Stop checking timeouts.
            _timeoutScheduler.Unregister(this);

            // Clean up remaining pending requests (in case of time out)
            if (_pendingsCount != 0)
            {
                foreach (var connection in _pendings.Keys.ToArray())
                {
                    ClearCorrelationIds(connection);
                }
            }

            if (_connection != null)
            {
                ClearConnection(_connection);
                _connection = null;
            }
        }

        // We consider ourself dead if we see too much successive errors
        // on the connection.
        private bool IsDead()
        {
            return Interlocked.Read(ref _successiveErrors) > _configuration.MaxSuccessiveNodeErrors;
        }

        private void ForwardResponse(IConnection connection, int correlationId, ReusableMemoryStream data)
        {
            _responseQueue.Post(new Response
            {
                ResponseType = ResponseType.Data,
                Connection = connection,
                ResponseValue = new ResponseValue
                {
                    ResponseData = new ResponseData {CorrelationId = correlationId, Data = data}
                }
            });
        }

        private void ClearConnection(IConnection connection)
        {
            connection.ReceiveError -= HandleConnectionError;
            connection.Response -= ForwardResponse;
            connection.Dispose();
        }

        // Initialize a new underlying connection:
        //   If we're currently encountering errors, we wait a little before
        //   retrying to connect, then we create the connection and subscribe
        //   to its events and connect.
        private async Task<IConnection> InitConnection()
        {
            await Task.Delay(TimeSpan.FromMilliseconds(_resolution*_successiveErrors));
            var connection = _connectionFactory();
            connection.ReceiveError += HandleConnectionError;
            connection.Response += ForwardResponse;
            await connection.ConnectAsync();
            OnConnected();
            _connection = connection;
            _pendings.TryAdd(connection, new ConcurrentQueue<Pending>());
            return connection;
        }

        public bool Post(IBatchByTopicByPartition<ProduceMessage> batch)
        {
            return Post(Request.CreateProduceRequest(batch));
        }

        public bool Post(IBatchByTopic<FetchMessage> batch)
        {
            return Post(Request.CreateFetchRequest(batch));
        }

        public bool Post(IBatchByTopic<OffsetMessage> batch)
        {
            return Post(Request.CreateOffsetRequest(batch));
        }

        // Post a message to the underlying request actor
        private bool Post(Request request)
        {
            if (request.RequestType == RequestType.Metadata)
            {
                _metadata.Enqueue(request);
            }
            else
            {
                _nonMetadata.Enqueue(request);
            }
            return _requestQueue.Post(new Ping());
        }

        // Serialize a request
        private ReusableMemoryStream Serialize(int correlationId, Request request)
        {
            switch (request.RequestType)
            {
                case RequestType.Metadata:
                    return _serialization.SerializeMetadataAllRequest(correlationId);

                case RequestType.BatchedProduce:
                    return _serialization.SerializeProduceBatch(correlationId,
                        request.RequestValue.ProduceBatchRequest);

                case RequestType.BatchedFetch:
                    return _serialization.SerializeFetchBatch(correlationId,
                        request.RequestValue.FetchBatchRequest);

                case RequestType.BatchedOffset:
                    return _serialization.SerializeOffsetBatch(correlationId,
                        request.RequestValue.OffsetBatchRequest);

                case RequestType.SingleOffset:
                    return _serialization.SerializeOffsetBatch(correlationId,
                        request.RequestValue.OneOffsetRequest.Request);

                case RequestType.GroupCoordinator:
                    return _serialization.SerializeRequest(correlationId,
                        request.RequestValue.GroupCoordinatorRequest.Request, Basics.ApiVersion.V0);

                case RequestType.Heartbeat:
                    return _serialization.SerializeRequest(correlationId,
                        request.RequestValue.HeartbeatRequest.Request, Basics.ApiVersion.V0);

                case RequestType.LeaveGroup:
                    return _serialization.SerializeRequest(correlationId,
                        request.RequestValue.LeaveGroupRequest.Request, Basics.ApiVersion.V0);

                case RequestType.JoinConsumerGroup:
                    return _serialization.SerializeRequest(correlationId,
                        request.RequestValue.JoinConsumerGroupRequest.Request,
                        _configuration.Compatibility == Compatibility.V0_8_2
                            ? Basics.ApiVersion.V0
                            : Basics.ApiVersion.V1);

                case RequestType.SyncConsumerGroup:
                    return _serialization.SerializeRequest(correlationId,
                        request.RequestValue.SyncConsumerGroupRequest.Request, Basics.ApiVersion.V0);

                case RequestType.OffsetCommit:
                    return _serialization.SerializeRequest(correlationId,
                        request.RequestValue.OffsetCommitRequest.Request, Basics.ApiVersion.V2);

                case RequestType.OffsetFetch:
                    return _serialization.SerializeRequest(correlationId,
                        request.RequestValue.OffsetFetchRequest.Request, Basics.ApiVersion.V1);

                default: // Compiler requires a default case, even if all possible cases are already handled
                    throw new ArgumentOutOfRangeException("request", "Non valid RequestType enum value: " + request.RequestType);
            }
        }

        private bool CheckAckRequired(Request request)
        {
            return _configuration.RequiredAcks != RequiredAcks.None || request.RequestType != RequestType.BatchedProduce;
        }

        private void RepostOrDrain(Request request)
        {
            if (!Post(request))
            {
                Drain(request, false);
            }
        }

        /// <summary>
        /// Process messages received on the request actor. Metadata
        /// requests are prioritized. We connect to the underlying connection
        /// if needed, serialize the request and send it.
        /// If the node is dead the request is canceled appropriately (see Drain).
        /// </summary>
        private async Task ProcessRequest(Ping dummy)
        {
            // Prioritize metadata
            Request request;
            if (!_metadata.TryDequeue(out request))
            {
                _nonMetadata.TryDequeue(out request);
            }

            if (IsDead())
            {
                Drain(request, false);
                return;
            }

            if (!RequestSlotAvailable() && request.RequestType == RequestType.BatchedProduce)
            {
                OnNoMoreRequestSlot();
                if (request.RequestType == RequestType.BatchedProduce)
                {
                    // Reroute incoming produce requests to reduce the load on slow nodes
                    Drain(request, false);
                    return;
                }
            }

            var connection = _connection;
            try
            {
                // Connect if needed
                if (connection == null)
                {
                    connection = await InitConnection();
                }

                // Get a new correlation id
                int correlationId = Interlocked.Increment(ref _correlationId);

                // Serialize & send
                using (var data = Serialize(correlationId, request))
                {
                    var acked = CheckAckRequired(request);
                    if (acked)
                    {
                        await ReadyToSendRequest();

                        // locking is necessary here to avoid a race when an error occured
                        // before we got a chance  to populate the pending queue (the queue could
                        // disappear just after having got it).
                        // TODO: find a cleaner way of doing this (no messy locking)
                        lock (_pendings)
                        {
                            ConcurrentQueue<Pending> pendingsQueue;
                            if (!_pendings.TryGetValue(connection, out pendingsQueue))
                            {
                                // Means a receive error just after connect, just repost the message
                                // since we never sent anything on the connection
                                RepostOrDrain(request);
                                return;
                            }
                            pendingsQueue.Enqueue(new Pending
                            {
                                CorrelationId = correlationId,
                                Request = request,
                                TimeStamp = DateTime.UtcNow
                            });
                            IncrementPendings();
                        }
                    }
                    await connection.SendAsync(correlationId, data, acked);
                    Interlocked.Exchange(ref _successiveErrors, 0);
                    OnRequestSent();
                    if (request.RequestType == RequestType.BatchedProduce)
                    {
                        OnProduceBatchSent(request.RequestValue.ProduceBatchRequest.Count, data.Length);
                    }
                    if (!acked)
                    {
                        // simulate a response
                        OnProduceAcknowledgement(BuildUnackedProduceResponseFromOriginal(request.RequestValue.ProduceBatchRequest));
                    }
                }
            }
            catch (TransportException ex)
            {
                // In case of connection error, we repost
                // the request, which will retry to connect eventually.
                if (ex.Error == TransportError.ConnectError)
                {
                    RepostOrDrain(request);
                }
                HandleConnectionError(connection, ex);
            }
            catch (Exception ex)
            {
                HandleConnectionError(connection, ex);

                // HandleConnectionError will only clean the pending queue,
                // we must drain any request that never had a chance to being put in
                // the pending queue because nothing was really sent over the wire
                // (so even produce requests with no ack required are fair game for a retry).
                if (connection == null || !CheckAckRequired(request))
                {
                    Drain(request, false);
                }
            }
        }

        /// <summary>
        /// Check if we've reached the max number of pending requests allowed
        /// </summary>
        /// <returns></returns>
        private bool RequestSlotAvailable()
        {
            if (_configuration.MaxInFlightRequests > 0)
            {
                return _pendingsCount < _configuration.MaxInFlightRequests;
            }
            return true;
        }

        private Task ReadyToSendRequest()
        {
            return RequestSlotAvailable() ? Task.CompletedTask : WaitRequestSlot();
        }

        private async Task WaitRequestSlot()
        {
            // Not the best way to do that but it's the simplest for now.
            // TODO: do a full event driven async wait
            var limit = DateTime.UtcNow.AddMilliseconds(_configuration.ClientRequestTimeoutMs * 2);
            while (_pendingsCount >= _configuration.MaxInFlightRequests)
            {
                // Avoid infinite loop in strange cases (namely an overlooked bug)
                if (DateTime.UtcNow >= limit)
                {
                    throw new TimeoutException("Too many pending requests for too much time!");
                }
                await Task.Delay(TimeSpan.FromMilliseconds(15));
            }
        }

        /// <summary>
        /// Process a response. A response is either some exception or data
        /// from the underlying connection.
        /// </summary>
        private void ProcessResponse(Response response)
        {
            try
            {
                switch (response.ResponseType)
                {
                    case ResponseType.Stop:
                        CleanUpConnection(response.Connection);
                        break;

                    case ResponseType.Exception:
                        CleanUpConnection(response.Connection, response.ResponseValue.ResponseException.Exception);
                        break;

                    case ResponseType.Data:
                        ConcurrentQueue<Pending> pendings;
                        if (!_pendings.TryGetValue(response.Connection, out pendings))
                        {
                            // Some race condition occured between send and receive error.
                            // It can theoretically happen but should be very rare.
                            // In that case we do nothing, the error is already being taken care of.
                            return;
                        }
                        Pending pending;
                        if (!pendings.TryDequeue(out pending))
                        {
                            // The request is not found in the correlation queue.
                            // This means that an error has previously occured and
                            // we already took care of that.
                            return;
                        }

                        DecrementPendings();

                        using (var data = response.ResponseValue.ResponseData.Data)
                        {
                            if (pending.CorrelationId != response.ResponseValue.ResponseData.CorrelationId)
                            {
                                // This is an error but it should not happen because the underlying connection
                                // is already supposed to have managed that.
                                CleanUpConnection(response.Connection);
                                return;
                            }

                            var latencyMs = DateTime.UtcNow.Subtract(pending.TimeStamp).TotalMilliseconds;
                            OnResponseReceived(latencyMs);

                            switch (pending.Request.RequestType)
                            {
                                case RequestType.BatchedProduce:
                                    ProcessProduceResponse(pending.CorrelationId, data,
                                        pending.Request.RequestValue.ProduceBatchRequest);

                                    break;

                                case RequestType.Metadata:
                                    ProcessMetadataResponse(pending.CorrelationId, data,
                                        pending.Request.RequestValue.MetadataRequest);
                                    break;

                                case RequestType.SingleOffset:
                                    ProcessSingleOffsetResponse(pending.CorrelationId, data,
                                        pending.Request.RequestValue.OneOffsetRequest);
                                    break;

                                case RequestType.BatchedFetch:
                                    ProcessFetchResponse(pending.CorrelationId, data,
                                        pending.Request.RequestValue.FetchBatchRequest);
                                    break;

                                case RequestType.BatchedOffset:
                                    ProcessOffsetResponse(pending.CorrelationId, data,
                                        pending.Request.RequestValue.OffsetBatchRequest);
                                    break;

                                case RequestType.GroupCoordinator:
                                    ProcessPromiseResponse(pending.CorrelationId, data,
                                        pending.Request.RequestValue.GroupCoordinatorRequest);
                                    break;

                                case RequestType.JoinConsumerGroup:
                                    ProcessPromiseResponse(pending.CorrelationId, data,
                                        pending.Request.RequestValue.JoinConsumerGroupRequest);
                                    break;

                                case RequestType.SyncConsumerGroup:
                                    ProcessPromiseResponse(pending.CorrelationId, data,
                                        pending.Request.RequestValue.SyncConsumerGroupRequest);
                                    break;

                                case RequestType.OffsetCommit:
                                    ProcessPromiseResponse(pending.CorrelationId, data,
                                        pending.Request.RequestValue.OffsetCommitRequest);
                                    break;

                                case RequestType.OffsetFetch:
                                    ProcessPromiseResponse(pending.CorrelationId, data,
                                        pending.Request.RequestValue.OffsetFetchRequest);
                                    break;

                                case RequestType.Heartbeat:
                                    ProcessSimpleResponse(pending.CorrelationId, data,
                                        pending.Request.RequestValue.HeartbeatRequest);
                                    break;

                                case RequestType.LeaveGroup:
                                    ProcessSimpleResponse(pending.CorrelationId, data,
                                        pending.Request.RequestValue.LeaveGroupRequest);
                                    break;
                            }
                        }
                        break;
                }
            }
            catch (Exception ex)
            {
                OnInternalError(ex);
            }
        }

        // Clean up the mess
        private void CleanUpConnection(IConnection connection, Exception exception = null)
        {
            try
            {
                ClearConnection(connection);
            }
            catch (Exception ex)
            {
                OnConnectionError(ex);
            }

            // Make sure we will always clean the connection's pending requests
            ClearCorrelationIds(connection, exception);
        }

        // Preallocated responses
        private static readonly long[] NoOffset = new long[0];

        // Build an empty response from a given Fetch request with error set to LocalError.
        private static FetchResponse BuildEmptyFetchResponseFromOriginal(
            IBatchByTopic<FetchMessage> originalRequest)
        {
            return new FetchResponse
            {
                FetchPartitionResponse =
                    new CommonResponse<FetchPartitionResponse>
                    {
                        TopicsResponse =
                            originalRequest.Select(
                                b =>
                                    new TopicData<FetchPartitionResponse>
                                    {
                                        TopicName = b.Key,
                                        PartitionsData =
                                            b.Select(
                                                fm =>
                                                    new FetchPartitionResponse
                                                    {
                                                        ErrorCode = ErrorCode.LocalError,
                                                        HighWatermarkOffset = -1,
                                                        Partition = fm.Partition,
                                                        Messages = ResponseMessageListPool.EmptyList
                                                    }).ToArray()
                                    })
                                .ToArray()
                    }
            };
        }

        // Build an empty response from a given Offset request with error set to LocalError.
        private static CommonResponse<OffsetPartitionResponse> BuildEmptyOffsetResponseFromOriginal(
            IBatchByTopic<OffsetMessage> originalRequest)
        {
            return new CommonResponse<OffsetPartitionResponse>
            {
                TopicsResponse = originalRequest.Select(b => new TopicData<OffsetPartitionResponse>
                {
                    TopicName = b.Key,
                    PartitionsData = b.Select(om => new OffsetPartitionResponse
                    {
                        ErrorCode = ErrorCode.LocalError,
                        Partition = om.Partition,
                        Offsets = NoOffset
                    }).ToArray()
                }).ToArray()
            };
        }

        // Build a response for produce requests sent in unacked mode.
        private static ProduceAcknowledgement BuildUnackedProduceResponseFromOriginal(
            IBatchByTopicByPartition<ProduceMessage> originalRequest)
        {
            return new ProduceAcknowledgement
            {
                OriginalBatch = originalRequest,
                ProduceResponse =
                    new ProduceResponse
                    {
                        ProducePartitionResponse =
                            new CommonResponse<ProducePartitionResponse>
                            {
                                TopicsResponse =
                                    originalRequest.Select(
                                        b =>
                                            new TopicData<ProducePartitionResponse>
                                            {
                                                TopicName = b.Key,
                                                PartitionsData =
                                                    b.Select(
                                                        fm =>
                                                            new ProducePartitionResponse
                                                            {
                                                                ErrorCode = ErrorCode.NoError,
                                                                Partition = fm.Key,
                                                            }).ToArray()
                                            }).ToArray()
                            }
                    },
                ReceiveDate = DateTime.UtcNow
            };
        }

        /// <summary>
        /// Deserialize a Fetch response and acknowledge it. In case of deserialization
        /// we build an empty response matching the original request. There's no need
        /// to pass back the original request, a consumer is supposed to maintain state
        /// on the partitions it's fetching from because empty responses from the broker
        /// are perfectly valid.
        /// </summary>
        private void ProcessFetchResponse(int correlationId, ReusableMemoryStream responseData,
            IBatchByTopic<FetchMessage> originalRequest)
        {
            var response = new CommonAcknowledgement<FetchResponse> {ReceivedDate = DateTime.UtcNow};
            try
            {
                response.Response = _serialization.DeserializeResponse<FetchResponse>(correlationId, responseData,
                    _configuration.Compatibility == Compatibility.V0_8_2 ? Basics.ApiVersion.V0 : Basics.ApiVersion.V2);
            }
            catch (Exception ex)
            {
                OnDecodeError(ex);
                response.Response = BuildEmptyFetchResponseFromOriginal(originalRequest);
            }
            originalRequest.Dispose();

            var tr = response.Response.FetchPartitionResponse.TopicsResponse;
            OnFetchResponseReceived(
                tr.Aggregate(0L, (l1, td) => td.PartitionsData.Aggregate(l1, (l2, pd) => l2 + pd.Messages.Count)),
                responseData.Length);
            OnMessagesReceived(response);
        }

        /// <summary>
        /// Deserialize an Offset response and acknowledge it. In case of deserialization
        /// we build an empty response matching the original request. There's no need
        /// to pass back the original request, a consumer is supposed to maintain state
        /// on the partitions it's requiring offset from because empty responses from the broker
        /// are perfectly valid (leader change).
        /// </summary>
        private void ProcessOffsetResponse(int correlationId, ReusableMemoryStream responseData,
            IBatchByTopic<OffsetMessage> originalRequest)
        {
            var response = new CommonAcknowledgement<CommonResponse<OffsetPartitionResponse>> {ReceivedDate = DateTime.UtcNow};
            try
            {
                response.Response = _serialization.DeserializeCommonResponse<OffsetPartitionResponse>(correlationId,
                    responseData, _configuration.Compatibility == Compatibility.V0_8_2 ? Basics.ApiVersion.V0 : Basics.ApiVersion.V1);
            }
            catch (Exception ex)
            {
                OnDecodeError(ex);
                response.Response = BuildEmptyOffsetResponseFromOriginal(originalRequest);
            }
            originalRequest.Dispose();

            OnOffsetsReceived(response);
        }

        private void ProcessSingleOffsetResponse(int correlationId, ReusableMemoryStream responseData,
            Requested<IBatchByTopic<OffsetMessage>, long>  originalRequest)
        {
            try
            {
                var offsetResponse = _serialization.DeserializeCommonResponse<OffsetPartitionResponse>(correlationId,
                    responseData,
                    _configuration.Compatibility == Compatibility.V0_8_2 ? Basics.ApiVersion.V0 : Basics.ApiVersion.V1);
                var response = offsetResponse.TopicsResponse[0].PartitionsData.First();
                originalRequest.Promise.SetResult(response.ErrorCode == ErrorCode.NoError ? response.Offsets[0] : -1);
                originalRequest.Request.Dispose();
            }
            catch (Exception ex)
            {
                OnDecodeError(ex);
                originalRequest.Promise.SetException(ex);
            }
        }

        /// <summary>
        /// Deserialize a Produce response and acknowledge it. We pass back the original
        /// request because in case of error the producer may try to resend the messages.
        /// </summary>
        private void ProcessProduceResponse(int correlationId, ReusableMemoryStream responseData,
            IBatchByTopicByPartition<ProduceMessage> originalRequest)
        {
            var acknowledgement = new ProduceAcknowledgement
            {
                OriginalBatch = originalRequest,
                ReceiveDate = DateTime.UtcNow
            };
            try
            {
                acknowledgement.ProduceResponse =
                    _serialization.DeserializeResponse<ProduceResponse>(correlationId, responseData, _configuration.Compatibility == Compatibility.V0_8_2 ? Basics.ApiVersion.V0 : Basics.ApiVersion.V2);
            }
            catch (Exception ex)
            {
                OnDecodeError(ex);
                acknowledgement.ProduceResponse = new ProduceResponse();
            }

            OnProduceAcknowledgement(acknowledgement);
        }

        /// <summary>
        /// Deserialize a metadata response and signal the corresponding promise accordingly.
        /// </summary>
        private void ProcessMetadataResponse(int correlationId, ReusableMemoryStream responseData,
            Requested<IEnumerable<string>, MetadataResponse> originalRequest)
        {
            try
            {
                var metadataResponse = _serialization.DeserializeMetadataResponse(correlationId, responseData);
                originalRequest.Promise.SetResult(metadataResponse);
            }
            catch (Exception ex)
            {
                OnDecodeError(ex);
                originalRequest.Promise.SetException(ex);
            }
        }

        private void ProcessPromiseResponse<TRequest, TResponse>(int correlationId, ReusableMemoryStream data,
            Requested<TRequest, TResponse> request) 
            where TRequest : class
            where TResponse : IMemoryStreamSerializable, new()
        {
            try
            {
                var response = _serialization.DeserializeResponse<TResponse>(correlationId, data, Basics.ApiVersion.Ignored);
                request.Promise.SetResult(response);
            }
            catch (Exception ex)
            {
                OnDecodeError(ex);
                request.Promise.SetException(ex);
            }
        }
        
        private void ProcessSimpleResponse<TRequest>(int correlationId, ReusableMemoryStream responseData,
            Requested<TRequest, ErrorCode> originalRequest) where TRequest : class
        {
            try
            {
                originalRequest.Promise.SetResult(_serialization.DeserializeResponse<SimpleResponse>(correlationId, responseData, Basics.ApiVersion.Ignored).ErrorCode);
            }
            catch (Exception ex)
            {
                OnDecodeError(ex);
                originalRequest.Promise.SetException(ex);
            }
        }

        // Check if a timeout has occured
        private void CheckForTimeout()
        {
            var connection = _connection;
            if (connection == null) return;
            ConcurrentQueue<Pending> pendings;
            if (_pendings.TryGetValue(connection, out pendings))
            {
                Pending pending;
                // Need only checking the first pending request
                if (pendings.TryPeek(out pending))
                {
                    if (DateTime.UtcNow.Subtract(pending.TimeStamp).TotalMilliseconds >
                        _configuration.ClientRequestTimeoutMs)
                    {
                        // Time out!
                        OnRequestTimeout();
                        HandleConnectionError(connection, new TimeoutException(string.Format("Request {0} from node {1} timed out!", pending.CorrelationId, Name)));
                    }
                }
            }
        }

        // Clean the queue of pending requests of the given connection.
        private void ClearCorrelationIds(IConnection connection, Exception exception = null)
        {
            lock (_pendings)
            {
                ConcurrentQueue<Pending> pendings;
                if (!_pendings.TryRemove(connection, out pendings)) return;
                foreach (var pending in pendings)
                {
                    Drain(pending.Request, true, exception);
                    DecrementPendings();
                }
            }
        }

        // Reset the connection and post an exception to the response actor.
        private void HandleConnectionError(IConnection connection, Exception ex)
        {
            // In case of send/receive error, it's almost guaranted you will actually have both,
            // so the first one to kick in will get to reset the connection.
            if (Interlocked.CompareExchange(ref _connection, null, connection) != connection) return;

            OnConnectionError(ex);
            if (Interlocked.Increment(ref _successiveErrors) == _configuration.MaxSuccessiveNodeErrors + 1)
            {
                OnDead();
            }

            if (connection != null)
            {
                _responseQueue.Post(new Response
                {
                    ResponseType = ResponseType.Exception,
                    Connection = connection,
                    ResponseValue = new ResponseValue {ResponseException = new ResponseException {Exception = ex}}
                });
            }
        }

        private void SignalCompletion<TData>(TaskCompletionSource<TData> promise, Exception exception)
        {
            if (exception != null)
            {
                promise.SetException(exception);
            }
            else
            {
                promise.SetCanceled();
            }
        }

        // "Cancel" a request. If wasSent is true that means the request
        // was actually sent on the connection.
        private void Drain(Request request, bool wasSent, Exception exception = null)
        {
            switch (request.RequestType)
            {
                    // Cancel promised requests
                case RequestType.Metadata:
                    SignalCompletion(request.RequestValue.MetadataRequest.Promise, exception);
                    break;

                case RequestType.GroupCoordinator:
                    SignalCompletion(request.RequestValue.GroupCoordinatorRequest.Promise, exception);
                    break;

                case RequestType.Heartbeat:
                    SignalCompletion(request.RequestValue.HeartbeatRequest.Promise, exception);
                    break;

                case RequestType.OffsetCommit:
                    SignalCompletion(request.RequestValue.OffsetCommitRequest.Promise, exception);
                    break;

                case RequestType.SingleOffset:
                    SignalCompletion(request.RequestValue.OneOffsetRequest.Promise, exception);
                    break;

                case RequestType.LeaveGroup:
                    SignalCompletion(request.RequestValue.LeaveGroupRequest.Promise, exception);
                    break;

                case RequestType.JoinConsumerGroup:
                    SignalCompletion(request.RequestValue.JoinConsumerGroupRequest.Promise, exception);
                    break;

                case RequestType.SyncConsumerGroup:
                    SignalCompletion(request.RequestValue.SyncConsumerGroupRequest.Promise, exception);
                    break;

                // Reroute produce requests
                case RequestType.BatchedProduce:
                    var ack = new ProduceAcknowledgement
                        {
                            OriginalBatch = request.RequestValue.ProduceBatchRequest,
                            ReceiveDate = wasSent ? DateTime.UtcNow : default(DateTime)
                        };
                    OnProduceAcknowledgement(ack);
                    break;

                    // Empty responses for Fetch / Offset requests
                case RequestType.BatchedFetch:
                    OnMessagesReceived(new CommonAcknowledgement<FetchResponse>
                    {
                        Response = BuildEmptyFetchResponseFromOriginal(request.RequestValue.FetchBatchRequest),
                        ReceivedDate = DateTime.UtcNow
                    });
                    break;

                case RequestType.BatchedOffset:
                    OnOffsetsReceived(new CommonAcknowledgement<CommonResponse<OffsetPartitionResponse>>
                    {
                        Response = BuildEmptyOffsetResponseFromOriginal(request.RequestValue.OffsetBatchRequest),
                        ReceivedDate = DateTime.UtcNow
                    });
                    break;
            }
        }

        public event Action<INode> RequestSent = n => { };
        private void OnRequestSent()
        {
            RequestSent(this);
        }

        public event Action<INode, long, long> ProduceBatchSent = (n, m, s) => { };
        private void OnProduceBatchSent(long count, long size)
        {
            ProduceBatchSent(this, count, size);
        }

        public event Action<INode, double> ResponseReceived = (n, l) => { };
        private void OnResponseReceived(double latency)
        {
            ResponseReceived(this, latency);
        }

        public event Action<INode, long, long> FetchResponseReceived = (n, m, s) => { };
        private void OnFetchResponseReceived(long count, long size)
        {
            FetchResponseReceived(this, count, size);
        }

        public event Action<INode, Exception> ConnectionError = (n, e) => { };
        private void OnConnectionError(Exception ex)
        {
            ConnectionError(this, ex);
        }

        public event Action<INode, Exception> DecodeError = (n, e) => { };

        private void OnDecodeError(Exception ex)
        {
            DecodeError(this, ex);
        }

        public event Action<INode, Exception> InternalError = (n, e) => { };

        private void OnInternalError(Exception ex)
        {
            InternalError(this, ex);
        }

        public event Action<INode> Dead = n => { };
        private void OnDead()
        {
            Dead(this);
        }

        public event Action<INode> RequestTimeout = n => { };
        private void OnRequestTimeout()
        {
            RequestTimeout(this);
        }

        public event Action<INode, ProduceAcknowledgement> ProduceAcknowledgement = (n, ack) => { };
        private void OnProduceAcknowledgement(ProduceAcknowledgement ack)
        {
            ProduceAcknowledgement(this, ack);
        }

        public event Action<INode, CommonAcknowledgement<FetchResponse>> FetchAcknowledgement = (n, r) => { };
        private void OnMessagesReceived(CommonAcknowledgement<FetchResponse> r)
        {
            FetchAcknowledgement(this, r);
        }

        public event Action<INode, CommonAcknowledgement<CommonResponse<OffsetPartitionResponse>>> OffsetAcknowledgement = (n, r) => { };
        private void OnOffsetsReceived(CommonAcknowledgement<CommonResponse<OffsetPartitionResponse>> r)
        {
            OffsetAcknowledgement(this, r);
        }

        public event Action<INode> Connected = n => { };
        private void OnConnected()
        {
            Connected(this);
        }

        public event Action<INode> NoMoreRequestSlot = n => { };
        private void OnNoMoreRequestSlot()
        {
            NoMoreRequestSlot(this);
        }
    }
}
