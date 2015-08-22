// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reactive.Concurrency;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using Kafka.Common;
using Kafka.Network;
using Kafka.Protocol;
using Kafka.Public;
using Kafka.Routing;

namespace Kafka.Cluster
{
    using ConnectionFactory = Func<IConnection>;

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
        /// Feed a fetch request to the node for batching.
        /// </summary>
        /// <param name="message">Fetch request to batch.</param>
        /// <returns>False if the node is dead.</returns>
        bool Fetch(FetchMessage message);

        /// <summary>
        /// Feed an offset request to the node for batching.
        /// </summary>
        /// <param name="message">Offset request to batch.</param>
        /// <returns>False if the node is dead.</returns>
        bool Offset(OffsetMessage message);

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
        Task<MetadataResponse> FetchMetadata(string topic);

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
        /// Some response has been received from the wire.
        /// </summary>
        event Action<INode> ResponseReceived;

        /// <summary>
        /// Some error occured on the underlying connection.
        /// </summary>
        event Action<INode, Exception> ConnectionError;

        /// <summary>
        /// A response could not be deserialized.
        /// </summary>
        event Action<INode, Exception> DecodeError;

        /// <summary>
        /// The node is dead.
        /// </summary>
        event Action<INode> Dead;

        /// <summary>
        /// The ode successfuly connected to the underlying broker.
        /// </summary>
        event Action<INode> Connected;

        /// <summary>
        /// An acknowledgement for a produce request has been received.
        /// </summary>
        event Action<INode, ProduceAcknowledgement> ProduceAcknowledgement;

        /// <summary>
        /// An acknowledgement for a fetch request has been received.
        /// </summary>
        event Action<INode, CommonAcknowledgement<FetchPartitionResponse>> FetchAcknowledgement;

        /// <summary>
        /// An acknowledgement for an offset request has been received.
        /// </summary>
        event Action<INode, CommonAcknowledgement<OffsetPartitionResponse>> OffsetAcknowledgement;
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
        internal interface ISerializer
        {
            ReusableMemoryStream SerializeProduceBatch(int correlationId, IEnumerable<IGrouping<string, ProduceMessage>> batch);
            ReusableMemoryStream SerializeMetadataAllRequest(int correlationId);
            ReusableMemoryStream SerializeFetchBatch(int correlationId, IEnumerable<IGrouping<string, FetchMessage>> batch);
            ReusableMemoryStream SerializeOffsetBatch(int correlationId, IEnumerable<IGrouping<string, OffsetMessage>> batch);

            MetadataResponse DeserializeMetadataResponse(int correlationId, ReusableMemoryStream data);
            CommonResponse<TPartitionResponse> DeserializeCommonResponse<TPartitionResponse>(int correlationId,
                ReusableMemoryStream data) where TPartitionResponse : IMemoryStreamSerializable, new();
        }

        /// <summary>
        /// The serializer for the real Kafka protocol.
        /// </summary>
        internal class Serializer : ISerializer
        {
            private readonly byte[] _clientId;
            private readonly short _requiredAcks;
            private readonly int _timeoutInMs;
            private readonly int _minBytes;
            private readonly int _maxWait;
            private readonly CompressionCodec _compressionCodec;

            public Serializer(byte[] clientId, RequiredAcks requiredAcks, int timeoutInMs, CompressionCodec compressionCodec, int minBytes, int maxWait)
            {
                _clientId = clientId;
                _requiredAcks = (short) requiredAcks;
                _timeoutInMs = timeoutInMs;
                _minBytes = minBytes;
                _maxWait = maxWait;
                _compressionCodec = compressionCodec;
            }

            public ReusableMemoryStream SerializeMetadataAllRequest(int correlationId)
            {
                return new TopicRequest().Serialize(correlationId, _clientId);
            }

            public ReusableMemoryStream SerializeProduceBatch(int correlationId, IEnumerable<IGrouping<string, ProduceMessage>> batch)
            {
                var produceRequest = new ProduceRequest
                {
                    RequiredAcks = _requiredAcks,
                    Timeout = _timeoutInMs,
                    TopicsData = batch.Select(gt => new TopicData<PartitionData>
                    {
                        TopicName = gt.Key,
                        PartitionsData = gt.GroupBy(m => m.Partition).Select(gp => new PartitionData
                        {
                            Partition = gp.Key,
                            Messages = gp.Select(pm => pm.Message),
                            CompressionCodec = _compressionCodec,
                        })
                    })
                };
                return produceRequest.Serialize(correlationId, _clientId);
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
                return fetchRequest.Serialize(correlationId, _clientId);
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
                return offsetRequest.Serialize(correlationId, _clientId);
            }

            public MetadataResponse DeserializeMetadataResponse(int notUsed, ReusableMemoryStream data)
            {
                return MetadataResponse.Deserialize(data);
            }

            public CommonResponse<TPartitionResponse> DeserializeCommonResponse<TPartitionResponse>(int correlationId,
                ReusableMemoryStream data) where TPartitionResponse : IMemoryStreamSerializable, new()
            {
                return CommonResponse<TPartitionResponse>.Deserialize(data);
            }
        }

        #endregion

        #region Requests

        internal struct BatchRequest<TBatched>
        {
            public IEnumerable<IGrouping<string, TBatched>> Batch;
        }

        internal struct MetadataRequest
        {
            public string Topic;
            public TaskCompletionSource<MetadataResponse> Promise;
        }

        [StructLayout(LayoutKind.Explicit)]
        internal struct RequestValue
        {
            [FieldOffset(0)]
            public BatchRequest<FetchMessage> FetchBatchRequest;

            [FieldOffset(0)]
            public BatchRequest<OffsetMessage> OffsetBatchRequest;

            [FieldOffset(0)]
            public BatchRequest<ProduceMessage> ProduceBatchRequest;

            [FieldOffset(0)]
            public MetadataRequest MetadataRequest;
        }

        /// <summary>
        /// Message type of the underlying send actor.
        /// </summary>
        internal enum RequestType
        {
            Fetch,
            Offset,
            Produce,
            Metadata,
        }

        internal struct Request
        {
            public RequestType RequestType;
            public RequestValue RequestValue;
        }

        // Dummy class to signal request messages incoming.
        private struct Ping
        {
        };

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
        private readonly Subject<ProduceMessage> _produceMessages; // incoming stream of produce messages
        private readonly Subject<FetchMessage> _fetchMessages; // incoming stream of fetch requests
        private readonly Subject<OffsetMessage> _offsetMessages; // incoming stream of offset requests
        private readonly ConcurrentQueue<Request> _metadata = new ConcurrentQueue<Request>(); // queue for metadata requests
        private readonly ConcurrentQueue<Request> _nonMetadata = new ConcurrentQueue<Request>(); // queue for all other batched requests
        private readonly ActionBlock<Ping> _requestQueue; // incoming request actor
        private readonly ActionBlock<Response> _responseQueue; // incoming response actor
        private readonly ISerializer _serializer;

        struct Pending
        {
            public int CorrelationId;
            public Request Request;
        }
        private readonly ConcurrentDictionary<IConnection, ConcurrentQueue<Pending>> _pendings = new ConcurrentDictionary<IConnection, ConcurrentQueue<Pending>>();

        private IConnection _connection; // underlying connection to the broker
        private long _successiveErrors; // used to decide when a node is dead

        private const long MaxSuccessiveErrors = 5;
        private double _resolution = 1000.0;

        // Transform a stream to a batched stream
        private Subject<TData> InitBatchedSubject<TData>(
            int bufferingCount,
            TimeSpan bufferingTime,
            Func<TData, string> grouper,
            Func<IEnumerable<IGrouping<string, TData>>, Request> rbuilder)
        {
            var subject = new Subject<TData>();
            subject
                .Buffer(bufferingTime, bufferingCount)
                .Where(batch => batch.Count > 0)
                .Select(batch => batch.GroupBy(grouper))
                .Subscribe(batch => Post(rbuilder(batch)));
            return subject;
        }

        private Subject<ProduceMessage> InitProduceSubject(int bufferingCount, TimeSpan bufferingTime)
        {
            return InitBatchedSubject<ProduceMessage>(bufferingCount, bufferingTime, m => m.Topic,
                batch => new Request
                {
                    RequestType = RequestType.Produce,
                    RequestValue = new RequestValue
                    {
                        ProduceBatchRequest = new BatchRequest<ProduceMessage> {Batch = batch}
                    }
                });
        }

        private Subject<FetchMessage> InitFetchSubject()
        {
            // TODO: make those values configurable
            return InitBatchedSubject<FetchMessage>(10, TimeSpan.FromMilliseconds(1000.0*_resolution/1000), f => f.Topic,
                batch => new Request
                {
                    RequestType = RequestType.Fetch,
                    RequestValue = new RequestValue
                    {
                        FetchBatchRequest = new BatchRequest<FetchMessage> {Batch = batch}
                    }
                });
        }

        private Subject<OffsetMessage> InitOffsetSubject()
        {
            // TODO: make those values configurable
            return InitBatchedSubject<OffsetMessage>(10, TimeSpan.FromMilliseconds(1000.0*_resolution/1000),
                f => f.Topic,
                batch => new Request
                {
                    RequestType = RequestType.Offset,
                    RequestValue = new RequestValue
                    {
                        OffsetBatchRequest = new BatchRequest<OffsetMessage> {Batch = batch}
                    }
                });
        }

        public string Name { get; internal set; }

        public Node(string name, ConnectionFactory connectionFactory, ISerializer serializer, Configuration configuration)
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
            _produceMessages = InitProduceSubject(configuration.BatchSize, configuration.BufferingTime);
            _fetchMessages = InitFetchSubject();
            _offsetMessages = InitOffsetSubject();
            _serializer = serializer;
        }

        /// <summary>
        /// Scaling applied to the number of successive errors when waiting
        /// before reconnecting. Default is 1000s per successive error.
        /// </summary>
        /// <param name="resolution"></param>
        public Node SetResolution(double resolution)
        {
            _resolution = resolution;
            return this;
        }

        public bool Produce(ProduceMessage message)
        {
            if (IsDead())
            {
                return false;
            }

            _produceMessages.OnNext(message);
            return true;
        }

        public bool Fetch(FetchMessage message)
        {
            if (IsDead())
            {
                return false;
            }

            _fetchMessages.OnNext(message);
            return true;
        }

        public bool Offset(OffsetMessage message)
        {
            if (IsDead())
            {
                return false;
            }

            _offsetMessages.OnNext(message);
            return true;
        }

        public Task<MetadataResponse> FetchMetadata()
        {
            return FetchMetadata(null);
        }

        public Task<MetadataResponse> FetchMetadata(string topic)
        {
            var promise = new TaskCompletionSource<MetadataResponse>();

            Post(new Request
                {
                    RequestType = RequestType.Metadata,
                    RequestValue = new RequestValue {MetadataRequest = new MetadataRequest {Topic = topic, Promise = promise}}
                });

            return promise.Task;
        }

        public async Task Stop()
        {
            _produceMessages.Dispose();
            _requestQueue.Complete();
            _responseQueue.Complete();
            await Task.WhenAll(_requestQueue.Completion, _responseQueue.Completion);
            if (_connection != null)
            {
                _connection.Dispose();
            }
        }

        // We consider ourself dead if we see too much successive errors
        // on the connection.
        private bool IsDead()
        {
            return Interlocked.Read(ref _successiveErrors) > MaxSuccessiveErrors;
        }

        // Initialize a new underlying connection:
        //   If we're currently encountering errors, we wait a little before
        //   retrying to connect, then we create the connection and subscribe
        //   to its events and connect.
        private async Task<IConnection> InitConnection()
        {
            await Task.Delay(TimeSpan.FromMilliseconds(_resolution * _successiveErrors));
            var connection = _connectionFactory();
            connection.ReceiveError += HandleConnectionError;
            connection.Response +=
                (c, i, d) =>
                _responseQueue.Post(new Response
                    {
                        ResponseType = ResponseType.Data,
                        Connection = c,
                        ResponseValue =
                            new ResponseValue {ResponseData = new ResponseData {CorrelationId = i, Data = d}}
                    });
            await connection.ConnectAsync();
            OnConnected();
            _connection = connection;
            _pendings.TryAdd(connection, new ConcurrentQueue<Pending>());
            return connection;
        }

        // Post a message to the underlying request actor
        private void Post(Request request)
        {
            if (request.RequestType == RequestType.Metadata)
            {
                _metadata.Enqueue(request);
            }
            else
            {
                _nonMetadata.Enqueue(request);
            }
            _requestQueue.Post(new Ping());
        }

        // Serialize a request
        private ReusableMemoryStream Serialize(int correlationId, Request request)
        {
            switch (request.RequestType)
            {
                case RequestType.Metadata:
                    return _serializer.SerializeMetadataAllRequest(correlationId);

                case RequestType.Produce:
                    return _serializer.SerializeProduceBatch(correlationId,
                        request.RequestValue.ProduceBatchRequest.Batch);

                case RequestType.Fetch:
                    return _serializer.SerializeFetchBatch(correlationId,
                        request.RequestValue.FetchBatchRequest.Batch);

                case RequestType.Offset:
                    return _serializer.SerializeOffsetBatch(correlationId,
                        request.RequestValue.OffsetBatchRequest.Batch);

                default: // Compiler requires a default case, even if all possible cases are already handled
                    return ReusableMemoryStream.Reserve();
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
                ConcurrentQueue<Pending> pendingsQueue;
                if (!_pendings.TryGetValue(connection, out pendingsQueue))
                {
                    // Means a receive error just after connect, just repost the message
                    // since we never sent anything on the connection
                    Post(request);
                    return;
                }

                // Serialize & send
                using (var data = Serialize(correlationId, request))
                {
                    pendingsQueue.Enqueue(new Pending {CorrelationId = correlationId, Request = request});
                    await connection.SendAsync(correlationId, data, true);
                    Interlocked.Exchange(ref _successiveErrors, 0);
                    OnRequestSent();
                }
            }
            catch (TransportException ex)
            {
                // In case of connection error, we repost
                // the request, which will retry to connect eventually.
                if (ex.Error == TransportError.ConnectError)
                {
                    Post(request);
                }
                HandleConnectionError(connection, ex);
            }
            catch (Exception ex)
            {
                HandleConnectionError(connection, ex);
                if (connection == null)
                {
                    Drain(request, false);
                }
            }
        }

        /// <summary>
        /// Process a response. A response is either some exception or data
        /// from the underlying connection.
        /// </summary>
        private void ProcessResponse(Response response)
        {
            switch (response.ResponseType)
            {
                case ResponseType.Exception:
                    ProcessConnectionError(response.Connection);
                    break;

                case ResponseType.Data:
                    OnResponseReceived();
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

                    using (var data = response.ResponseValue.ResponseData.Data)
                    {
                        if (pending.CorrelationId != response.ResponseValue.ResponseData.CorrelationId)
                        {
                            // This is an error but it should not happen because the underlying connection
                            // is already supposed to have managed that.
                            ProcessConnectionError(response.Connection);
                            return;
                        }

                        switch (pending.Request.RequestType)
                        {
                            case RequestType.Produce:
                                ProcessProduceResponse(
                                    pending.CorrelationId,
                                    data,
                                    pending.Request.RequestValue.ProduceBatchRequest);

                                break;

                            case RequestType.Metadata:
                                ProcessMetadataResponse(
                                    pending.CorrelationId,
                                    data,
                                    pending.Request.RequestValue.MetadataRequest);
                                break;

                            case RequestType.Fetch:
                                ProcessFetchResponse(
                                    pending.CorrelationId,
                                    data,
                                    pending.Request.RequestValue.FetchBatchRequest);
                                break;

                            case RequestType.Offset:
                                ProcessOffsetResponse(
                                    pending.CorrelationId,
                                    data,
                                    pending.Request.RequestValue.OffsetBatchRequest);
                                break;
                        }
                    }
                    break;
            }
        }

        // Clean up the mess
        private void ProcessConnectionError(IConnection connection)
        {
            connection.Dispose();
            ClearCorrelationIds(connection);
        }

        // Preallocated responses
        private static readonly List<ResponseMessage> EmptyMessages = new List<ResponseMessage>();
        private static readonly long[] NoOffset = new long[0];

        // Build an empty response from a given Fetch request with error set to LocalError.
        private static CommonResponse<FetchPartitionResponse> BuildEmptyFetchResponseFromOriginal(
            BatchRequest<FetchMessage> originalRequest)
        {
            return new CommonResponse<FetchPartitionResponse>
            {
                TopicsResponse = originalRequest.Batch.Select(b => new TopicData<FetchPartitionResponse>
                {
                    TopicName = b.Key,
                    PartitionsData = b.Select(fm => new FetchPartitionResponse
                    {
                        ErrorCode = ErrorCode.LocalError,
                        HighWatermarkOffset = -1,
                        Partition = fm.Partition,
                        Messages = EmptyMessages
                    })
                }).ToArray()
            };
        }

        // Build an empty response from a given Offset request with error set to LocalError.
        private static CommonResponse<OffsetPartitionResponse> BuildEmptyOffsetResponseFromOriginal(
            BatchRequest<OffsetMessage> originalRequest)
        {
            return new CommonResponse<OffsetPartitionResponse>
            {
                TopicsResponse = originalRequest.Batch.Select(b => new TopicData<OffsetPartitionResponse>
                {
                    TopicName = b.Key,
                    PartitionsData = b.Select(om => new OffsetPartitionResponse
                    {
                        ErrorCode = ErrorCode.LocalError,
                        Partition = om.Partition,
                        Offsets = NoOffset
                    })
                }).ToArray()
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
            BatchRequest<FetchMessage> originalRequest)
        {
            var response = new CommonAcknowledgement<FetchPartitionResponse>{ReceivedDate = DateTime.UtcNow};
            try
            {
                response.Response = _serializer.DeserializeCommonResponse<FetchPartitionResponse>(correlationId, responseData);
            }
            catch (Exception ex)
            {
                OnDecodeError(ex);
                response.Response = BuildEmptyFetchResponseFromOriginal(originalRequest);
            }

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
            BatchRequest<OffsetMessage> originalRequest)
        {
            var response = new CommonAcknowledgement<OffsetPartitionResponse> {ReceivedDate = DateTime.UtcNow};
            try
            {
                response.Response = _serializer.DeserializeCommonResponse<OffsetPartitionResponse>(correlationId, responseData);
            }
            catch (Exception ex)
            {
                OnDecodeError(ex);
                response.Response = BuildEmptyOffsetResponseFromOriginal(originalRequest);
            }

            OnOffsetsReceived(response);
        }

        /// <summary>
        /// Deserialize a Produce response and acknowledge it. We pass back the original
        /// request because in case of error the producer may try to resend the messages.
        /// </summary>
        private void ProcessProduceResponse(int correlationId, ReusableMemoryStream responseData,
            BatchRequest<ProduceMessage> originalRequest)
        {
            var acknowledgement = new ProduceAcknowledgement
            {
                OriginalBatch = originalRequest.Batch,
                ReceiveDate = DateTime.UtcNow
            };
            try
            {
                acknowledgement.ProduceResponse = _serializer.DeserializeCommonResponse<ProducePartitionResponse>(correlationId, responseData);
            }
            catch (Exception ex)
            {
                OnDecodeError(ex);
                acknowledgement.ProduceResponse = new CommonResponse<ProducePartitionResponse>();
            }

            OnProduceAcknowledgement(acknowledgement);
        }

        /// <summary>
        /// Deserialize a metadata response and signal the corresponding promise accordingly.
        /// </summary>
        private void ProcessMetadataResponse(int correlationId, ReusableMemoryStream responseData, MetadataRequest originalRequest)
        {
            try
            {
                var metadataResponse = _serializer.DeserializeMetadataResponse(correlationId, responseData);
                originalRequest.Promise.SetResult(metadataResponse);
            }
            catch (Exception ex)
            {
                OnDecodeError(ex);
                originalRequest.Promise.SetException(ex);
            }
        }

        // Clean the queue of pending requests of the given connection.
        private void ClearCorrelationIds(IConnection connection)
        {
            ConcurrentQueue<Pending> pendings;
            if (_pendings.TryRemove(connection, out pendings))
            {
                foreach (var pending in pendings)
                {
                    Drain(pending.Request, true);
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
            if (Interlocked.Increment(ref _successiveErrors) == MaxSuccessiveErrors + 1)
            {
                OnDead();
            }

            if (connection != null)
            {
                _responseQueue.Post(new Response
                    {
                        ResponseType = ResponseType.Exception,
                        Connection = connection,
                        ResponseValue =
                            new ResponseValue
                                {
                                    ResponseException = new ResponseException {Exception = ex}
                                }
                    });
            }
        }

        // "Cancel" a request. If wasSent is true that means the request
        // was actually sent on the connection.
        private void Drain(Request request, bool wasSent)
        {
            switch (request.RequestType)
            {
                    // Cancel metadata requests
                case RequestType.Metadata:
                    request.RequestValue.MetadataRequest.Promise.SetCanceled();
                    break;

                    // Reroute produce requests
                case RequestType.Produce:
                    var ack = new ProduceAcknowledgement
                        {
                            OriginalBatch = request.RequestValue.ProduceBatchRequest.Batch,
                            ReceiveDate = wasSent ? DateTime.UtcNow : default(DateTime)
                        };
                    OnProduceAcknowledgement(ack);
                    break;

                    // Empty responses for Fetch / Offset requests
                case RequestType.Fetch:
                    OnMessagesReceived(new CommonAcknowledgement<FetchPartitionResponse>
                    {
                        Response = BuildEmptyFetchResponseFromOriginal(request.RequestValue.FetchBatchRequest),
                        ReceivedDate = DateTime.UtcNow
                    });
                    break;

                case RequestType.Offset:
                    OnOffsetsReceived(new CommonAcknowledgement<OffsetPartitionResponse>
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

        public event Action<INode> ResponseReceived = n => { };
        private void OnResponseReceived()
        {
            ResponseReceived(this);
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

        public event Action<INode> Dead = n => { };
        private void OnDead()
        {
            Dead(this);
        }

        public event Action<INode, ProduceAcknowledgement> ProduceAcknowledgement = (n, ack) => { };
        private void OnProduceAcknowledgement(ProduceAcknowledgement ack)
        {
            ProduceAcknowledgement(this, ack);
        }

        public event Action<INode, CommonAcknowledgement<FetchPartitionResponse>> FetchAcknowledgement = (n, r) => { };
        private void OnMessagesReceived(CommonAcknowledgement<FetchPartitionResponse> r)
        {
            FetchAcknowledgement(this, r);
        }

        public event Action<INode, CommonAcknowledgement<OffsetPartitionResponse>> OffsetAcknowledgement = (n, r) => { };
        private void OnOffsetsReceived(CommonAcknowledgement<OffsetPartitionResponse> r)
        {
            OffsetAcknowledgement(this, r);
        }

        public event Action<INode> Connected = n => { };
        private void OnConnected()
        {
            Connected(this);
        }
    }
}
