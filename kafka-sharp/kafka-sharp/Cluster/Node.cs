// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. 
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
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

    interface INode
    {
        string Name { get; }

        void Produce(string topic, int partition, Message message, DateTime expirationDate);
        Task<MetadataResponse> FetchMetadata();
        Task Stop();

        event Action<INode, string, int> SuccessfulSent;
        event Action<INode, string, int> MessagesDiscarded;
        event Action<INode> RequestSent;
        event Action<INode> ResponseReceived;
        event Action<INode, Exception> ConnectionError;
        event Action<INode, Exception> DecodeError;
        event Action<INode> Dead;
        event Action<INode> Connected;
        event Action<INode> RecoverableError;
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
    class Node : INode
    {
        class ProduceMessage
        {
            public string Topic;
            public Message Message;
            public DateTime ExpirationDate;
            public int Partition;

            // Those objects are pooled to minimize stress on the GC.
            // Use New/Release for managing lifecycle.

            private ProduceMessage() { }

            public static ProduceMessage New(string topic, int partition, Message message, DateTime expirationDate)
            {
                ProduceMessage reserved;
                if (!_produceMessagePool.TryDequeue(out reserved))
                {
                    reserved = new ProduceMessage();
                }
                reserved.Topic = topic;
                reserved.Partition = partition;
                reserved.Message = message;
                reserved.ExpirationDate = expirationDate;
                return reserved;
            }

            public static void Release(ProduceMessage message)
            {
                _produceMessagePool.Enqueue(message);
            }

            static readonly ConcurrentQueue<ProduceMessage> _produceMessagePool = new ConcurrentQueue<ProduceMessage>();
        }

        struct ProduceBatchRequest
        {
            public IEnumerable<IGrouping<string, ProduceMessage>> Batch;
            public byte[] Serialized;
        }

        struct MetadataRequest
        {
            public string Topic;
            public TaskCompletionSource<MetadataResponse> Promise;
        }

        [StructLayout(LayoutKind.Explicit)]
        struct RequestValue
        {
            [FieldOffset(0)]
            public ProduceBatchRequest ProduceBatchRequest;

            [FieldOffset(0)]
            public MetadataRequest MetadataRequest;
        }

        enum RequestType
        {
            Produce,
            Metadata
        }

        struct Request
        {
            public RequestType RequestType;
            public RequestValue RequestValue;
        }

        struct ResponseData
        {
            public byte[] Data;
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

        enum ResponseType
        {
            Data,
            Exception
        }

        class Response
        {
            public ResponseType ResponseType;
            public IConnection Connection;
            public ResponseValue ResponseValue;
        }

        private struct Ping
        {
        };

        private static int _correlationId;

        private readonly ConnectionFactory _connectionFactory;
        private readonly IRouter _router;
        private readonly Subject<ProduceMessage> _produceMessages;
        private readonly ConcurrentQueue<Request> _metadata = new ConcurrentQueue<Request>();
        private readonly ConcurrentQueue<Request> _nonMetadata = new ConcurrentQueue<Request>();
        private readonly ActionBlock<Ping> _requestQueue;
        private readonly ActionBlock<Response> _responseQueue;
        private readonly byte[] _clientId;
        private readonly byte[] _metadataRequest;
        private readonly Configuration _configuration;

        class Pending
        {
            public int CorrelationId;
            public Request Request;
        }
        private readonly ConcurrentDictionary<IConnection, ConcurrentQueue<Pending>> _pendings = new ConcurrentDictionary<IConnection, ConcurrentQueue<Pending>>();
        private IConnection _connection;
        private long _successiveErrors;

        private const long MaxSuccessiveErrors = 3;
        private double _resolution = 1000.0;

        private Subject<ProduceMessage> InitProduceSubject(int bufferingCount, TimeSpan bufferingTime)
        {
            var subject = new Subject<ProduceMessage>();
            subject
                .Buffer(bufferingTime, bufferingCount)
                .Where(batch => batch.Count > 0)
                .Select(batch => batch.GroupBy(m => m.Topic))
                .Subscribe(batch => Post(new Request
                    {
                        RequestType = RequestType.Produce,
                        RequestValue = new RequestValue
                            {
                                ProduceBatchRequest = new ProduceBatchRequest {Batch = batch}
                            }
                    }));
            return subject;
        }

        public string Name { get; internal set; }

        public Node(string name, byte[] clientId, ConnectionFactory connectionFactory, IRouter router, Configuration configuration)
        {
            Name = name ?? "[Unknown]";
            _configuration = configuration;
            _connectionFactory = connectionFactory;
            _router = router;
            var options = new ExecutionDataflowBlockOptions
                {
                    MaxMessagesPerTask = 1,
                    TaskScheduler = configuration.TaskScheduler
                };
            _requestQueue = new ActionBlock<Ping>(r => ProcessRequest(r), options);
            _responseQueue = new ActionBlock<Response>(r => ProcessResponse(r), options);
            _produceMessages = InitProduceSubject(configuration.BatchSize, configuration.BufferingTime);
            _clientId = clientId;
            _metadataRequest = new TopicRequest().Serialize(0, _clientId);
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

        public void Produce(string topic, int partition, Message message, DateTime expirationDate)
        {
            if (IsDead())
            {
                _router.Route(topic, message, expirationDate);
                return;
            }

            _produceMessages.OnNext(ProduceMessage.New(topic, partition, message, expirationDate));
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
            _connection.Dispose();
        }

        private bool IsDead()
        {
            return Interlocked.Read(ref _successiveErrors) > MaxSuccessiveErrors;
        }

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

        void Post(Request request)
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

        private async Task ProcessRequest(Ping ping)
        {
            // Prioritize metadata
            Request request;
            if (!_metadata.TryDequeue(out request))
            {
                _nonMetadata.TryDequeue(out request);
            }
            
            if (IsDead())
            {
                Drain(request);
                return;
            }

            var connection = _connection;
            try
            {
                if (connection == null)
                {
                    connection = await InitConnection();
                }

                // Serialize
                int correlationId = Interlocked.Increment(ref _correlationId);
                ConcurrentQueue<Pending> pendingsQueue;
                if (!_pendings.TryGetValue(connection, out pendingsQueue))
                {
                    // Means a receive error just after connect, just repost the message
                    // since we never sent anything on the connection
                    Post(request);
                    return;
                }
                
                byte[] buffer = null;
                switch (request.RequestType)
                {
                    case RequestType.Metadata:
                        buffer = _metadataRequest;
                        // Header is: size(4) - apikey(2) - apiversion(2)
                        BigEndianConverter.Write(buffer, correlationId, 8);
                        break;

                    case RequestType.Produce:
                        request = SerializeProduceRequest(request, correlationId);
                        buffer = request.RequestValue.ProduceBatchRequest.Serialized;
                        break;
                }

                pendingsQueue.Enqueue(new Pending { CorrelationId = correlationId, Request = request });
                await connection.SendAsync(correlationId, buffer, true);
                Interlocked.Exchange(ref _successiveErrors, 0);
                OnRequestSent();
            }
            catch (TransportException ex)
            {
                if (ex.Error == TransportError.ConnectError)
                {
                    Post(request);
                }
                HandleConnectionError(connection, ex);
            }
            catch (Exception ex)
            {
                HandleConnectionError(connection, ex);
            }
        }

        private Request SerializeProduceRequest(Request request, int correlationId)
        {
            var batch = request.RequestValue.ProduceBatchRequest.Batch;
            var produceRequest = new ProduceRequest
                {
                    RequiredAcks = (short)_configuration.RequiredAcks,
                    Timeout = _configuration.RequestTimeoutMs,
                    TopicData = batch.Select(gt => new TopicData
                        {
                            TopicName = gt.Key,
                            PartitionsData = gt.GroupBy(m => m.Partition).Select(gp => new PartitionData
                                {
                                    Partition = gp.Key,
                                    Messages = gp.Select(pm => pm.Message),
                                    CompressionCodec = _configuration.CompressionCodec,
                                })
                        })
                };
            request.RequestValue.ProduceBatchRequest.Serialized = produceRequest.Serialize(correlationId, _clientId);
            return request;
        }

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
                    if (pendings.TryDequeue(out pending))
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
                                    response.ResponseValue.ResponseData.Data,
                                    pending.Request.RequestValue.ProduceBatchRequest);
                               
                                break;
                            case RequestType.Metadata:
                                ProcessMetadataResponse(
                                    response.ResponseValue.ResponseData.Data,
                                    pending.Request.RequestValue.MetadataRequest);
                                break;
                        }
                    }
                    break;
            }
        }

        private void ProcessConnectionError(IConnection connection)
        {
            connection.Dispose();
            ClearCorrelationIds(connection);
        }

        private readonly Dictionary<string, HashSet<int>> _tmpPartitionsInError = new Dictionary<string, HashSet<int>>();
        private readonly Dictionary<string, HashSet<int>> _tmpPartitionsInRecoverableError = new Dictionary<string, HashSet<int>>();

        private static readonly HashSet<int> NullHash = new HashSet<int>(); 

        private void ProcessProduceResponse(byte[] responseData, ProduceBatchRequest originalRequest)
        {
            // The whole point of deserializing the response is to search for errors.
            ProduceResponse produceResponse;
            try
            {
                produceResponse = ProduceResponse.Deserialize(responseData);
            }
            catch (Exception ex)
            {
                // Corrupted data.
                OnDecodeError(ex);
                DrainOrDiscard(new Request
                {
                    RequestType = RequestType.Produce,
                    RequestValue = new RequestValue {ProduceBatchRequest = originalRequest}
                });
                return;
            }

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
                        OnRecoverableError();
                        _tmpPartitionsInError[tr.TopicName] = new HashSet<int>();
                        _tmpPartitionsInRecoverableError[tr.TopicName] = new HashSet<int>();
                    }

                    if (Error.IsPartitionErrorRecoverable(p.ErrorCode))
                    {
                        _tmpPartitionsInRecoverableError[tr.TopicName].Add(p.Partition);
                    }
                    else
                    {
                        _tmpPartitionsInError[tr.TopicName].Add(p.Partition);
                    }
                }
            }

            // Scan messages for errors and release memory
            foreach (var grouping in originalRequest.Batch)
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
                        _router.Route(pm.Topic, pm.Message, pm.ExpirationDate);
                    }
                    else if (errPartitions.Contains(pm.Partition))
                    {
                        ++discarded;
                    }
                    else
                    {
                        ++sent;
                    }
                    ProduceMessage.Release(pm);
                }
                if (sent > 0)
                {
                    OnMessagesSent(grouping.Key, sent);
                }
                if (discarded > 0)
                {
                    OnMessagesDiscarded(grouping.Key, discarded);
                }
            }
        }

        private void ProcessMetadataResponse(byte[] responseData, MetadataRequest originalRequest)
        {
            try
            {
                var metadataResponse = MetadataResponse.Deserialize(responseData);
                originalRequest.Promise.SetResult(metadataResponse);
            }
            catch (Exception ex)
            {
                originalRequest.Promise.SetException(ex);
                OnDecodeError(ex);
            }
        }

        private void ClearCorrelationIds(IConnection connection)
        {
            ConcurrentQueue<Pending> pendings;
            if (_pendings.TryRemove(connection, out pendings))
            {
                foreach (var pending in pendings)
                {
                    DrainOrDiscard(pending.Request);
                }
            }
        }

        private void DrainOrDiscard(Request request)
        {
            if (_configuration.ErrorStrategy == ErrorStrategy.Retry ||
                request.RequestType == RequestType.Metadata)
            {
                Drain(request);
            }
            else
            {
                foreach (var grouping in request.RequestValue.ProduceBatchRequest.Batch)
                {
                    OnMessagesDiscarded(grouping.Key, grouping.Count());
                    foreach (var message in grouping)
                    {
                        ProduceMessage.Release(message);
                    }
                }
            }
        }

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

        private void Drain(Request request)
        {
            switch (request.RequestType)
            {
                    // Cancel metadata requests
                case RequestType.Metadata:
                    request.RequestValue.MetadataRequest.Promise.SetCanceled();
                    break;

                    // Reroute produce requests
                case RequestType.Produce:
                    foreach (var grouping in request.RequestValue.ProduceBatchRequest.Batch)
                    {
                        foreach (var message in grouping)
                        {
                            _router.Route(grouping.Key, message.Message, message.ExpirationDate);
                            ProduceMessage.Release(message);
                        }
                    }
                    break;
            }
        }

        public event Action<INode, string, int> SuccessfulSent = (n, s, i) => { };
        private void OnMessagesSent(string topic, int count)
        {
            SuccessfulSent(this, topic, count);
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

        public event Action<INode, string, int> MessagesDiscarded = (n, t, c) => { };
        private void OnMessagesDiscarded(string topic, int number)
        {
            MessagesDiscarded(this, topic, number);
        }

        public event Action<INode> Connected = n => { };
        private void OnConnected()
        {
            Connected(this);
        }

        public event Action<INode> RecoverableError = n => { };
        private void OnRecoverableError()
        {
            RecoverableError(this);
        }
    }
}
