using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Kafka.Cluster;
using Kafka.Network;
using Kafka.Protocol;
using Kafka.Public;
using Kafka.Routing;
using ICluster = Kafka.Cluster.ICluster;

namespace tests_kafka_sharp
{
    struct Void {}

    static class TestData
    {
        public static readonly MetadataResponse TestMetadataResponse = new MetadataResponse
        {
            BrokersMeta = new[]
            {
                new BrokerMeta {Id = 1, Host = "localhost", Port = 1},
                new BrokerMeta {Id = 2, Host = "localhost", Port = 2},
                new BrokerMeta {Id = 3, Host = "localhost", Port = 3}
            },
            TopicsMeta = new[]
            {
                new TopicMeta {TopicName = "topic1", ErrorCode = ErrorCode.NoError, Partitions = new []
                {
                    new PartitionMeta{ErrorCode = ErrorCode.NoError, Id = 1, Leader = 1},
                }},
                new TopicMeta {TopicName = "topic2", ErrorCode = ErrorCode.NoError, Partitions = new []
                {
                    new PartitionMeta{ErrorCode = ErrorCode.NoError, Id = 1, Leader = 1},
                    new PartitionMeta{ErrorCode = ErrorCode.NoError, Id = 2, Leader = 2},
                }},
                new TopicMeta {TopicName = "topic3", ErrorCode = ErrorCode.NoError, Partitions = new []
                {
                    new PartitionMeta{ErrorCode = ErrorCode.NoError, Id = 1, Leader = 1},
                    new PartitionMeta{ErrorCode = ErrorCode.NoError, Id = 2, Leader = 2},
                    new PartitionMeta{ErrorCode = ErrorCode.NoError, Id = 3, Leader = 3},
                }},
                new TopicMeta {TopicName = "error1", ErrorCode = ErrorCode.Unknown, Partitions = new []
                {
                    new PartitionMeta{ErrorCode = ErrorCode.NoError, Id = 1, Leader = 1},
                }},
                new TopicMeta {TopicName = "error2", ErrorCode = ErrorCode.NoError, Partitions = new []
                {
                    new PartitionMeta{ErrorCode = ErrorCode.LeaderNotAvailable, Id = 1, Leader = 1},
                    new PartitionMeta{ErrorCode = ErrorCode.NoError, Id = 2, Leader = 2},
                }},
            }
        };

        public static void Reset()
        {
            EchoConnectionMock.Reset();
            ScenarioSerializerMock.Reset();
        }
    }

    class NodeMock : INode
    {
        private MetadataResponse _response;
        public NodeMock() : this(new MetadataResponse()) { }

        public NodeMock(MetadataResponse response)
        {
            _response = response;
        }

        public string Name
        {
            get { return "Some node"; }
        }

        public bool Produce(ProduceMessage message)
        {
            MessageReceived(message.Topic);
            var ack = new ProduceAcknowledgement
                {
                    OriginalBatch = new[] {new BatchMock {Key = message.Topic, Messages = new[] {message}}},
                    ProduceResponse =
                        new ProduceResponse
                            {
                                TopicsResponse =
                                    new[]
                                        {
                                            new TopicResponse
                                                {
                                                    TopicName = message.Topic,
                                                    Partitions =
                                                        new[]
                                                            {
                                                                new PartitionResponse
                                                                    {
                                                                        ErrorCode = ErrorCode.NoError,
                                                                        Offset = 0,
                                                                        Partition = message.Partition
                                                                    }
                                                            }
                                                }
                                        }
                            },
                    ReceiveDate = DateTime.UtcNow
                };
            ProduceAcknowledgement(this, ack);
            return true;
        }

        public Task<MetadataResponse> FetchMetadata()
        {
            return Task.FromResult(_response);
        }

        public Task Stop()
        {
            return Task.FromResult(new Void());
        }

        public event Action<INode> RequestSent = n => { };
        public event Action<INode> ResponseReceived = n => { };
        public event Action<INode, Exception> ConnectionError = (n, e) => { };
        public event Action<INode, Exception> DecodeError = (n, e) => { };
        public event Action<INode> Dead = _ => { };
        public event Action<INode> Connected = _ => { };
        public event Action<INode, ProduceAcknowledgement> ProduceAcknowledgement = (n, ack) => { };

        public event Action<string> MessageReceived = _ => { };
    }

    class ClusterMock : ICluster
    {
        public Dictionary<string, Partition[]> Partitions { private get; set; }

        public ClusterMock(Dictionary<string, Partition[]> partitions)
        {
            Partitions = partitions;
        }

        public Task<RoutingTable> RequireNewRoutingTable()
        {
            var r = new RoutingTable(Partitions);
            return Task.FromResult(r);
        }

        public Statistics Statistics
        {
            get { return new Statistics(); }
        }
    }

    class ConnectionMock : IConnection
    {
        public virtual Task SendAsync(int correlationId, byte[] buffer, bool acknowledge)
        {
            throw new NotImplementedException();
        }

        public virtual Task ConnectAsync()
        {
            throw new NotImplementedException();
        }

        public event Action<IConnection, int, byte[]> Response;
        public event Action<IConnection, Exception> ReceiveError;

        public void Dispose()
        {
        }

        protected void OnResponse(int correlationId, byte[] data)
        {
            Response(this, correlationId, data);
        }

        protected void OnReceiveError(Exception ex)
        {
            ReceiveError(this, ex);
        }
    }

    class SuccessConnectionMock : ConnectionMock
    {
        public override Task SendAsync(int correlationId, byte[] buffer, bool acknowledge)
        {
            return Task.FromResult(true);
        }

        public override Task ConnectAsync()
        {
            return Task.FromResult(true);
        }
    }

    /// <summary>
    /// This has to be used with a custom ISerializer that won't
    /// actually use the returned buffer but takes advantage of the
    /// correlation id to "deserialize" proper responses.
    /// </summary>
    class EchoConnectionMock : SuccessConnectionMock
    {
        private readonly bool _forceErrors;
        private static int _count;

        public static void Reset()
        {
            _count = 1;
        }

        public EchoConnectionMock(bool forceErrors = false)
        {
            _forceErrors = forceErrors;
        }

        public override Task SendAsync(int correlationId, byte[] buffer, bool acknowledge)
        {
            if (_forceErrors)
            {
                if (Interlocked.Increment(ref _count)%3 == 0)
                {
                    OnReceiveError(new SocketException((int) SocketError.Interrupted));
                    return Task.FromResult(true);
                }

                if (Interlocked.Increment(ref _count)%4 == 0)
                {
                    var tcs = new TaskCompletionSource<Void>();
                    tcs.SetException(new SocketException((int) SocketError.Interrupted));
                    return tcs.Task;
                }
            }

            if (acknowledge)
            {
                OnResponse(correlationId, buffer);
            }
            return Task.FromResult(true);
        }
    }

    class ConnectFailingConnectionMock : SuccessConnectionMock
    {
        public override Task ConnectAsync()
        {
            var p = new TaskCompletionSource<Void>();
            p.SetException(new TransportException(TransportError.ConnectError));
            return p.Task;
        }
    }

    class SendFailingConnectionMock : SuccessConnectionMock
    {
        public override Task SendAsync(int correlationId, byte[] buffer, bool acknowledge)
        {
            var p = new TaskCompletionSource<bool>();
            p.SetException(new TransportException(TransportError.WriteError));
            return p.Task;
        }
    }

    class ReceiveFailingConnectionMock : SuccessConnectionMock
    {
        public override Task SendAsync(int correlationId, byte[] buffer, bool acknowledge)
        {
            Task.Factory.StartNew(() => OnReceiveError(new TransportException(TransportError.ReadError)));
            return Task.FromResult(true);
        }
    }

    class ProduceRouterMock : IProduceRouter
    {
        public void ChangeRoutingTable(RoutingTable table)
        {
            OnChangeRouting(table);
        }

        public void Route(string topic, Message message, DateTime expirationDate)
        {
            Route(ProduceMessage.New(topic, message, expirationDate));
        }

        public void Route(ProduceMessage message)
        {
            MessageRouted(message.Topic);
        }

        public void Acknowledge(ProduceAcknowledgement acknowledgement)
        {
            // do nothing
        }

        public Task Stop()
        {
            return Task.FromResult(new Void());
        }

        public event Action<string> MessageRouted;
        public event Action<string> MessageExpired;
        public event Action<string, int> MessagesDiscarded;
        public event Action<string, int> MessagesAcknowledged = (t, c) => { };
        public event Action<RoutingTable> OnChangeRouting = _ => { };
    }

    class DummySerializer : Node.ISerializer
    {
        public byte[] SerializeProduceBatch(int correlationId, IEnumerable<IGrouping<string, ProduceMessage>> batch)
        {
            return new byte[0];
        }

        public byte[] SerializeMetadataAllRequest(int correlationId)
        {
            return new byte[0];
        }

        public ProduceResponse DeserializeProduceResponse(int correlationId, byte[] data)
        {
            return new ProduceResponse();
        }

        public MetadataResponse DeserializeMetadataResponse(int correlationId, byte[] data)
        {
            return new MetadataResponse();
        }
    }

    class MetadataSerializer : Node.ISerializer
    {
        private readonly MetadataResponse _metadataResponse;

        public MetadataSerializer(MetadataResponse returned)
        {
            _metadataResponse = returned;
        }

        public byte[] SerializeProduceBatch(int correlationId, IEnumerable<IGrouping<string, ProduceMessage>> batch)
        {
            throw new NotImplementedException();
        }

        public byte[] SerializeMetadataAllRequest(int correlationId)
        {
            return new byte[0];
        }

        public ProduceResponse DeserializeProduceResponse(int correlationId, byte[] data)
        {
            throw new NotImplementedException();
        }

        public MetadataResponse DeserializeMetadataResponse(int correlationId, byte[] data)
        {
            return _metadataResponse;
        }
    }

    class ProduceSerializer : Node.ISerializer
    {
        private readonly ProduceResponse _produceResponse;

        public ProduceSerializer(ProduceResponse returned)
        {
            _produceResponse = returned;
        }

        public byte[] SerializeProduceBatch(int correlationId, IEnumerable<IGrouping<string, ProduceMessage>> batch)
        {
            return new byte[0];
        }

        public byte[] SerializeMetadataAllRequest(int correlationId)
        {
            throw new NotImplementedException();
        }

        public ProduceResponse DeserializeProduceResponse(int correlationId, byte[] data)
        {
            return _produceResponse;
        }

        public MetadataResponse DeserializeMetadataResponse(int correlationId, byte[] data)
        {
            throw new NotImplementedException();
        }
    }

    class BatchMock : IGrouping<string, ProduceMessage>
    {
        public string Key { get; internal set; }
        internal ProduceMessage[] Messages;

        public IEnumerator<ProduceMessage> GetEnumerator()
        {
            return Messages.AsEnumerable().GetEnumerator();
        }

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            return Messages.GetEnumerator();
        }
    }

    class ScenarioSerializerMock : Node.ISerializer
    {
        readonly ConcurrentDictionary<int, ProduceResponse> _produceResponses = new ConcurrentDictionary<int, ProduceResponse>();
        private readonly MetadataResponse _metadataResponse;
        private readonly bool _forceErrors;
        private static int _count;

        public static void Reset()
        {
            _count = 1;
        }

        public ScenarioSerializerMock(MetadataResponse returned, bool forceErrors = false)
        {
            _metadataResponse = returned;
            _forceErrors = forceErrors;
        }

        public byte[] SerializeProduceBatch(int correlationId, IEnumerable<IGrouping<string, ProduceMessage>> batch)
        {
            var r = new ProduceResponse
            {
                TopicsResponse = batch.Select(g => new TopicResponse
                {
                    TopicName = g.Key,
                    Partitions = g.GroupBy(m => m.Partition).Select(pg => new PartitionResponse
                    {
                        ErrorCode = _forceErrors && Interlocked.Increment(ref _count)%2 == 0
                            ? ErrorCode.LeaderNotAvailable
                            : _forceErrors && Interlocked.Increment(ref _count)%3 == 0
                                ? ErrorCode.MessageSizeTooLarge
                                : _metadataResponse.TopicsMeta.Where(tm => tm.TopicName == g.Key)
                                    .Select(tm => tm.Partitions.First(p => p.Id == pg.Key).ErrorCode)
                                    .First(),
                        Offset = 0,
                        Partition = pg.Key
                    }).ToArray()
                }).ToArray()
            };

            _produceResponses[correlationId] = r;

            return new byte[0];
        }

        public byte[] SerializeMetadataAllRequest(int correlationId)
        {
            return new byte[0];
        }

        public ProduceResponse DeserializeProduceResponse(int correlationId, byte[] data)
        {
            ProduceResponse pr;
            _produceResponses.TryRemove(correlationId, out pr);
            return pr;
        }

        public MetadataResponse DeserializeMetadataResponse(int correlationId, byte[] data)
        {
            return _metadataResponse;
        }
    }

    class TestLogger : ILogger
    {
        private readonly ConcurrentQueue<string> _information = new ConcurrentQueue<string>();
        private readonly ConcurrentQueue<string> _warning = new ConcurrentQueue<string>();
        private readonly ConcurrentQueue<string> _error = new ConcurrentQueue<string>();

        public IEnumerable<string> InformationLog
        {
            get { return _information; }
        }

        public IEnumerable<string> WarningLog
        {
            get { return _warning; }
        }

        public IEnumerable<string> ErrorLog
        {
            get { return _error; }
        }

        public void LogInformation(string message)
        {
            _information.Enqueue(message);
        }

        public void LogWarning(string message)
        {
            _warning.Enqueue(message);
        }

        public void LogError(string message)
        {
            _error.Enqueue(message);
        }
    }
}
