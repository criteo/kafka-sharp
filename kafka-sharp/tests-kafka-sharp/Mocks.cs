using System;
using System.Collections.Generic;
using System.Linq;
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

        public void Produce(ProduceMessage message)
        {
            SuccessfulSent(this, message.Topic, 1);
        }

        public Task<MetadataResponse> FetchMetadata()
        {
            return Task.FromResult(_response);
        }

        public Task Stop()
        {
            return Task.FromResult(new Void());
        }

        public event Action<INode, string, int> SuccessfulSent = (n, s, i) => { };
        public event Action<INode, string, int> MessagesDiscarded = (n, s, i) => { };
        public event Action<INode, string> MessageExpired = (n, t) => { };
        public event Action<INode> RequestSent = n => { };
        public event Action<INode> ResponseReceived = n => { };
        public event Action<INode, Exception> ConnectionError = (n, e) => { };
        public event Action<INode, Exception> DecodeError = (n, e) => { };
        public event Action<INode> Dead = _ => { };
        public event Action<INode> Connected = _ => { };
        public event Action<INode> RecoverableError = _ => { };
    }

    class ClusterMock : ICluster
    {
        private readonly Dictionary<string, Partition[]> _partitions;

        public ClusterMock(Dictionary<string, Partition[]> partitions)
        {
            _partitions = partitions;
        }

        public Task<RoutingTable> RequireNewRoutingTable()
        {
            var r = new RoutingTable(_partitions);
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
            if (acknowledge)
            {
            }
            return Task.FromResult(true);
        }

        public override Task ConnectAsync()
        {
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

    class RouterMock : IRouter
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

        public Task Stop()
        {
            return Task.FromResult(new Void());
        }

        public event Action<string> MessageRouted;
        public event Action<string> MessageExpired;
        public event Action<RoutingTable> OnChangeRouting = _ => { };
    }

}
