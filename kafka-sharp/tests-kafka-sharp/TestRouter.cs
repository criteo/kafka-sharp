using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Kafka.Protocol;
using Kafka.Public;
using Kafka.Routing;
using NUnit.Framework;

namespace tests_kafka_sharp
{
    [TestFixture]
    class TestRouter
    {
        

        private NodeMock[] _nodes;
        private ClusterMock _cluster;
        private Router _router;
        private Dictionary<string, int> _counters;
        private int MessagesEnqueued;
        private int MessagesReEnqueued;
        private int MessagesRouted;
        private int MessagesExpired;
        private int RoutingTableRequired;

        [SetUp]
        public void SetUp()
        {
            _counters = new Dictionary<string, int>
                {
                    {"test1p", 0},
                    {"test2p", 0},
                    {"testallp", 0}
                };
            _nodes = new NodeMock[5];
            for (int i = 0; i < _nodes.Length; ++i)
            {
                int n = i;
                _nodes[n] = new NodeMock();
                _nodes[n].SuccessfulSent += (_, t, m) => _counters[t] += m; // No need to interlock, NodeMock is synchronous
            }

            var routes = new Dictionary<string, Partition[]>
                {
                    {"test1p", new[] {new Partition {Id = 0, Leader = _nodes[0]}}},
                    {"test2p", new[] {new Partition {Id = 0, Leader = _nodes[0]}, new Partition {Id = 1, Leader = _nodes[1]}}},
                    {"testallp", new[]
                        {
                            new Partition {Id = 0, Leader = _nodes[0]},
                            new Partition {Id = 1, Leader = _nodes[1]},
                            new Partition {Id = 2, Leader = _nodes[2]},
                            new Partition {Id = 3, Leader = _nodes[3]},
                            new Partition {Id = 4, Leader = _nodes[4]},
                        }}
                };

            _cluster = new ClusterMock(routes);

            MessagesEnqueued = MessagesExpired = MessagesReEnqueued = MessagesRouted = RoutingTableRequired = 0;
            _router = new Router(_cluster, new Configuration());
            _router.MessageEnqueued += _ => ++MessagesEnqueued;
            _router.MessageReEnqueued += _ => ++MessagesReEnqueued;
            _router.MessageExpired += _ => ++MessagesExpired;
            _router.MessageRouted += _ => ++MessagesRouted;
            _router.RoutingTableRequired += () => ++RoutingTableRequired;
        }

        [TearDown]
        public void TearDown()
        {
            _nodes = null;
            _cluster = null;
        }

        [Test]
        public async Task TestMessagesAreSent()
        {
            _router.Route("test1p", new Message(), DateTime.UtcNow.AddMinutes(5));
            _router.Route("test1p", new Message(), DateTime.UtcNow.AddMinutes(5));
            _router.Route("test2p", new Message(), DateTime.UtcNow.AddMinutes(5));
            _router.Route("test2p", new Message(), DateTime.UtcNow.AddMinutes(5));
            _router.Route("testallp", new Message(), DateTime.UtcNow.AddMinutes(5));
            _router.Route("testallp", new Message(), DateTime.UtcNow.AddMinutes(5));
            
            await _router.Stop();

            Assert.AreEqual(2, _counters["test1p"]);
            Assert.AreEqual(2, _counters["test2p"]);
            Assert.AreEqual(2, _counters["testallp"]);
        }

        [Test]
        public void TestDefaultPartitioner()
        {
            var partitions = new[]
                {
                    new Partition {Id = 0, Leader = _nodes[0]},
                    new Partition {Id = 1, Leader = _nodes[1]},
                    new Partition {Id = 2, Leader = _nodes[2]},
                    new Partition {Id = 3, Leader = _nodes[3]},
                    new Partition {Id = 4, Leader = _nodes[4]},
                };
            var partitioner = new DefaultPartitioner();
            Assert.AreEqual(partitions[0], partitioner.GetPartition(new Message(), partitions));
            Assert.AreEqual(partitions[1], partitioner.GetPartition(new Message(), partitions));
            Assert.AreEqual(partitions[2], partitioner.GetPartition(new Message(), partitions));
            Assert.AreEqual(partitions[3], partitioner.GetPartition(new Message(), partitions));
            Assert.AreEqual(partitions[4], partitioner.GetPartition(new Message(), partitions));
            Assert.AreEqual(partitions[0], partitioner.GetPartition(new Message(), partitions));
        }

        class TestPartitioner : IPartitioner
        {
            private readonly int _p;
            public TestPartitioner(int p)
            {
                _p = p;
            }

            public Partition GetPartition(Message message, Partition[] partitions)
            {
                return partitions[_p];
            }
        }

        [Test]
        public void TestPartitionerChanges()
        {
            var ev = new AutoResetEvent(false);
            _router.MessageRouted += _ => ev.Set();
            int node3rec = 0;
            _nodes[3].SuccessfulSent += (_1, _2, n) => node3rec += n;
            int node2rec = 0;
            _nodes[2].SuccessfulSent += (_1, _2, n) => node2rec += n;

            _router.SetPartitioners(new Dictionary<string, IPartitioner> {{"testallp", new TestPartitioner(3)}});
            _router.Route("testallp", new Message(), DateTime.UtcNow.AddMinutes(5));
            ev.WaitOne();
            _router.Route("testallp", new Message(), DateTime.UtcNow.AddMinutes(5));
            ev.WaitOne();

            Assert.AreEqual(2, node3rec);
            Assert.AreEqual(0, node2rec);

            _router.SetPartitioner("testallp", new TestPartitioner(2));
            _router.Route("testallp", new Message(), DateTime.UtcNow.AddMinutes(5));
            ev.WaitOne();
            _router.Route("testallp", new Message(), DateTime.UtcNow.AddMinutes(5));
            ev.WaitOne();
            _router.Route("testallp", new Message(), DateTime.UtcNow.AddMinutes(5));
            ev.WaitOne();

            Assert.AreEqual(2, node3rec);
            Assert.AreEqual(3, node2rec);
        }
    }

    [TestFixture]
    class TestRoutingTable
    {
        [Test]
        public void TestRoutingTableReturnsPartitions()
        {
            var node = new NodeMock();
            var routes = new Dictionary<string, Partition[]>
                {
                    {"test1p", new[] {new Partition {Id = 0, Leader = node}}},
                };
            var routingTable = new RoutingTable(routes);

            var partitions = routingTable.GetPartitions("test1p");
            Assert.AreEqual(1, partitions.Length);
            Assert.AreEqual(0, partitions[0].Id);
            Assert.AreSame(node, partitions[0].Leader);
        }

        [Test]
        public void TestRoutingTableReturnsNullForAbsentTopic()
        {
            var node = new NodeMock();
            var routes = new Dictionary<string, Partition[]>
                {
                    {"test1p", new[] {new Partition {Id = 0, Leader = node}}},
                };
            var routingTable = new RoutingTable(routes);

            Assert.NotNull(routingTable.GetPartitions("test1p"));
            Assert.Null(routingTable.GetPartitions("tortemoque"));
        }
    }
}
