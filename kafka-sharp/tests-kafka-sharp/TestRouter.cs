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
        private Dictionary<string, Partition[]> _routes;
        private Router _router;
        private Dictionary<string, int> _messagesSentByTopic;
        private int MessagesEnqueued;
        private int MessagesReEnqueued;
        private int MessagesRouted;
        private int MessagesExpired;
        private int RoutingTableRequired;

        private CountdownEvent _finished;


        [SetUp]
        public void SetUp()
        {
            _messagesSentByTopic = new Dictionary<string, int>
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
                _nodes[n].SuccessfulSent += (_, t, m) => _messagesSentByTopic[t] += m; // No need to interlock, NodeMock is synchronous
            }

            _routes = new Dictionary<string, Partition[]>
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

            _cluster = new ClusterMock(_routes);

            MessagesEnqueued = MessagesExpired = MessagesReEnqueued = MessagesRouted = RoutingTableRequired = 0;
            _router = new Router(_cluster, new Configuration());
            _router.MessageEnqueued += _ =>
                {
                    ++MessagesEnqueued;
                    if(_finished != null) _finished.Signal();
                };
            _router.MessageReEnqueued += _ =>
                {
                    ++MessagesReEnqueued;
                    if (_finished != null) _finished.Signal();
                };
            _router.MessageExpired += _ =>
                {
                    ++MessagesExpired;
                    if (_finished != null) _finished.Signal();
                };
            _router.MessageRouted += _ =>
                {
                    ++MessagesRouted;
                    if (_finished != null) _finished.Signal();
                };
            _router.RoutingTableRequired += () =>
                {
                    ++RoutingTableRequired;
                    if (_finished != null) _finished.Signal();
                };

            _finished = null;
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

            Assert.AreEqual(2, _messagesSentByTopic["test1p"]);
            Assert.AreEqual(2, _messagesSentByTopic["test2p"]);
            Assert.AreEqual(2, _messagesSentByTopic["testallp"]);

            CheckCounters(expectedMessagesEnqueued: 6, expectedMessagesReEnqueued: 0, expectedMessagesRouted: 6, expectedMessagesExpired: 0, expectedRoutingTableRequired: 1);
        }

        private void CheckCounters(int expectedMessagesEnqueued, int expectedMessagesReEnqueued, int expectedMessagesRouted, int expectedMessagesExpired, int expectedRoutingTableRequired)
        {
            Assert.AreEqual(expectedMessagesEnqueued, MessagesEnqueued);
            Assert.AreEqual(expectedMessagesReEnqueued, MessagesReEnqueued);
            Assert.AreEqual(expectedMessagesRouted, MessagesRouted);
            Assert.AreEqual(expectedMessagesExpired, MessagesExpired);
            Assert.AreEqual(expectedRoutingTableRequired, RoutingTableRequired);
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

            CheckCounters(expectedMessagesEnqueued: 5, expectedMessagesReEnqueued: 0, expectedMessagesRouted: 5, expectedMessagesExpired: 0, expectedRoutingTableRequired: 1);
        }

        [Test]
        public async Task TestExpiredMessagesAreNotRouted()
        {
            _router.Route("test1p", new Message(), DateTime.UtcNow.AddMilliseconds(-1));
            _router.Route("test2p", new Message(), DateTime.UtcNow.AddMilliseconds(-1));

            await _router.Stop();

            Assert.AreEqual(0, _messagesSentByTopic["test1p"]);
            Assert.AreEqual(0, _messagesSentByTopic["test2p"]);
            CheckCounters(expectedMessagesEnqueued: 2, expectedMessagesReEnqueued: 0, expectedMessagesRouted: 0, expectedMessagesExpired: 2, expectedRoutingTableRequired: 0);
        }

        [Test]
        public async Task TestNoMessageIsSentAfterStop()
        {
            _router.Route("test1p", new Message(), DateTime.UtcNow.AddMinutes(5));
            await _router.Stop();
            Assert.AreEqual(1, _messagesSentByTopic["test1p"]);

            _router.Route("test1p", new Message(), DateTime.UtcNow.AddMinutes(5));
            await Task.Delay(TimeSpan.FromMilliseconds(100));

            Assert.AreEqual(1, _messagesSentByTopic["test1p"]);
            CheckCounters(expectedMessagesEnqueued: 1, expectedMessagesReEnqueued: 0, expectedMessagesRouted: 1, expectedMessagesExpired: 0, expectedRoutingTableRequired: 1);
        }

        [Test]
        public void TestMessagesArePostponedIfThereAreNoPartitions_AndSentWhenPartitionsBecomeAvailable()
        {
            _cluster.Partitions = new Dictionary<string, Partition[]>();

            using (_finished = new CountdownEvent(5))
            {
                _router.Route("test1p", new Message(), DateTime.UtcNow.AddMinutes(5)); // => 1x MessageRenqueued, 1x RoutingTableRequired
                _router.Route("test1p", new Message(), DateTime.UtcNow.AddMinutes(5)); // => 1x MessageRenqueued, no routing table required because postponed
                _router.Route("test2p", new Message(), DateTime.UtcNow.AddMinutes(5)); // => 1x MessageRenqueued, 1x RoutingTableRequired
                _finished.Wait();
            }

            Assert.AreEqual(0, _messagesSentByTopic["test1p"]);
            Assert.AreEqual(0, _messagesSentByTopic["test2p"]);
            CheckCounters(expectedMessagesEnqueued: 3, expectedMessagesReEnqueued: 0, expectedMessagesRouted: 0, expectedMessagesExpired: 0, expectedRoutingTableRequired: 2);

            using (_finished = new CountdownEvent(6))
            {
                _router.ChangeRoutingTable(new RoutingTable(_routes)); // => 3x Renqueued, 3x Routed
                _finished.Wait();
            }

            Assert.AreEqual(2, _messagesSentByTopic["test1p"]);
            Assert.AreEqual(1, _messagesSentByTopic["test2p"]);
            CheckCounters(expectedMessagesEnqueued: 3, expectedMessagesReEnqueued: 3, expectedMessagesRouted: 3, expectedMessagesExpired: 0, expectedRoutingTableRequired: 2);
        }

        [Test]
        public async Task TestPostponedMessagesAreNotReEnqueuedIfExpired()
        {
            _cluster.Partitions = new Dictionary<string, Partition[]>();

            _router.Route("test1p", new Message(), DateTime.UtcNow.AddMilliseconds(50));
            await Task.Delay(TimeSpan.FromMilliseconds(100));

            Assert.AreEqual(0, _messagesSentByTopic["test1p"]);
            CheckCounters(expectedMessagesEnqueued: 1, expectedMessagesReEnqueued: 0, expectedMessagesRouted: 0, expectedMessagesExpired: 0, expectedRoutingTableRequired: 1);

            //trigger check postponed
            _router.ChangeRoutingTable(new RoutingTable(_routes));

            await _router.Stop();

            Assert.AreEqual(0, _messagesSentByTopic["test1p"]);
            CheckCounters(expectedMessagesEnqueued: 1, expectedMessagesReEnqueued: 0, expectedMessagesRouted: 0, expectedMessagesExpired: 1, expectedRoutingTableRequired: 1);
        }

        [Test]
        [Explicit]
        public async Task TestPostponedExpiredMessagesAreRemovedWhenNoPartition()
        {
            _cluster.Partitions = new Dictionary<string, Partition[]>();

            _router.Route("test1p", new Message(), DateTime.UtcNow.AddMilliseconds(50));
            _router.Route("test1p", new Message(), DateTime.UtcNow.AddMinutes(5));
            await Task.Delay(TimeSpan.FromMilliseconds(100));

            Assert.AreEqual(0, _messagesSentByTopic["test1p"]);
            CheckCounters(expectedMessagesEnqueued: 2, expectedMessagesReEnqueued: 0, expectedMessagesRouted: 0, expectedMessagesExpired: 0, expectedRoutingTableRequired: 1);

            //trigger check postponed
            using (_finished = new CountdownEvent(3))
            {
                _router.ChangeRoutingTable(new RoutingTable(_routes)); // => 1x renqueue, 1x routed, 1x expired
                _finished.Wait();
            }

            Assert.AreEqual(1, _messagesSentByTopic["test1p"]);
            CheckCounters(expectedMessagesEnqueued: 2, expectedMessagesReEnqueued: 1, expectedMessagesRouted: 1, expectedMessagesExpired: 1, expectedRoutingTableRequired: 1);
        }
    }
}
