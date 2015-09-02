using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Kafka.Cluster;
using Kafka.Protocol;
using Kafka.Public;
using Kafka.Routing;
using NUnit.Framework;
using Cluster = Kafka.Cluster.Cluster;
using Moq;

namespace tests_kafka_sharp
{
    [TestFixture]
    class TestCluster
    {
        private Mock<INode>[] _nodeMocks;
        private readonly Mock<IProduceRouter> _routerMock = new Mock<IProduceRouter>();
        private readonly Mock<IConsumeRouter> _consumeMock = new Mock<IConsumeRouter>();
        private RoutingTable _routingTable;

        private Cluster _cluster;
        private int _errors;

        [SetUp]
        public void Setup()
        {
            _nodeMocks = new[]
            {
                GenerateNodeMock(1),
                GenerateNodeMock(2),
                GenerateNodeMock(3),
                GenerateNodeMock(4)
            };

            _routingTable = null;

            _consumeMock.Setup(c => c.Stop()).Returns(Task.FromResult(new Void()));
            _routerMock.Setup(r => r.Stop()).Returns(Task.FromResult(new Void()));
            _routerMock.Setup(r => r.ChangeRoutingTable(It.IsAny<RoutingTable>())).Callback<RoutingTable>( r =>
            {
                _routingTable = r;
            });

            _cluster = new Cluster(new Configuration { Seeds = "localhost:1", TaskScheduler = new CurrentThreadTaskScheduler() }, new DevNullLogger(),
                                   (h, p) => _nodeMocks[p - 1].Object,
                                   () => _routerMock.Object, () => _consumeMock.Object);
            _errors = 0;
            _cluster.InternalError += _ => ++_errors;
        }

        private Mock<INode> GenerateNodeMock(int port)
        {
            var nodeMock = new Mock<INode>();
            nodeMock.Setup(n => n.Name).Returns("localhost:" + port);
            nodeMock.Setup(n => n.FetchMetadata()).Returns(Task.FromResult(TestData.TestMetadataResponse));
            nodeMock.Setup(n => n.Stop()).Returns(Task.FromResult(true));
            return nodeMock;
        }

        void AssertDefaultRouting(RoutingTable routing)
        {
            var defaultRoutingTable = new RoutingTable(new Dictionary<string, Partition[]>
                {
                    {"topic1", new[] {new Partition {Id = 1, Leader = _nodeMocks[0].Object}}},
                    {"topic2", new[]
                    {
                        new Partition {Id = 1, Leader = _nodeMocks[0].Object},
                        new Partition {Id = 2, Leader = _nodeMocks[1].Object}
                    }},
                    {"topic3", new[]
                    {
                        new Partition {Id = 1, Leader = _nodeMocks[0].Object},
                        new Partition {Id = 2, Leader = _nodeMocks[1].Object},
                        new Partition {Id = 3, Leader = _nodeMocks[2].Object},
                    }},
                    {"error2", new[] {new Partition {Id = 2, Leader = _nodeMocks[1].Object}}}
                });

            AssertRouting(defaultRoutingTable, routing);
        }

        void AssertRouting(RoutingTable routing, RoutingTable expectedRoutingTable)
        {
            var topics = TestData.TestMetadataResponse.TopicsMeta.Select(t => t.TopicName);
            AssertRoutingTablesAreEqual(expectedRoutingTable, routing, topics);
        }

        void AssertRoutingTablesAreEqual(RoutingTable expectedRoutingTable, RoutingTable routingTable, IEnumerable<string> topics)
        {
            foreach (var topic in topics)
            {
                var expectedPartitions = expectedRoutingTable.GetPartitions(topic);
                var partitions = routingTable.GetPartitions(topic);
                Assert.AreEqual(expectedPartitions.Length, partitions.Length);

                for (int i = 0; i < expectedPartitions.Length; i++)
                {
                    Assert.AreEqual(expectedPartitions[i].Id, partitions[i].Id);
                    Assert.AreEqual(expectedPartitions[i].Leader.Name, partitions[i].Leader.Name);
                }
            }
        }

        private void AssertStatistics(IStatistics statistics, int successfulSent = 0, int requestSent = 0, int responseReceived = 0, int errors = 0,
            int nodeDead = 0, int expired = 0, int discarded = 0, int exit = 0, int received = 0)
        {
            Assert.AreEqual(successfulSent, statistics.SuccessfulSent);
            Assert.AreEqual(requestSent, statistics.RequestSent);
            Assert.AreEqual(responseReceived, statistics.ResponseReceived);
            Assert.AreEqual(errors, statistics.Errors);
            Assert.AreEqual(nodeDead, statistics.NodeDead);
            Assert.AreEqual(expired, statistics.Expired);
            Assert.AreEqual(discarded, statistics.Discarded);
            Assert.AreEqual(exit, statistics.Exited);
            Assert.AreEqual(received, statistics.Received);
        }

        [Test]
        public async Task TestFetchRoutingTable()
        {
            _cluster.Start();
            var routing = await _cluster.RequireNewRoutingTable();

            Assert.AreEqual(0, _errors);
            AssertDefaultRouting(routing);
        }

        [Test]
        public void TestSignalRoutingTableTriggeredByClusterStart()
        {
            _cluster.Start();

            Assert.AreEqual(0, _errors);
            AssertDefaultRouting(_routingTable);
        }

        [Test]
        public void TestDeadNode()
        {
            _cluster.Start();

            var metadataResponseAfterDeadNodeId1 = new MetadataResponse
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
                            new PartitionMeta{ErrorCode = ErrorCode.LeaderNotAvailable, Id = 1, Leader = 1},
                        }},
                        new TopicMeta {TopicName = "topic2", ErrorCode = ErrorCode.NoError, Partitions = new []
                        {
                            new PartitionMeta{ErrorCode = ErrorCode.LeaderNotAvailable, Id = 1, Leader = 1},
                            new PartitionMeta{ErrorCode = ErrorCode.NoError, Id = 2, Leader = 2},
                        }},
                        new TopicMeta {TopicName = "topic3", ErrorCode = ErrorCode.NoError, Partitions = new []
                        {
                            new PartitionMeta{ErrorCode = ErrorCode.LeaderNotAvailable, Id = 1, Leader = 1},
                            new PartitionMeta{ErrorCode = ErrorCode.NoError, Id = 2, Leader = 2},
                            new PartitionMeta{ErrorCode = ErrorCode.NoError, Id = 3, Leader = 3},
                        }},
                        new TopicMeta {TopicName = "error1", ErrorCode = ErrorCode.Unknown, Partitions = new []
                        {
                            new PartitionMeta{ErrorCode = ErrorCode.LeaderNotAvailable, Id = 1, Leader = 1},
                        }},
                        new TopicMeta {TopicName = "error2", ErrorCode = ErrorCode.NoError, Partitions = new []
                        {
                            new PartitionMeta{ErrorCode = ErrorCode.LeaderNotAvailable, Id = 1, Leader = 1},
                            new PartitionMeta{ErrorCode = ErrorCode.NoError, Id = 2, Leader = 2},
                        }},
                    }
            };

            foreach (var nodeMock in _nodeMocks)
            {
                nodeMock.Setup(n => n.FetchMetadata()).Returns(Task.FromResult(metadataResponseAfterDeadNodeId1));
            }

            _nodeMocks[0].Raise(n => n.Dead += null, _nodeMocks[0].Object); //metadata is refreshed

            AssertStatistics(_cluster.Statistics, nodeDead: 1);

            var routingTableAfterDeadNode = new RoutingTable(new Dictionary<string, Partition[]>
                {
                    {"topic2", new[]
                    {
                        new Partition {Id = 2, Leader = _nodeMocks[1].Object}
                    }},
                    {"topic3", new[]
                    {
                        new Partition {Id = 2, Leader = _nodeMocks[1].Object},
                        new Partition {Id = 3, Leader = _nodeMocks[2].Object},
                    }},
                    {"error2", new[] {new Partition {Id = 2, Leader = _nodeMocks[1].Object}}}
                });

            AssertRouting(_routingTable, routingTableAfterDeadNode);

            Assert.AreEqual(0, _errors);
        }

        [Test]
        public async Task TestConnectionError()
        {
            _cluster.Start();
            _nodeMocks[0].Raise(n => n.ConnectionError += null, _nodeMocks[0].Object, null);

            await _cluster.Stop();
            Assert.AreEqual(0, _errors);
            AssertStatistics(_cluster.Statistics, errors: 1);
        }

        [Test]
        public async Task TestDecodeError()
        {
            _cluster.Start();
            _nodeMocks[0].Raise(n => n.DecodeError += null, _nodeMocks[0].Object, null);

            await _cluster.Stop();
            Assert.AreEqual(0, _errors);
            AssertStatistics(_cluster.Statistics, errors: 1);
        }

        [Test]
        public async Task TestRequestSent()
        {
            _cluster.Start();
            _nodeMocks[0].Raise(n => n.RequestSent += null, _nodeMocks[0].Object);

            await _cluster.Stop();
            Assert.AreEqual(0, _errors);
            AssertStatistics(_cluster.Statistics, requestSent: 1);
        }

        [Test]
        public async Task TestResponseReceived()
        {
            _cluster.Start();
            _nodeMocks[0].Raise(n => n.ResponseReceived += null, _nodeMocks[0].Object);

            await _cluster.Stop();
            Assert.AreEqual(0, _errors);
            AssertStatistics(_cluster.Statistics, responseReceived: 1);
        }

        [Test]
        public async Task TestMessageExpired()
        {
            _cluster.Start();
            _routerMock.Raise(r => r.MessageExpired += null, "testTopic", new Message());

            await _cluster.Stop();
            Assert.AreEqual(0, _errors);
            AssertStatistics(_cluster.Statistics, expired: 1, exit: 1);
        }

        [Test]
        public async Task TestMessagesAcknowledged()
        {
            _cluster.Start();
            const int messagesAcknowledged = 2;
            _routerMock.Raise(r => r.MessagesAcknowledged += null, "testTopic", messagesAcknowledged);

            await _cluster.Stop();
            Assert.AreEqual(0, _errors);
            AssertStatistics(_cluster.Statistics, successfulSent: messagesAcknowledged, exit: messagesAcknowledged);
        }

        [Test]
        public async Task TestMessagesDiscarded()
        {
            _cluster.Start();
            const int messagesDiscarded = 3;
            var message = new Message();
            _routerMock.Raise(r => r.MessageDiscarded += null, "testTopic", message);
            _routerMock.Raise(r => r.MessageDiscarded += null, "testTopic", message);
            _routerMock.Raise(r => r.MessageDiscarded += null, "testTopic", message);

            await _cluster.Stop();
            Assert.AreEqual(0, _errors);
            AssertStatistics(_cluster.Statistics, discarded: messagesDiscarded, exit: messagesDiscarded);
        }

        [Test]
        public async Task TestProduceAcknowledgement()
        {
            _cluster.Start();
            var pa = new ProduceAcknowledgement();
            _nodeMocks[0].Raise(n => n.ProduceAcknowledgement += null, _nodeMocks[0].Object, pa);

            await _cluster.Stop();
            _routerMock.Verify(r => r.Acknowledge(pa));

            Assert.AreEqual(0, _errors);
        }

        [Test]
        public void TestFetchAcknowledgement()
        {
            _cluster.Start();
            var ca = new CommonAcknowledgement<FetchPartitionResponse>();
            _nodeMocks[0].Raise(n => n.FetchAcknowledgement += null, _nodeMocks[0].Object, ca);

            _consumeMock.Verify(r => r.Acknowledge(ca));

            Assert.AreEqual(0, _errors);
        }

        [Test]
        public void TestOffsetAcknowledgement()
        {
            _cluster.Start();
            var ca = new CommonAcknowledgement<OffsetPartitionResponse>();
            _nodeMocks[0].Raise(n => n.OffsetAcknowledgement += null, _nodeMocks[0].Object, ca);

            _consumeMock.Verify(r => r.Acknowledge(ca));

            Assert.AreEqual(0, _errors);
        }

        [Test]
        public void TestConsumerMessageReceived()
        {
            _cluster.Start();
            _consumeMock.Raise(r => r.MessageReceived += null, It.IsAny<KafkaRecord>());

            Assert.AreEqual(0, _errors);
            AssertStatistics(_cluster.Statistics, received: 1);
        }

        [Test]
        public async Task TestNewNodeInMetadataResponse()
        {
            _cluster.Start();

            var metadataResponseWithNewNodeId4 = new MetadataResponse
            {
                BrokersMeta = new[]
                {
                    new BrokerMeta {Id = 1, Host = "localhost", Port = 1},
                    new BrokerMeta {Id = 2, Host = "localhost", Port = 2},
                    new BrokerMeta {Id = 3, Host = "localhost", Port = 3},
                    new BrokerMeta {Id = 4, Host = "localhost", Port = 4}
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
                        new PartitionMeta{ErrorCode = ErrorCode.NoError, Id = 4, Leader = 4}
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

            foreach (var nodeMock in _nodeMocks)
            {
                nodeMock.Setup(n => n.FetchMetadata()).Returns(Task.FromResult(metadataResponseWithNewNodeId4));
            }

            var routing = await _cluster.RequireNewRoutingTable();

            var routingTableAfterNewNode = new RoutingTable(new Dictionary<string, Partition[]>
                {
                    {"topic1", new[] {new Partition {Id = 1, Leader = _nodeMocks[0].Object}}},
                    {"topic2", new[]
                    {
                        new Partition {Id = 1, Leader = _nodeMocks[0].Object},
                        new Partition {Id = 2, Leader = _nodeMocks[1].Object}
                    }},
                    {"topic3", new[]
                    {
                        new Partition {Id = 1, Leader = _nodeMocks[0].Object},
                        new Partition {Id = 2, Leader = _nodeMocks[1].Object},
                        new Partition {Id = 3, Leader = _nodeMocks[2].Object},
                        new Partition {Id = 4, Leader = _nodeMocks[3].Object},
                    }},
                    {"error2", new[] {new Partition {Id = 2, Leader = _nodeMocks[1].Object}}}
                });

            AssertRouting(routing, routingTableAfterNewNode);

            Assert.AreEqual(0, _errors);
        }

        [Test]
        public async Task TestInternalErrorOnFetchMetadata()
        {
            var tcs = new TaskCompletionSource<MetadataResponse>();
            tcs.SetException(new Exception("testEx"));
            foreach (var nodeMock in _nodeMocks)
            {
                nodeMock.Setup(n => n.FetchMetadata()).Returns(tcs.Task);
            }

            _cluster.Start();
            await _cluster.Stop();

            Assert.AreEqual(1, _errors);
        }

        [Test]
        public void TestInternalErrorOnFetchAllPartitionsForTopic()
        {
            _cluster.Start();

            Assert.That(async () => await _cluster.RequireAllPartitionsForTopic("nonexistingTopic"), Throws.TypeOf<TaskCanceledException>());

            Assert.AreEqual(1, _errors);
        }

        [Test]
        public async Task TestEmptyResponseMetadata()
        {
            var emptyMetadataResponse = new MetadataResponse
            {
                BrokersMeta = new BrokerMeta[0],
                TopicsMeta = new TopicMeta[0]
            };

            foreach (var nodeMock in _nodeMocks)
            {
                nodeMock.Setup(n => n.FetchMetadata()).Returns(Task.FromResult(emptyMetadataResponse));
            }

            _cluster.Start();

            _nodeMocks[0].Verify(n => n.FetchMetadata(), Times.Once());

            var emptyRoutingTable = new RoutingTable(new Dictionary<string, Partition[]>());
            AssertRouting(_routingTable, emptyRoutingTable);

            //next we check that even if the routing table is empty we can still refresh metadata by reloading the node from seeds

            var metadataResponseWithNodes = new MetadataResponse
            {
                BrokersMeta = new[]
                {
                    new BrokerMeta {Id = 1, Host = "localhost", Port = 1},
                    new BrokerMeta {Id = 2, Host = "localhost", Port = 2}
                },
                TopicsMeta = new[]
                {
                    new TopicMeta {TopicName = "topic2", ErrorCode = ErrorCode.NoError, Partitions = new []
                    {
                        new PartitionMeta{ErrorCode = ErrorCode.NoError, Id = 1, Leader = 1},
                        new PartitionMeta{ErrorCode = ErrorCode.NoError, Id = 2, Leader = 2},
                    }}
                }
            };

            foreach (var nodeMock in _nodeMocks)
            {
                nodeMock.Setup(n => n.FetchMetadata()).Returns(Task.FromResult(metadataResponseWithNodes));
            }

            var routing = await _cluster.RequireNewRoutingTable();
            _nodeMocks[0].Verify(n => n.FetchMetadata(), Times.Exactly(2));

            var routingTableWithNodes = new RoutingTable(new Dictionary<string, Partition[]>
                {
                    {"topic2", new[]
                    {
                        new Partition {Id = 1, Leader = _nodeMocks[0].Object},
                        new Partition {Id = 2, Leader = _nodeMocks[1].Object}
                    }}
                });

            AssertRouting(routing, routingTableWithNodes);
            Assert.AreEqual(0, _errors);
        }

        [Test]
        public async Task TestAllNodesDead()
        {
            var metadataResponseWithOneNode = new MetadataResponse
            {
                BrokersMeta = new[]
                {
                    new BrokerMeta {Id = 1, Host = "localhost", Port = 1}
                },
                TopicsMeta = new[]
                {
                    new TopicMeta {TopicName = "topic1", ErrorCode = ErrorCode.NoError, Partitions = new []
                    {
                        new PartitionMeta{ErrorCode = ErrorCode.NoError, Id = 1, Leader = 1},
                    }}
                }
            };

            foreach (var nodeMock in _nodeMocks)
            {
                nodeMock.Setup(n => n.FetchMetadata()).Returns(Task.FromResult(metadataResponseWithOneNode));
            }

            _cluster.Start();

            _nodeMocks[0].Verify(n => n.FetchMetadata(), Times.Once());

            //kill the only available node and check that it is reloaded from seeds in order to refresh the metadata
            _nodeMocks[1].Raise(n => n.Dead += null, _nodeMocks[1].Object);
            await _cluster.RequireNewRoutingTable();
            _nodeMocks[0].Verify(n=>n.FetchMetadata(), Times.Exactly(2));

            Assert.AreEqual(0, _errors);
        }

        [Test]
        public async Task TestStop()
        {
            _cluster.Start();
            _nodeMocks[0].Raise(n => n.ConnectionError += null, _nodeMocks[0].Object, null);

            await _cluster.Stop();
            AssertStatistics(_cluster.Statistics, errors: 1);

            _nodeMocks[0].Raise(n => n.ConnectionError += null, _nodeMocks[0].Object, null);
            await Task.Delay(100);
            AssertStatistics(_cluster.Statistics, errors: 1);

            Assert.AreEqual(0, _errors);
        }

        [Test]
        public void TestEmptySeedsThrowArgumentException()
        {
            Assert.Throws<ArgumentException>(() => new Cluster(new Configuration {Seeds = ""}, new DevNullLogger(),
                                                         (h, p) => _nodeMocks[p - 1].Object,
                                                         () => _routerMock.Object, () => _consumeMock.Object));
        }

        [Test]
        public async Task TestFetchAllPartitionsForTopic()
        {
            var oneTopicMetadataResponse = new MetadataResponse
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
                        new PartitionMeta{ErrorCode = ErrorCode.LeaderNotAvailable, Id = 2, Leader = 2},
                        new PartitionMeta{ErrorCode = ErrorCode.NoError, Id = 3, Leader = 3},
                    }}
                }
            };

            foreach (var nodeMock in _nodeMocks)
            {
                nodeMock.Setup(n => n.FetchMetadata("topic1")).Returns(Task.FromResult(oneTopicMetadataResponse));
            }

            _cluster.Start();
            var partitions = await _cluster.RequireAllPartitionsForTopic("topic1");

            CollectionAssert.AreEqual(new[] { 1, 2, 3 }, partitions);
            Assert.AreEqual(0, _errors);
        }
    }
}
