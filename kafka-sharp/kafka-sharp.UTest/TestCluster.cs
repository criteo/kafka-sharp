using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Kafka.Cluster;
using Kafka.Network;
using Kafka.Protocol;
using Kafka.Public;
using Kafka.Public.Loggers;
using Kafka.Routing;
using NUnit.Framework;
using Cluster = Kafka.Cluster.Cluster;
using Moq;

namespace tests_kafka_sharp
{
    [TestFixture]
    internal class TestCluster
    {
        private Mock<INode>[] _nodeMocks;
        private readonly Mock<IProduceRouter> _routerMock = new Mock<IProduceRouter>();
        private readonly Mock<IConsumeRouter> _consumeMock = new Mock<IConsumeRouter>();
        private RoutingTable _routingTable;

        private Cluster _cluster;
        private int _internalErrors;
        private readonly Mock<ILogger> _logger = new Mock<ILogger>();

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

            _cluster =
                new Cluster(
                    new Configuration
                    {
                        Seeds = "localhost:1",
                        TaskScheduler = new CurrentThreadTaskScheduler(),
                        MinimumTimeBetweenRefreshMetadata = TimeSpan.FromSeconds(0),
                        MinInSyncReplicas = 2
                    }, _logger.Object,
                    nodeFactory: (h, p) => _nodeMocks[p - 1].Object,
                    producerFactory: () => _routerMock.Object, consumerFactory: () => _consumeMock.Object);
            _internalErrors = 0;
            _cluster.InternalError += _ => ++_internalErrors;
        }

        private Mock<INode> GenerateNodeMock(int port)
        {
            var nodeMock = new Mock<INode>();
            nodeMock.Setup(n => n.Name).Returns("localhost:" + port);
            nodeMock.Setup(n => n.FetchMetadata()).Returns(Task.FromResult(TestData.TestMetadataResponse));
            nodeMock.Setup(n => n.FetchMetadata(It.IsAny<IEnumerable<string>>())).Returns(Task.FromResult(TestData.TestMetadataResponse));
            nodeMock.Setup(n => n.Stop()).Returns(Task.FromResult(true));
            return nodeMock;
        }

        void AssertDefaultRouting(RoutingTable routing)
        {
            var defaultRoutingTable = new RoutingTable(new Dictionary<string, Partition[]>
                {
                    {"topic1", new[] {new Partition {Id = 1, Leader = _nodeMocks[0].Object, NbIsr = 1}}},
                    {"topic2", new[]
                    {
                        new Partition {Id = 1, Leader = _nodeMocks[0].Object, NbIsr = 1},
                        new Partition {Id = 2, Leader = _nodeMocks[1].Object, NbIsr = 1}
                    }},
                    {"topic3", new[]
                    {
                        new Partition {Id = 1, Leader = _nodeMocks[0].Object, NbIsr = 1},
                        new Partition {Id = 2, Leader = _nodeMocks[1].Object, NbIsr = 1},
                        new Partition {Id = 3, Leader = _nodeMocks[2].Object, NbIsr = 1},
                    }},
                    {"error2", new[] {new Partition {Id = 2, Leader = _nodeMocks[1].Object, NbIsr = 1}}}
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
            int nodeDead = 0, int expired = 0, int discarded = 0, int exit = 0, int received = 0,
            int rawReceived = 0, int rawReceivedBytes = 0, int rawProduced = 0, int rawProducedBytes = 0, int requestTimeout = 0)
        {
            Assert.AreEqual(successfulSent, statistics.SuccessfulSent);
            Assert.AreEqual(requestSent, statistics.RequestSent);
            Assert.AreEqual(responseReceived, statistics.ResponseReceived);
            Assert.AreEqual(errors, statistics.Errors);
            Assert.AreEqual(nodeDead, statistics.NodeDead);
            Assert.AreEqual(requestTimeout, statistics.RequestTimeout);
            Assert.AreEqual(expired, statistics.Expired);
            Assert.AreEqual(discarded, statistics.Discarded);
            Assert.AreEqual(exit, statistics.Exited);
            Assert.AreEqual(received, statistics.Received);
            Assert.AreEqual(rawProduced, statistics.RawProduced);
            Assert.AreEqual(rawProducedBytes, statistics.RawProducedBytes);
            Assert.AreEqual(rawReceived, statistics.RawReceived);
            Assert.AreEqual(rawReceivedBytes, statistics.RawReceivedBytes);
        }

        [Test]
        public async Task TestFetchRoutingTable()
        {
            _cluster.Start();
            var routing = await _cluster.RequireNewRoutingTable();

            Assert.AreEqual(0, _internalErrors);
            AssertDefaultRouting(routing);
        }

        [Test]
        public void TestFetchRoutingTableTimeout()
        {
            var failed = new TaskCompletionSource<MetadataResponse>();
            failed.SetException(new TimeoutException());
            foreach (var node in _nodeMocks)
            {
                node.Setup(n => n.FetchMetadata()).Returns(failed.Task);
            }
            _cluster.Start();
#if NET_CORE
            Assert.ThrowsAsync<TimeoutException>(async () => await _cluster.RequireNewRoutingTable());
#else
            Assert.Throws<TimeoutException>(async () => await _cluster.RequireNewRoutingTable());
#endif
            Assert.AreEqual(0, _internalErrors);
        }

        [Test]
        public void TestSignalRoutingTableTriggeredByClusterStart()
        {
            _cluster.Start();

            Assert.AreEqual(0, _internalErrors);
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
                            new PartitionMeta{ErrorCode = ErrorCode.LeaderNotAvailable, Id = 1, Leader = 1, Isr = TestData.Isr1},
                        }},
                        new TopicMeta {TopicName = "topic2", ErrorCode = ErrorCode.NoError, Partitions = new []
                        {
                            new PartitionMeta{ErrorCode = ErrorCode.LeaderNotAvailable, Id = 1, Leader = 1, Isr = TestData.Isr1},
                            new PartitionMeta{ErrorCode = ErrorCode.NoError, Id = 2, Leader = 2, Isr = TestData.Isr1},
                        }},
                        new TopicMeta {TopicName = "topic3", ErrorCode = ErrorCode.NoError, Partitions = new []
                        {
                            new PartitionMeta{ErrorCode = ErrorCode.LeaderNotAvailable, Id = 1, Leader = 1, Isr = TestData.Isr1},
                            new PartitionMeta{ErrorCode = ErrorCode.NoError, Id = 2, Leader = 2, Isr = TestData.Isr1},
                            new PartitionMeta{ErrorCode = ErrorCode.NoError, Id = 3, Leader = 3, Isr = TestData.Isr1},
                        }},
                        new TopicMeta {TopicName = "error1", ErrorCode = ErrorCode.Unknown, Partitions = new []
                        {
                            new PartitionMeta{ErrorCode = ErrorCode.LeaderNotAvailable, Id = 1, Leader = 1, Isr = TestData.Isr1},
                        }},
                        new TopicMeta {TopicName = "error2", ErrorCode = ErrorCode.NoError, Partitions = new []
                        {
                            new PartitionMeta{ErrorCode = ErrorCode.LeaderNotAvailable, Id = 1, Leader = 1, Isr = TestData.Isr1},
                            new PartitionMeta{ErrorCode = ErrorCode.NoError, Id = 2, Leader = 2, Isr = TestData.Isr1},
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

            Assert.AreEqual(0, _internalErrors);
        }

        [Test]
        public async Task TestRequestSent()
        {
            _cluster.Start();
            _nodeMocks[0].Raise(n => n.RequestSent += null, _nodeMocks[0].Object);

            await _cluster.Stop();
            Assert.AreEqual(0, _internalErrors);
            AssertStatistics(_cluster.Statistics, requestSent: 1);
        }

        [Test]
        public async Task TestResponseReceived()
        {
            _cluster.Start();
            _nodeMocks[0].Raise(n => n.ResponseReceived += null, _nodeMocks[0].Object, 0);

            await _cluster.Stop();
            Assert.AreEqual(0, _internalErrors);
            AssertStatistics(_cluster.Statistics, responseReceived: 1);
        }

        [Test]
        public async Task TestMessageExpired()
        {
            _cluster.Start();
            _routerMock.Raise(r => r.MessageExpired += null, "testTopic", new Message());

            await _cluster.Stop();
            Assert.AreEqual(0, _internalErrors);
            AssertStatistics(_cluster.Statistics, expired: 1, exit: 1);
        }

        [Test]
        public async Task TestMessagesAcknowledged()
        {
            _cluster.Start();
            const int messagesAcknowledged = 2;
            _routerMock.Raise(r => r.MessagesAcknowledged += null, "testTopic", messagesAcknowledged);

            await _cluster.Stop();
            Assert.AreEqual(0, _internalErrors);
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
            Assert.AreEqual(0, _internalErrors);
            AssertStatistics(_cluster.Statistics, discarded: messagesDiscarded, exit: messagesDiscarded);
        }

        [Test]
        public async Task TestConnectionError()
        {
            _cluster.Start();
            _nodeMocks[0].Raise(n => n.ConnectionError += null, _nodeMocks[0].Object, null);
            _nodeMocks[0].Raise(n => n.ConnectionError += null, _nodeMocks[0].Object, new TransportException(TransportError.ConnectError));
            _nodeMocks[0].Raise(n => n.ConnectionError += null, _nodeMocks[0].Object, new TransportException(TransportError.ReadError));
            _nodeMocks[0].Raise(n => n.ConnectionError += null, _nodeMocks[0].Object, new TransportException(TransportError.WriteError));

            await _cluster.Stop();
            Assert.AreEqual(0, _internalErrors);
            AssertStatistics(_cluster.Statistics, errors: 4);
        }

        [Test]
        public async Task TestRequestTimeout()
        {
            _cluster.Start();
            _nodeMocks[0].Raise(n => n.RequestTimeout += null, _nodeMocks[0].Object);

            await _cluster.Stop();
            Assert.AreEqual(0, _internalErrors);
            AssertStatistics(_cluster.Statistics, requestTimeout: 1);
        }

        [Test]
        public async Task TestDecodeError()
        {
            _cluster.Start();
            _nodeMocks[0].Raise(n => n.DecodeError += null, _nodeMocks[0].Object, new CrcException("some message"));
            _nodeMocks[0].Raise(n => n.DecodeError += null, _nodeMocks[0].Object, new UncompressException("some message", CompressionCodec.Gzip, null));

            await _cluster.Stop();
            Assert.AreEqual(0, _internalErrors);
            AssertStatistics(_cluster.Statistics, errors: 2);
        }

        [Test]
        public async Task TestProduceBatchSent()
        {
            _cluster.Start();
            _nodeMocks[0].Raise(n => n.ProduceBatchSent += null, _nodeMocks[0].Object, 3, 14);
            _nodeMocks[0].Raise(n => n.ProduceBatchSent += null, _nodeMocks[0].Object, 3, 14);

            await _cluster.Stop();
            Assert.AreEqual(0, _internalErrors);
            AssertStatistics(_cluster.Statistics, rawProduced: 6, rawProducedBytes: 28);
        }

        [Test]
        public async Task TestFetchResponseReceived()
        {
            _cluster.Start();
            _nodeMocks[0].Raise(n => n.FetchResponseReceived += null, _nodeMocks[0].Object, 3, 14);
            _nodeMocks[0].Raise(n => n.FetchResponseReceived += null, _nodeMocks[0].Object, 3, 14);

            await _cluster.Stop();
            Assert.AreEqual(0, _internalErrors);
            AssertStatistics(_cluster.Statistics, rawReceived: 6, rawReceivedBytes: 28);
        }

        [Test]
        public async Task TestProduceAcknowledgement()
        {
            _cluster.Start();
            var pa = new ProduceAcknowledgement();
            _nodeMocks[0].Raise(n => n.ProduceAcknowledgement += null, _nodeMocks[0].Object, pa);

            await _cluster.Stop();
            _routerMock.Verify(r => r.Acknowledge(pa));

            Assert.AreEqual(0, _internalErrors);
        }

        [Test]
        public void TestFetchAcknowledgement()
        {
            _cluster.Start();
            var ca = new CommonAcknowledgement<FetchResponse>();
            _nodeMocks[0].Raise(n => n.FetchAcknowledgement += null, _nodeMocks[0].Object, ca);

            _consumeMock.Verify(r => r.Acknowledge(ca));

            Assert.AreEqual(0, _internalErrors);
        }

        [Test]
        public void TestOffsetAcknowledgement()
        {
            _cluster.Start();
            var ca = new CommonAcknowledgement<CommonResponse<OffsetPartitionResponse>>();
            _nodeMocks[0].Raise(n => n.OffsetAcknowledgement += null, _nodeMocks[0].Object, ca);

            _consumeMock.Verify(r => r.Acknowledge(ca));

            Assert.AreEqual(0, _internalErrors);
        }

        [Test]
        public void TestConsumerMessageReceived()
        {
            _cluster.Start();
            _consumeMock.Raise(r => r.MessageReceived += null, It.IsAny<RawKafkaRecord>());

            Assert.AreEqual(0, _internalErrors);
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
                        new PartitionMeta{ErrorCode = ErrorCode.NoError, Id = 1, Leader = 1, Isr = TestData.Isr1},
                    }},
                    new TopicMeta {TopicName = "topic2", ErrorCode = ErrorCode.NoError, Partitions = new []
                    {
                        new PartitionMeta{ErrorCode = ErrorCode.NoError, Id = 1, Leader = 1, Isr = TestData.Isr1},
                        new PartitionMeta{ErrorCode = ErrorCode.NoError, Id = 2, Leader = 2, Isr = TestData.Isr1},
                    }},
                    new TopicMeta {TopicName = "topic3", ErrorCode = ErrorCode.NoError, Partitions = new []
                    {
                        new PartitionMeta{ErrorCode = ErrorCode.NoError, Id = 1, Leader = 1, Isr = TestData.Isr1},
                        new PartitionMeta{ErrorCode = ErrorCode.NoError, Id = 2, Leader = 2, Isr = TestData.Isr1},
                        new PartitionMeta{ErrorCode = ErrorCode.NoError, Id = 3, Leader = 3, Isr = TestData.Isr1},
                        new PartitionMeta{ErrorCode = ErrorCode.NoError, Id = 4, Leader = 4, Isr = TestData.Isr1}
                    }},
                    new TopicMeta {TopicName = "error1", ErrorCode = ErrorCode.Unknown, Partitions = new []
                    {
                        new PartitionMeta{ErrorCode = ErrorCode.NoError, Id = 1, Leader = 1, Isr = TestData.Isr1},
                    }},
                    new TopicMeta {TopicName = "error2", ErrorCode = ErrorCode.NoError, Partitions = new []
                    {
                        new PartitionMeta{ErrorCode = ErrorCode.LeaderNotAvailable, Id = 1, Leader = 1, Isr = TestData.Isr1},
                        new PartitionMeta{ErrorCode = ErrorCode.NoError, Id = 2, Leader = 2, Isr = TestData.Isr1},
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

            Assert.AreEqual(0, _internalErrors);
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

            Assert.AreEqual(1, _internalErrors);
        }

        [Test]
        public void TestBehaviorOnFetchAllPartitionsForMissingTopic()
        {
            const string missingTopic = "doesnotexist";
            _cluster.Start();

            Assert.That(async () => await _cluster.RequireAllPartitionsForTopic(missingTopic), Throws.TypeOf<KeyNotFoundException>());

            _logger.Verify(l => l.LogError(It.IsAny<string>()), Times.Once);
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
                        new PartitionMeta{ErrorCode = ErrorCode.NoError, Id = 1, Leader = 1, Isr = TestData.Isr1},
                        new PartitionMeta{ErrorCode = ErrorCode.NoError, Id = 2, Leader = 2, Isr = TestData.Isr1},
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
            Assert.AreEqual(0, _internalErrors);
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
                        new PartitionMeta{ErrorCode = ErrorCode.NoError, Id = 1, Leader = 1, Isr = TestData.Isr1},
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

            Assert.AreEqual(0, _internalErrors);
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

            Assert.AreEqual(0, _internalErrors);
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
                        new PartitionMeta{ErrorCode = ErrorCode.NoError, Id = 1, Leader = 1, Replicas = TestData.Isr1, Isr = TestData.Isr1},
                        new PartitionMeta{ErrorCode = ErrorCode.LeaderNotAvailable, Id = 2, Leader = 2, Replicas = TestData.Isr1, Isr = TestData.Isr1},
                        new PartitionMeta{ErrorCode = ErrorCode.NoError, Id = 3, Leader = 3, Replicas = TestData.Isr1, Isr = TestData.Isr1},
                    }}
                }
            };

            foreach (var nodeMock in _nodeMocks)
            {
                nodeMock.Setup(n => n.FetchMetadata(new[] { "topic1" })).Returns(Task.FromResult(oneTopicMetadataResponse));
            }

            _cluster.Start();
            var partitions = await _cluster.RequireAllPartitionsForTopic("topic1");

            CollectionAssert.AreEqual(new[] { 1, 2, 3 }, partitions);
            Assert.AreEqual(0, _internalErrors);
        }

        [Test]
        public void TestPools()
        {
            var stats = new Statistics();
            var pools = new Pools(stats);
            pools.InitMessageBuffersPool(1, 16);

            var m1 = pools.MessageBuffersPool.Reserve();
            m1.Capacity = 32;
            m1.Dispose();

            var m2 = pools.MessageBuffersPool.Reserve();
            Assert.AreSame(m1, m2);
            Assert.AreEqual(16, m2.Capacity);
            Assert.AreEqual(1, stats.MessageBuffers);

            pools.InitRequestsBuffersPool();
            var r = pools.RequestsBuffersPool.Reserve();
            r.Dispose();
            Assert.AreEqual(1, stats.RequestsBuffers);

            pools.InitSocketBuffersPool(16);
            var b = pools.SocketBuffersPool.Reserve();
            Assert.AreEqual(16, b.Length);
            Assert.AreEqual(1, stats.SocketBuffers);
        }
    }
}
