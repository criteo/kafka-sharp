using System;
using System.Threading;
using System.Threading.Tasks;
using Kafka.Cluster;
using Kafka.Protocol;
using Kafka.Public;
using NUnit.Framework;

namespace tests_kafka_sharp
{
    [TestFixture]
    class TestNode
    {
        [Test]
        public async Task TestFetchMetadata()
        {
            var node = new Node("Node", () => new EchoConnectionMock(), new MetadataSerializer(new MetadataResponse()),
                                new RouterMock(),
                                new Configuration()).SetResolution(1);
            var response = await node.FetchMetadata();
            Assert.IsNotNull(response);
        }

        [Test]
        public async Task TestMetadataDecodeError()
        {
            var node = new Node("Pepitomustogussimo", () => new EchoConnectionMock(),
                                new Node.Serializer(new byte[0], 1, 1, CompressionCodec.None), new RouterMock(),
                                new Configuration());
            Exception ex = null;
            node.DecodeError += (n, e) =>
                {
                    Assert.AreSame(node, n);
                    ex = e;
                };
            var thrown = Assert.Throws<Exception>(async () => await node.FetchMetadata());
            Assert.AreSame(thrown, ex);
        }

        [Test]
        public void TestProduceWithNoErrors()
        {
            var produceResponse = new ProduceResponse
                {
                    TopicsResponse = new[]
                        {
                            new TopicResponse
                                {
                                    TopicName = "test",
                                    Partitions = new[]
                                        {
                                            new PartitionResponse
                                                {
                                                    ErrorCode = ErrorCode.NoError,
                                                    Offset = 0,
                                                    Partition = 0
                                                }
                                        }
                                }
                        }
                };
            _TestProduceWithNoErrors(produceResponse);
        }

        [Test]
        public void TestProduceReplicaNotAvailableIsNotAnError()
        {
            var produceResponse = new ProduceResponse
            {
                TopicsResponse = new[]
                        {
                            new TopicResponse
                                {
                                    TopicName = "test",
                                    Partitions = new[]
                                        {
                                            new PartitionResponse
                                                {
                                                    ErrorCode = ErrorCode.ReplicaNotAvailable,
                                                    Offset = 0,
                                                    Partition = 0
                                                }
                                        }
                                }
                        }
            };
            _TestProduceWithNoErrors(produceResponse);
        }

        private void _TestProduceWithNoErrors(ProduceResponse produceResponse)
        {
            var node = new Node("Node", () => new EchoConnectionMock(), new ProduceSerializer(produceResponse),
                                new RouterMock(),
                                new Configuration()).SetResolution(1);
            var ev = new ManualResetEvent(false);
            node.SuccessfulSent += (n, t, i) =>
                {
                    Assert.AreSame(node, n);
                    Assert.AreEqual("test", t);
                    Assert.AreEqual(1, i);
                    ev.Set();
                };

            node.Produce(ProduceMessage.New("test", 0, new Message(), DateTime.UtcNow.AddDays(1)));

            ev.WaitOne();
        }

        [Test]
        public void TestProduceDecodeErrorDiscard()
        {
            var node = new Node("Pepitomustogussimo", () => new EchoConnectionMock(),
                                new Node.Serializer(new byte[0], 1, 1, CompressionCodec.None), new RouterMock(),
                                new Configuration {ErrorStrategy = ErrorStrategy.Discard});
            Exception ex = null;
            var ev = new ManualResetEvent(false);
            int rec = 0;
            bool discarded = false;
            node.DecodeError += (n, e) =>
                {
                    Assert.AreSame(node, n);
                    ex = e;
                    if (Interlocked.Increment(ref rec) == 2)
                        ev.Set();
                };
            node.MessagesDiscarded += (n, t, i) =>
                {
                    discarded = true;
                    Assert.AreSame(node, n);
                    Assert.AreEqual("test", t);
                    if (Interlocked.Increment(ref rec) == 2)
                        ev.Set();
                };
            node.Produce(ProduceMessage.New("test", 0, new Message(), DateTime.UtcNow.AddDays(1)));
            ev.WaitOne();
            Assert.IsNotNull(ex);
            Assert.IsTrue(discarded);
        }

        [Test]
        public void TestProduceDecodeErrorRetry()
        {
            Exception ex = null;
            var ev = new ManualResetEvent(false);
            int rec = 0;
            bool routed = false;
            bool discarded = false;
            var router = new RouterMock();
            router.MessageRouted += t =>
                {
                    routed = true;
                    if (Interlocked.Increment(ref rec) == 2)
                        ev.Set();
                };
            var node = new Node("Pepitomustogussimo", () => new EchoConnectionMock(),
                                new Node.Serializer(new byte[0], 1, 1, CompressionCodec.None), router,
                                new Configuration {ErrorStrategy = ErrorStrategy.Retry});
            node.DecodeError += (n, e) =>
                {
                    Assert.AreSame(node, n);
                    ex = e;
                    if (Interlocked.Increment(ref rec) == 2)
                        ev.Set();
                };
            node.MessagesDiscarded += (n, t, i) =>
                {
                    discarded = true;
                    Assert.AreSame(node, n);
                    Assert.AreEqual("test", t);
                    if (Interlocked.Increment(ref rec) == 2)
                        ev.Set();
                };
            node.Produce(ProduceMessage.New("test", 0, new Message(), DateTime.UtcNow.AddDays(1)));
            ev.WaitOne();
            Assert.IsNotNull(ex);
            Assert.IsTrue(routed);
            Assert.IsFalse(discarded);
        }

        [Test]
        public void TestProduceWithNonRecoverableErrors()
        {
            var produceResponse = new ProduceResponse
            {
                TopicsResponse = new[]
                        {
                            new TopicResponse
                                {
                                    TopicName = "test",
                                    Partitions = new []
                                        {
                                            new PartitionResponse
                                                {
                                                    ErrorCode = ErrorCode.NoError,
                                                    Offset = 0,
                                                    Partition = 0
                                                },
                                                new PartitionResponse
                                                {
                                                    ErrorCode = ErrorCode.MessageSizeTooLarge,
                                                    Offset = 0,
                                                    Partition = 1
                                                }
                                        }
                                }
                        }
            };
            var node = new Node("Node", () => new EchoConnectionMock(), new ProduceSerializer(produceResponse),
                                new RouterMock(),
                                new Configuration()).SetResolution(1);
            var ev = new ManualResetEvent(false);
            int rec = 0;
            int success = 0;
            int discarded = 0;
            node.SuccessfulSent += (n, t, i) =>
                {
                    Interlocked.Add(ref success, i);
                    if (Interlocked.Add(ref rec, i) == 2)
                    {
                        ev.Set();
                    }
                };
            node.MessagesDiscarded += (n, t, i) =>
                {
                    Interlocked.Add(ref discarded, i);
                    if (Interlocked.Add(ref rec, i) == 2)
                    {
                        ev.Set();
                    }
                };
            node.Produce(ProduceMessage.New("test", 0, new Message(), DateTime.UtcNow.AddDays(1)));
            node.Produce(ProduceMessage.New("test", 1, new Message(), DateTime.UtcNow.AddDays(1)));
            ev.WaitOne();
            Assert.AreEqual(2, rec);
            Assert.AreEqual(1, success);
            Assert.AreEqual(1, discarded);
        }

        [Test]
        public void TestProduceRecoverableErrorsAreRerouted()
        {
            var produceResponse = new ProduceResponse
                {
                    TopicsResponse = new[]
                        {
                            new TopicResponse
                                {
                                    TopicName = "test",
                                    Partitions = new[]
                                        {
                                            new PartitionResponse
                                                {
                                                    ErrorCode = ErrorCode.NoError,
                                                    Offset = 0,
                                                    Partition = 0
                                                },
                                            new PartitionResponse
                                                {
                                                    ErrorCode = ErrorCode.NotLeaderForPartition,
                                                    Offset = 0,
                                                    Partition = 1
                                                },
                                            new PartitionResponse
                                                {
                                                    ErrorCode = ErrorCode.LeaderNotAvailable,
                                                    Offset = 0,
                                                    Partition = 2
                                                },
                                            new PartitionResponse
                                                {
                                                    ErrorCode = ErrorCode.RequestTimedOut,
                                                    Offset = 0,
                                                    Partition = 3
                                                },
                                            new PartitionResponse
                                                {
                                                    ErrorCode = ErrorCode.UnknownTopicOrPartition,
                                                    Offset = 0,
                                                    Partition = 4
                                                }
                                        }
                                }
                        }
                };

            const int exp = 5;
            var ev = new ManualResetEvent(false);
            int rec = 0;
            int success = 0;
            int discarded = 0;
            int rerouted = 0;
            var router = new RouterMock();
            router.MessageRouted += t =>
                {
                    Interlocked.Increment(ref rerouted);
                    if (Interlocked.Increment(ref rec) == exp)
                    {
                        ev.Set();
                    }
                };
            var node = new Node("Node", () => new EchoConnectionMock(), new ProduceSerializer(produceResponse),
                                router,
                                new Configuration()).SetResolution(1);
            node.SuccessfulSent += (n, t, i) =>
                {
                    Interlocked.Add(ref success, i);
                    if (Interlocked.Add(ref rec, i) == exp)
                    {
                        ev.Set();
                    }
                };
            node.MessagesDiscarded += (n, t, i) =>
                {
                    Interlocked.Add(ref discarded, i);
                    if (Interlocked.Add(ref rec, i) == exp)
                    {
                        ev.Set();
                    }
                };
            node.Produce(ProduceMessage.New("test", 0, new Message(), DateTime.UtcNow.AddDays(1)));
            node.Produce(ProduceMessage.New("test", 1, new Message(), DateTime.UtcNow.AddDays(1)));
            node.Produce(ProduceMessage.New("test", 2, new Message(), DateTime.UtcNow.AddDays(1)));
            node.Produce(ProduceMessage.New("test", 3, new Message(), DateTime.UtcNow.AddDays(1)));
            node.Produce(ProduceMessage.New("test", 4, new Message(), DateTime.UtcNow.AddDays(1)));
            ev.WaitOne();
            Assert.AreEqual(5, rec);
            Assert.AreEqual(1, success);
            Assert.AreEqual(0, discarded);
            Assert.AreEqual(4, rerouted);
        }

        [Test]
        public async Task TestNodeFailingConnectionsMakesMetadataRequestCancelled()
        {
            var config = new Configuration {BatchSize = 1, BufferingTime = TimeSpan.FromMilliseconds(15)};
            var node =
                new Node("[Failing node]", () => new ConnectFailingConnectionMock(), new DummySerializer(), new RouterMock(), config).SetResolution(1);
            bool dead = false;
            node.Dead += _ =>
            {
                Assert.AreEqual(node, _);
                dead = true;
            };
            try
            {
                var m = await node.FetchMetadata();
                Assert.IsFalse(true);
            }
            catch (Exception e)
            {
                Assert.IsInstanceOf<OperationCanceledException>(e);
            }
            Assert.IsTrue(dead);
        }

        [Test]
        public async Task TestNodeFailingSendMakesMetadataRequestCancelled()
        {
            var config = new Configuration { BatchSize = 1, BufferingTime = TimeSpan.FromMilliseconds(15) };
            var node =
                new Node("[Failing node]", () => new SendFailingConnectionMock(), new DummySerializer(), new RouterMock(), config).SetResolution(1);
            try
            {
                var m = await node.FetchMetadata();
                Assert.IsFalse(true);
            }
            catch (Exception e)
            {
                Assert.IsInstanceOf<OperationCanceledException>(e);
            }
        }

        [Test]
        public async Task TestNodeFailingResponseMakesMetadataRequestCancelled()
        {
            var config = new Configuration { BatchSize = 1, BufferingTime = TimeSpan.FromMilliseconds(15) };
            var node =
                new Node("[Failing node]", () => new ReceiveFailingConnectionMock(), new DummySerializer(), new RouterMock(), config).SetResolution(1);
            try
            {
                var m = await node.FetchMetadata();
                Assert.IsFalse(true);
            }
            catch (Exception e)
            {
                Assert.IsInstanceOf<OperationCanceledException>(e);
            }
        }

        [Test]
        public void TestNodeFailToConnectReRouteMessages()
        {
            var router = new RouterMock();
            int rec = 0;
            var ev = new ManualResetEvent(false);
            router.MessageRouted += _ =>
            {
                if (Interlocked.Increment(ref rec) == 2)
                {
                    ev.Set();
                }
            };
            var config = new Configuration { BatchSize = 1, BufferingTime = TimeSpan.FromMilliseconds(15) };
            var node =
                new Node("[Failing node]", () => new ConnectFailingConnectionMock(), new DummySerializer(), router, config).SetResolution(1);
            bool dead = false;
            node.Dead += _ =>
            {
                Assert.AreEqual(node, _);
                dead = true;
            };

            node.Produce(ProduceMessage.New("poulpe", 1, new Message(), DateTime.UtcNow.AddMinutes(5)));
            node.Produce(ProduceMessage.New("poulpe", 2, new Message(), DateTime.UtcNow.AddMinutes(5)));

            ev.WaitOne();
            Assert.IsTrue(dead);
        }
    }
}
