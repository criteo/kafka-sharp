using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Kafka.Cluster;
using Kafka.Network;
using Kafka.Protocol;
using Kafka.Public;
using Kafka.Routing;
using Moq;
using NUnit.Framework;

namespace tests_kafka_sharp
{
    [TestFixture]
    class TestNode
    {
        private static readonly Task Success = Task.FromResult(true);

        [Test]
        public void TestFetch()
        {
            var ev = new ManualResetEvent(false);
            var connection = new Mock<IConnection>();
            connection.Setup(c => c.SendAsync(It.IsAny<int>(), It.IsAny<byte[]>(), It.IsAny<bool>()))
                .Returns(Success)
                .Callback(() => ev.Set());
            connection.Setup(c => c.ConnectAsync()).Returns(Success);
            var serializer = new Mock<Node.ISerializer>();
            var node =
                new Node("Node", () => connection.Object, serializer.Object,
                    new Configuration {TaskScheduler = new CurrentThreadTaskScheduler()}).SetResolution(1);
            var message = new FetchMessage {Topic = "balbuzzard", Offset = 42, Partition = 1, MaxBytes = 1242};
            node.Fetch(message);
            ev.WaitOne();
            serializer.Verify(
                s => s.SerializeFetchBatch(It.IsAny<int>(), It.IsAny<IEnumerable<IGrouping<string, FetchMessage>>>()),
                Times.Once());
            serializer.Verify(
                s => s.SerializeFetchBatch(It.IsAny<int>(), It.Is<IEnumerable<IGrouping<string, FetchMessage>>>(
                    msgs => msgs.Count() == 1 && msgs.First().Key == message.Topic && msgs.First().First().Equals(message))));
            connection.Verify(c => c.SendAsync(It.IsAny<int>(), It.IsAny<byte[]>(), It.IsAny<bool>()), Times.Once());
        }

        [Test]
        public void TestFetchWithNoErrors()
        {
            // Prepare
            var serializer = new Mock<Node.ISerializer>();
            var connection = new Mock<IConnection>();
            var node = new Node("Node", () => connection.Object, serializer.Object,
                new Configuration {TaskScheduler = new CurrentThreadTaskScheduler()}).SetResolution(1);
            var ev = new ManualResetEvent(false);
            var corrs = new Queue<int>();
            connection.Setup(c => c.SendAsync(It.IsAny<int>(), It.IsAny<byte[]>(), It.IsAny<bool>()))
                .Returns((int c, byte[] d, bool a) =>
                {
                    corrs.Enqueue(c);
                    return Success;
                })
                .Callback(() => ev.Set());
            connection.Setup(c => c.ConnectAsync()).Returns(Success);
            var message = new FetchMessage { Topic = "balbuzzard", Offset = 42, Partition = 1, MaxBytes = 1242 };
            node.Fetch(message);
            ev.WaitOne();
            Assert.AreEqual(1, corrs.Count);
            int corr = corrs.Dequeue();
            int response = 0;
            node.ResponseReceived += _ =>
            {
                Assert.AreSame(node, _);
                ++response;
            };
            var acknowledgement = new CommonAcknowledgement<FetchPartitionResponse>();
            node.FetchAcknowledgement += (n, ack) =>
            {
                acknowledgement = ack;
                Assert.AreSame(node, n);
            };

            serializer.Setup(s => s.DeserializeCommonResponse<FetchPartitionResponse>(corr, It.IsAny<byte[]>()))
                .Returns(new CommonResponse<FetchPartitionResponse>
                {
                    TopicsResponse =
                        new[]
                        {
                            new TopicData<FetchPartitionResponse>
                            {
                                TopicName = "balbuzzard",
                                PartitionsData =
                                    new[]
                                    {
                                        new FetchPartitionResponse
                                        {
                                            ErrorCode = ErrorCode.NoError,
                                            HighWatermarkOffset = 42,
                                            Partition = 1,
                                            Messages =
                                                new[] {new ResponseMessage {Offset = 28, Message = new Message()}}
                                        }
                                    }
                            }
                        }
                });

            // Now send a response
            connection.Raise(c => c.Response += null, connection.Object, corr, new byte[0]);

            // Checks
            Assert.AreNotEqual(default(DateTime), acknowledgement.ReceivedDate);
            Assert.AreEqual(1, acknowledgement.Response.TopicsResponse.Length);
            Assert.AreEqual("balbuzzard", acknowledgement.Response.TopicsResponse[0].TopicName);
            Assert.AreEqual(1, acknowledgement.Response.TopicsResponse[0].PartitionsData.Count());
            var fetch = acknowledgement.Response.TopicsResponse[0].PartitionsData.First();
            Assert.AreEqual(ErrorCode.NoError, fetch.ErrorCode);
            Assert.AreEqual(42, fetch.HighWatermarkOffset);
            Assert.AreEqual(1, fetch.Partition);
            Assert.AreEqual(1, fetch.Messages.Length);
            Assert.AreEqual(28, fetch.Messages[0].Offset);
            Assert.AreEqual(new Message(), fetch.Messages[0].Message); // This is not full proof but come on...
        }

        [Test]
        public void TestFetchWithDecodeError()
        {
            // Prepare
            var serializer = new Mock<Node.ISerializer>();
            var connection = new Mock<IConnection>();
            var node = new Node("Node", () => connection.Object, serializer.Object,
                new Configuration { TaskScheduler = new CurrentThreadTaskScheduler() }).SetResolution(1);
            var ev = new ManualResetEvent(false);
            var corrs = new Queue<int>();
            connection.Setup(c => c.SendAsync(It.IsAny<int>(), It.IsAny<byte[]>(), It.IsAny<bool>()))
                .Returns((int c, byte[] d, bool a) =>
                {
                    corrs.Enqueue(c);
                    return Success;
                })
                .Callback(() => ev.Set());
            connection.Setup(c => c.ConnectAsync()).Returns(Success);
            var message = new FetchMessage { Topic = "balbuzzard", Offset = 42, Partition = 1, MaxBytes = 1242 };
            node.Fetch(message);
            ev.WaitOne();
            Assert.AreEqual(1, corrs.Count);
            int corr = corrs.Dequeue();
            int response = 0;
            node.ResponseReceived += _ =>
            {
                Assert.AreSame(node, _);
                ++response;
            };
            var acknowledgement = new CommonAcknowledgement<FetchPartitionResponse>();
            node.FetchAcknowledgement += (n, ack) =>
            {
                acknowledgement = ack;
                Assert.AreSame(node, n);
            };
            int decodeError = 0;
            node.DecodeError += (n, e) => ++decodeError;

            serializer.Setup(s => s.DeserializeCommonResponse<FetchPartitionResponse>(corr, It.IsAny<byte[]>()))
                .Throws(new Exception());

            // Now send a response
            connection.Raise(c => c.Response += null, connection.Object, corr, new byte[0]);

            // Checks
            Assert.AreEqual(1, decodeError);
            Assert.AreNotEqual(default(DateTime), acknowledgement.ReceivedDate);
            Assert.AreEqual(1, acknowledgement.Response.TopicsResponse.Length);
            Assert.AreEqual("balbuzzard", acknowledgement.Response.TopicsResponse[0].TopicName);
            Assert.AreEqual(1, acknowledgement.Response.TopicsResponse[0].PartitionsData.Count());
            var fetch = acknowledgement.Response.TopicsResponse[0].PartitionsData.First();
            Assert.AreEqual(ErrorCode.LocalError, fetch.ErrorCode);
            Assert.AreEqual(-1, fetch.HighWatermarkOffset);
            Assert.AreEqual(1, fetch.Partition);
            Assert.AreEqual(0, fetch.Messages.Length);
        }

        [Test]
        public void TestOffset()
        {
            var ev = new ManualResetEvent(false);
            var connection = new Mock<IConnection>();
            connection.Setup(c => c.SendAsync(It.IsAny<int>(), It.IsAny<byte[]>(), It.IsAny<bool>()))
                .Returns(Success)
                .Callback(() => ev.Set());
            connection.Setup(c => c.ConnectAsync()).Returns(Success);
            var serializer = new Mock<Node.ISerializer>();
            var node =
                new Node("Node", () => connection.Object, serializer.Object,
                    new Configuration { TaskScheduler = new CurrentThreadTaskScheduler() }).SetResolution(1);
            var message = new OffsetMessage {Topic = "balbuzzard", Partition = 1, MaxNumberOfOffsets = 1, Time = 12341};
            node.Offset(message);
            ev.WaitOne();
            serializer.Verify(
                s => s.SerializeOffsetBatch(It.IsAny<int>(), It.IsAny<IEnumerable<IGrouping<string, OffsetMessage>>>()),
                Times.Once());
            serializer.Verify(
                s => s.SerializeOffsetBatch(It.IsAny<int>(), It.Is<IEnumerable<IGrouping<string, OffsetMessage>>>(
                    msgs => msgs.Count() == 1 && msgs.First().Key == message.Topic && msgs.First().First().Equals(message))));
            connection.Verify(c => c.SendAsync(It.IsAny<int>(), It.IsAny<byte[]>(), It.IsAny<bool>()), Times.Once());
        }

        [Test]
        public void TestOffsetWithNoErrors()
        {
            // Prepare
            var serializer = new Mock<Node.ISerializer>();
            var connection = new Mock<IConnection>();
            var node = new Node("Node", () => connection.Object, serializer.Object,
                new Configuration { TaskScheduler = new CurrentThreadTaskScheduler() }).SetResolution(1);
            var ev = new ManualResetEvent(false);
            var corrs = new Queue<int>();
            connection.Setup(c => c.SendAsync(It.IsAny<int>(), It.IsAny<byte[]>(), It.IsAny<bool>()))
                .Returns((int c, byte[] d, bool a) =>
                {
                    corrs.Enqueue(c);
                    return Success;
                })
                .Callback(() => ev.Set());
            connection.Setup(c => c.ConnectAsync()).Returns(Success);
            var message = new OffsetMessage { Topic = "balbuzzard", Partition = 1, MaxNumberOfOffsets = 1, Time = 12341 };
            node.Offset(message);
            ev.WaitOne();
            Assert.AreEqual(1, corrs.Count);
            int corr = corrs.Dequeue();
            int response = 0;
            node.ResponseReceived += _ =>
            {
                Assert.AreSame(node, _);
                ++response;
            };
            var acknowledgement = new CommonAcknowledgement<OffsetPartitionResponse>();
            node.OffsetAcknowledgement += (n, ack) =>
            {
                acknowledgement = ack;
                Assert.AreSame(node, n);
            };

            serializer.Setup(s => s.DeserializeCommonResponse<OffsetPartitionResponse>(corr, It.IsAny<byte[]>()))
                .Returns(new CommonResponse<OffsetPartitionResponse>
                {
                    TopicsResponse =
                        new[]
                        {
                            new TopicData<OffsetPartitionResponse>
                            {
                                TopicName = "balbuzzard",
                                PartitionsData =
                                    new[]
                                    {
                                        new OffsetPartitionResponse
                                        {
                                            ErrorCode = ErrorCode.NoError,
                                            Partition = 1,
                                            Offsets = new [] {27L}
                                        }
                                    }
                            }
                        }
                });

            // Now send a response
            connection.Raise(c => c.Response += null, connection.Object, corr, new byte[0]);

            // Checks
            Assert.AreNotEqual(default(DateTime), acknowledgement.ReceivedDate);
            Assert.AreEqual(1, acknowledgement.Response.TopicsResponse.Length);
            Assert.AreEqual("balbuzzard", acknowledgement.Response.TopicsResponse[0].TopicName);
            Assert.AreEqual(1, acknowledgement.Response.TopicsResponse[0].PartitionsData.Count());
            var fetch = acknowledgement.Response.TopicsResponse[0].PartitionsData.First();
            Assert.AreEqual(ErrorCode.NoError, fetch.ErrorCode);
            Assert.AreEqual(1, fetch.Partition);
            Assert.AreEqual(1, fetch.Offsets.Length);
            Assert.AreEqual(27, fetch.Offsets[0]);
        }

        [Test]
        public void TestOffsetWithDecodeError()
        {
            // Prepare
            var serializer = new Mock<Node.ISerializer>();
            var connection = new Mock<IConnection>();
            var node = new Node("Node", () => connection.Object, serializer.Object,
                new Configuration { TaskScheduler = new CurrentThreadTaskScheduler() }).SetResolution(1);
            var ev = new ManualResetEvent(false);
            var corrs = new Queue<int>();
            connection.Setup(c => c.SendAsync(It.IsAny<int>(), It.IsAny<byte[]>(), It.IsAny<bool>()))
                .Returns((int c, byte[] d, bool a) =>
                {
                    corrs.Enqueue(c);
                    return Success;
                })
                .Callback(() => ev.Set());
            connection.Setup(c => c.ConnectAsync()).Returns(Success);
            var message = new OffsetMessage { Topic = "balbuzzard", Partition = 1, MaxNumberOfOffsets = 1, Time = 12341 };
            node.Offset(message);
            ev.WaitOne();
            Assert.AreEqual(1, corrs.Count);
            int corr = corrs.Dequeue();
            int response = 0;
            node.ResponseReceived += _ =>
            {
                Assert.AreSame(node, _);
                ++response;
            };
            var acknowledgement = new CommonAcknowledgement<OffsetPartitionResponse>();
            node.OffsetAcknowledgement += (n, ack) =>
            {
                acknowledgement = ack;
                Assert.AreSame(node, n);
            };
            int decodeError = 0;
            node.DecodeError += (n, e) => ++decodeError;

            serializer.Setup(s => s.DeserializeCommonResponse<OffsetPartitionResponse>(corr, It.IsAny<byte[]>()))
                .Throws(new Exception());

            // Now send a response
            connection.Raise(c => c.Response += null, connection.Object, corr, new byte[0]);

            // Checks
            Assert.AreEqual(1, decodeError);
            Assert.AreNotEqual(default(DateTime), acknowledgement.ReceivedDate);
            Assert.AreEqual(1, acknowledgement.Response.TopicsResponse.Length);
            Assert.AreEqual("balbuzzard", acknowledgement.Response.TopicsResponse[0].TopicName);
            Assert.AreEqual(1, acknowledgement.Response.TopicsResponse[0].PartitionsData.Count());
            var fetch = acknowledgement.Response.TopicsResponse[0].PartitionsData.First();
            Assert.AreEqual(ErrorCode.LocalError, fetch.ErrorCode);
            Assert.AreEqual(1, fetch.Partition);
            Assert.AreEqual(0, fetch.Offsets.Length);
        }

        [Test]
        public async Task TestFetchMetadata()
        {
            var node = new Node("Node", () => new EchoConnectionMock(), new MetadataSerializer(new MetadataResponse()),
                                new Configuration()).SetResolution(1);
            var response = await node.FetchMetadata();
            Assert.IsNotNull(response);
        }

        [Test]
        public async Task TestMetadataDecodeError()
        {
            var node = new Node("Pepitomustogussimo", () => new EchoConnectionMock(),
                                new Node.Serializer(new byte[0], RequiredAcks.Leader, 1, CompressionCodec.None, 0, 100),
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
            var node = new Node("Node", () => new EchoConnectionMock(), new ProduceSerializer(new ProduceResponse()),
                                new Configuration {BufferingTime = TimeSpan.FromMilliseconds(15)}).SetResolution(1);
            var ev = new ManualResetEvent(false);
            node.ResponseReceived += n =>
                {
                    Assert.AreSame(node, n);
                    ev.Set();
                };

            node.Produce(ProduceMessage.New("test", 0, new Message(), DateTime.UtcNow.AddDays(1)));

            ev.WaitOne();
        }

        [Test]
        public void TestProduceDecodeErrorAreAcknowledged()
        {
            var node = new Node("Pepitomustogussimo", () => new EchoConnectionMock(),
                                new Node.Serializer(new byte[0], RequiredAcks.Leader, 1, CompressionCodec.None, 0, 100),
                                new Configuration
                                    {
                                        ErrorStrategy = ErrorStrategy.Discard,
                                        BufferingTime = TimeSpan.FromMilliseconds(15)
                                    }).SetResolution(1);
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
            node.ProduceAcknowledgement += (n, ack) =>
                {
                    discarded = true;
                    Assert.AreSame(node, n);
                    Assert.AreEqual(default(ProduceResponse), ack.ProduceResponse);
                    Assert.AreNotEqual(default(DateTime), ack.ReceiveDate);
                    if (Interlocked.Increment(ref rec) == 2)
                        ev.Set();
                };
            node.Produce(ProduceMessage.New("test", 0, new Message(), DateTime.UtcNow.AddDays(1)));
            ev.WaitOne();
            Assert.IsNotNull(ex);
            Assert.IsTrue(discarded);
        }

        [Test]
        public async Task TestNodeFailingConnectionsMakesMetadataRequestCancelled()
        {
            var config = new Configuration {BatchSize = 1, BufferingTime = TimeSpan.FromMilliseconds(15)};
            var node =
                new Node("[Failing node]", () => new ConnectFailingConnectionMock(), new DummySerializer(), config).SetResolution(1);
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
                new Node("[Failing node]", () => new SendFailingConnectionMock(), new DummySerializer(), config).SetResolution(1);
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
                new Node("[Failing node]", () => new ReceiveFailingConnectionMock(), new DummySerializer(), config).SetResolution(1);
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
            int rec = 0;
            var ev = new ManualResetEvent(false);
            var config = new Configuration { BatchSize = 1, BufferingTime = TimeSpan.FromMilliseconds(15) };
            var node =
                new Node("[Failing node]", () => new ConnectFailingConnectionMock(), new DummySerializer(), config).SetResolution(1);
            bool dead = false;
            node.Dead += _ =>
            {
                Assert.AreEqual(node, _);
                dead = true;
            };
            node.ProduceAcknowledgement += (n, ack) =>
                {
                    Assert.AreSame(node, n);
                    if (Interlocked.Add(ref rec, ack.OriginalBatch.SelectMany(g => g).Count()) == 2)
                        ev.Set();
                };

            node.Produce(ProduceMessage.New("poulpe", 1, new Message(), DateTime.UtcNow.AddMinutes(5)));
            node.Produce(ProduceMessage.New("poulpe", 2, new Message(), DateTime.UtcNow.AddMinutes(5)));

            ev.WaitOne();
            Assert.IsTrue(dead);
        }
    }
}
