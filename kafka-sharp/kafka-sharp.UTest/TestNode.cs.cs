using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Kafka.Cluster;
using Kafka.Common;
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
        private static readonly Pool<ReusableMemoryStream> Pool =
            new Pool<ReusableMemoryStream>(() => new ReusableMemoryStream(Pool), (m, b) => { m.SetLength(0); });

        [Test]
        public void TestFetch()
        {
            var ev = new ManualResetEvent(false);
            var connection = new Mock<IConnection>();
            connection.Setup(c => c.SendAsync(It.IsAny<int>(), It.IsAny<ReusableMemoryStream>(), It.IsAny<bool>()))
                .Returns(Success)
                .Callback(() => ev.Set());
            connection.Setup(c => c.ConnectAsync()).Returns(Success);
            var serializer = new Mock<Node.ISerialization>();
            var node =
                new Node("Node", () => connection.Object, serializer.Object,
                    new Configuration {TaskScheduler = new CurrentThreadTaskScheduler(), ConsumeBatchSize = 1}, 1);
            var message = new FetchMessage {Topic = "balbuzzard", Offset = 42, Partition = 1, MaxBytes = 1242};
            node.Fetch(message);
            ev.WaitOne();
            serializer.Verify(
                s => s.SerializeFetchBatch(It.IsAny<int>(), It.IsAny<IEnumerable<IGrouping<string, FetchMessage>>>()),
                Times.Once());
            serializer.Verify(
                s => s.SerializeFetchBatch(It.IsAny<int>(), It.Is<IEnumerable<IGrouping<string, FetchMessage>>>(
                    msgs => msgs.Count() == 1 && msgs.First().Key == message.Topic && msgs.First().First().Equals(message))));
            connection.Verify(c => c.SendAsync(It.IsAny<int>(), It.IsAny<ReusableMemoryStream>(), It.IsAny<bool>()), Times.Once());
        }

        [Test]
        public void TestFetchWithNoErrors()
        {
            // Prepare
            var serializer = new Mock<Node.ISerialization>();
            var connection = new Mock<IConnection>();
            var node = new Node("Node", () => connection.Object, serializer.Object,
                new Configuration {TaskScheduler = new CurrentThreadTaskScheduler(), ConsumeBatchSize = 1}, 1);
            var ev = new ManualResetEvent(false);
            var corrs = new Queue<int>();
            connection.Setup(c => c.SendAsync(It.IsAny<int>(), It.IsAny<ReusableMemoryStream>(), It.IsAny<bool>()))
                .Returns((int c, ReusableMemoryStream d, bool a) =>
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
            node.ResponseReceived += (n, l) =>
            {
                Assert.AreSame(node, n);
                ++response;
            };
            var acknowledgement = new CommonAcknowledgement<FetchPartitionResponse>();
            node.FetchAcknowledgement += (n, ack) =>
            {
                acknowledgement = ack;
                Assert.AreSame(node, n);
            };

            serializer.Setup(s => s.DeserializeCommonResponse<FetchPartitionResponse>(corr, It.IsAny<ReusableMemoryStream>()))
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
                                                new List<ResponseMessage> {new ResponseMessage {Offset = 28, Message = new Message()}}
                                        }
                                    }
                            }
                        }
                });

            bool resp = false;
            node.FetchResponseReceived += (n, c, s) =>
            {
                Assert.That(n, Is.EqualTo(node));
                Assert.That(c, Is.EqualTo(1));
                Assert.That(s ,Is.EqualTo(28));
                resp = true;
            };

            // Now send a response
            var r = new ReusableMemoryStream(null);
            r.SetLength(28);
            connection.Raise(c => c.Response += null, connection.Object, corr, r);

            // Checks
            Assert.IsTrue(resp);
            Assert.AreNotEqual(default(DateTime), acknowledgement.ReceivedDate);
            Assert.AreEqual(1, acknowledgement.Response.TopicsResponse.Length);
            Assert.AreEqual("balbuzzard", acknowledgement.Response.TopicsResponse[0].TopicName);
            Assert.AreEqual(1, acknowledgement.Response.TopicsResponse[0].PartitionsData.Count());
            var fetch = acknowledgement.Response.TopicsResponse[0].PartitionsData.First();
            Assert.AreEqual(ErrorCode.NoError, fetch.ErrorCode);
            Assert.AreEqual(42, fetch.HighWatermarkOffset);
            Assert.AreEqual(1, fetch.Partition);
            Assert.AreEqual(1, fetch.Messages.Count());
            Assert.AreEqual(28, fetch.Messages.First().Offset);
            Assert.AreEqual(new Message(), fetch.Messages.First().Message); // This is not full proof but come on...
        }

        [Test]
        public void TestFetchWithDecodeError()
        {
            // Prepare
            var serializer = new Mock<Node.ISerialization>();
            var connection = new Mock<IConnection>();
            var node = new Node("Node", () => connection.Object, serializer.Object,
                new Configuration { TaskScheduler = new CurrentThreadTaskScheduler(), ConsumeBatchSize = 1}, 1);
            var ev = new ManualResetEvent(false);
            var corrs = new Queue<int>();
            connection.Setup(c => c.SendAsync(It.IsAny<int>(), It.IsAny<ReusableMemoryStream>(), It.IsAny<bool>()))
                .Returns((int c, ReusableMemoryStream d, bool a) =>
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
            node.ResponseReceived += (n, l) =>
            {
                Assert.AreSame(node, n);
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

            serializer.Setup(s => s.DeserializeCommonResponse<FetchPartitionResponse>(corr, It.IsAny<ReusableMemoryStream>()))
                .Throws(new Exception());

            // Now send a response
            connection.Raise(c => c.Response += null, connection.Object, corr, new ReusableMemoryStream(null));

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
            Assert.AreEqual(0, fetch.Messages.Count());
        }

        [Test]
        public void TestFetchTimeout()
        {
            // Prepare
            var serializer = new Mock<Node.ISerialization>();
            var connection = new Mock<IConnection>();
            var node = new Node("Node", () => connection.Object, serializer.Object,
                new Configuration { TaskScheduler = new CurrentThreadTaskScheduler(), ConsumeBatchSize = 1, ClientRequestTimeoutMs = 2},
                new TimeoutScheduler(3), 1);
            var ev = new ManualResetEvent(false);
            connection.Setup(c => c.SendAsync(It.IsAny<int>(), It.IsAny<ReusableMemoryStream>(), It.IsAny<bool>()))
                .Returns(Success);
            connection.Setup(c => c.ConnectAsync()).Returns(Success);
            var message = new FetchMessage { Topic = "balbuzzard", Offset = 42, Partition = 1, MaxBytes = 1242 };

            var acknowledgement = new CommonAcknowledgement<FetchPartitionResponse>();
            node.FetchAcknowledgement += (n, ack) =>
            {
                acknowledgement = ack;
                Assert.AreSame(node, n);

                ev.Set();
            };
            var exception = new Exception();
            node.ConnectionError += (n, e) =>
            {
                exception = e;
            };

            node.Fetch(message);
            ev.WaitOne();

            // Checks
            Assert.IsInstanceOf<TimeoutException>(exception);
            Assert.AreNotEqual(default(DateTime), acknowledgement.ReceivedDate);
            Assert.AreEqual(1, acknowledgement.Response.TopicsResponse.Length);
            Assert.AreEqual("balbuzzard", acknowledgement.Response.TopicsResponse[0].TopicName);
            Assert.AreEqual(1, acknowledgement.Response.TopicsResponse[0].PartitionsData.Count());
            var fetch = acknowledgement.Response.TopicsResponse[0].PartitionsData.First();
            Assert.AreEqual(ErrorCode.LocalError, fetch.ErrorCode);
            Assert.AreEqual(-1, fetch.HighWatermarkOffset);
            Assert.AreEqual(1, fetch.Partition);
            Assert.AreEqual(0, fetch.Messages.Count());
        }

        [Test]
        public void TestOffset()
        {
            var ev = new ManualResetEvent(false);
            var connection = new Mock<IConnection>();
            connection.Setup(c => c.SendAsync(It.IsAny<int>(), It.IsAny<ReusableMemoryStream>(), It.IsAny<bool>()))
                .Returns(Success)
                .Callback(() => ev.Set());
            connection.Setup(c => c.ConnectAsync()).Returns(Success);
            var serializer = new Mock<Node.ISerialization>();
            var node =
                new Node("Node", () => connection.Object, serializer.Object,
                    new Configuration { TaskScheduler = new CurrentThreadTaskScheduler(), ConsumeBatchSize = 1}, 1);
            var message = new OffsetMessage {Topic = "balbuzzard", Partition = 1, MaxNumberOfOffsets = 1, Time = 12341};
            node.Offset(message);
            ev.WaitOne();
            serializer.Verify(
                s => s.SerializeOffsetBatch(It.IsAny<int>(), It.IsAny<IEnumerable<IGrouping<string, OffsetMessage>>>()),
                Times.Once());
            serializer.Verify(
                s => s.SerializeOffsetBatch(It.IsAny<int>(), It.Is<IEnumerable<IGrouping<string, OffsetMessage>>>(
                    msgs => msgs.Count() == 1 && msgs.First().Key == message.Topic && msgs.First().First().Equals(message))));
            connection.Verify(c => c.SendAsync(It.IsAny<int>(), It.IsAny<ReusableMemoryStream>(), It.IsAny<bool>()), Times.Once());
        }

        [Test]
        public void TestOffsetWithNoErrors()
        {
            // Prepare
            var serializer = new Mock<Node.ISerialization>();
            var connection = new Mock<IConnection>();
            var node = new Node("Node", () => connection.Object, serializer.Object,
                new Configuration { TaskScheduler = new CurrentThreadTaskScheduler(), ConsumeBatchSize = 1}, 1);
            var ev = new ManualResetEvent(false);
            var corrs = new Queue<int>();
            connection.Setup(c => c.SendAsync(It.IsAny<int>(), It.IsAny<ReusableMemoryStream>(), It.IsAny<bool>()))
                .Returns((int c, ReusableMemoryStream d, bool a) =>
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
            node.ResponseReceived += (n, l) =>
            {
                Assert.AreSame(node, n);
                ++response;
            };
            var acknowledgement = new CommonAcknowledgement<OffsetPartitionResponse>();
            node.OffsetAcknowledgement += (n, ack) =>
            {
                acknowledgement = ack;
                Assert.AreSame(node, n);
            };

            serializer.Setup(s => s.DeserializeCommonResponse<OffsetPartitionResponse>(corr, It.IsAny<ReusableMemoryStream>()))
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
            connection.Raise(c => c.Response += null, connection.Object, corr, new ReusableMemoryStream(null));

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
            var serializer = new Mock<Node.ISerialization>();
            var connection = new Mock<IConnection>();
            var node = new Node("Node", () => connection.Object, serializer.Object,
                new Configuration { TaskScheduler = new CurrentThreadTaskScheduler(), ConsumeBatchSize = 1}, 1);
            var ev = new ManualResetEvent(false);
            var corrs = new Queue<int>();
            connection.Setup(c => c.SendAsync(It.IsAny<int>(), It.IsAny<ReusableMemoryStream>(), It.IsAny<bool>()))
                .Returns((int c, ReusableMemoryStream d, bool a) =>
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
            node.ResponseReceived += (n, l) =>
            {
                Assert.AreSame(node, n);
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

            serializer.Setup(s => s.DeserializeCommonResponse<OffsetPartitionResponse>(corr, It.IsAny<ReusableMemoryStream>()))
                .Throws(new Exception());

            // Now send a response
            connection.Raise(c => c.Response += null, connection.Object, corr, new ReusableMemoryStream(null));

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
        [Ignore("randomly fails. (last failure: expected instance of TimeoutException but was Exception")]
        public void TestOffsetTimeout()
        {
            // Prepare
            var serializer = new Mock<Node.ISerialization>();
            var connection = new Mock<IConnection>();
            var node = new Node("Node", () => connection.Object, serializer.Object,
                new Configuration { TaskScheduler = new CurrentThreadTaskScheduler(), ConsumeBatchSize = 1, ClientRequestTimeoutMs = 2 },
                new TimeoutScheduler(3), 1);
            var ev = new ManualResetEvent(false);
            connection.Setup(c => c.SendAsync(It.IsAny<int>(), It.IsAny<ReusableMemoryStream>(), It.IsAny<bool>()))
                .Returns(Success);
            connection.Setup(c => c.ConnectAsync()).Returns(Success);
            var message = new OffsetMessage { Topic = "balbuzzard", Partition = 1, MaxNumberOfOffsets = 1, Time = 12341 };
            node.Offset(message);

            var acknowledgement = new CommonAcknowledgement<OffsetPartitionResponse>();
            node.OffsetAcknowledgement += (n, ack) =>
            {
                acknowledgement = ack;
                Assert.AreSame(node, n);
                ev.Set();
            };
            var exception = new Exception();
            node.ConnectionError += (n, e) =>
            {
                exception = e;
            };

            node.Offset(message);
            ev.WaitOne();

            // Checks
            Assert.IsInstanceOf<TimeoutException>(exception);
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
            var node = new Node("Node", () => new EchoConnectionMock(), new MetadataSerialization(new MetadataResponse()),
                                new Configuration(), 1);
            var response = await node.FetchMetadata();
            Assert.IsNotNull(response);
        }

        [Test]
        public void TestFetchMetadataTimeout()
        {
            var con = new Mock<IConnection>();
            con.Setup(c => c.ConnectAsync()).Returns(Task.FromResult(true));
            con.Setup(c => c.SendAsync(It.IsAny<int>(), It.IsAny<ReusableMemoryStream>(), It.IsAny<bool>()))
                .Returns(Task.FromResult(true));
            var node = new Node("Node", () => con.Object, new MetadataSerialization(new MetadataResponse()),
                                new Configuration{ClientRequestTimeoutMs = 1}, new TimeoutScheduler(1), 1);
            bool isDead = false;
            node.Dead += _ => isDead = true;
            var timeoutEvent = new ManualResetEvent(false);
            node.RequestTimeout += n =>
            {
                Assert.AreSame(node, n);
                timeoutEvent.Set();
            };
            Assert.ThrowsAsync<TimeoutException>(async () => await node.FetchMetadata());
            Assert.IsFalse(isDead);

            timeoutEvent.WaitOne();
        }

        [Test]
        public void TestMetadataDecodeError()
        {
            var node = new Node("Pepitomustogussimo", () => new EchoConnectionMock(),
                                new Node.Serialization(null, Pool, new byte[0], RequiredAcks.Leader, 1, CompressionCodec.None, 0, 100),
                                new Configuration());
            Exception ex = null;
            node.DecodeError += (n, e) =>
                {
                    Assert.AreSame(node, n);
                    ex = e;
                };

            var thrown = Assert.ThrowsAsync<Exception>(async () => await node.FetchMetadata());
            Assert.AreSame(thrown, ex);
        }

        [Test]
        public void TestProduceWithNoErrors()
        {
            double requestLatency = 0;
            var expectedLatency = 3;
            var node = new Node("Node", () => new EchoConnectionMock(false, expectedLatency), new ProduceSerialization(new CommonResponse<ProducePartitionResponse>()),
                                new Configuration {ProduceBufferingTime = TimeSpan.FromMilliseconds(15)}, 1);
            var count = new CountdownEvent(2);
            node.ResponseReceived += (n, l) =>
                {
                    Assert.AreSame(node, n);
                    requestLatency = l;
                    count.Signal();
                };
            bool batch = false;
            node.ProduceBatchSent += (n, c, s) =>
            {
                Assert.That(n, Is.EqualTo(node));
                Assert.That(c, Is.EqualTo(1));
                Assert.That(s, Is.EqualTo(0));
                batch = true;
                count.Signal();
            };

            node.Produce(ProduceMessage.New("test", 0, new Message(), DateTime.UtcNow.AddDays(1)));

            count.Wait();
            Assert.IsTrue(batch);
            Assert.GreaterOrEqual(requestLatency, expectedLatency);
        }

        [Test]
        public void TestProduceWithNoErrorsNoAck()
        {
            var node = new Node("Node", () => new EchoConnectionMock(), new ProduceSerialization(new CommonResponse<ProducePartitionResponse>()),
                                new Configuration { ProduceBufferingTime = TimeSpan.FromMilliseconds(15), RequiredAcks = RequiredAcks.None}, 1);
            var count = new CountdownEvent(2);
            bool batch = false;
            ProduceAcknowledgement acknowledgement = new ProduceAcknowledgement();
            node.ProduceBatchSent += (n, c, s) =>
            {
                count.Signal();
            };
            node.ProduceAcknowledgement += (n, ack) =>
            {
                acknowledgement = ack;
                count.Signal();
            };

            node.Produce(ProduceMessage.New("test", 0, new Message(), DateTime.UtcNow.AddDays(1)));

            count.Wait();
            Assert.AreEqual(ErrorCode.NoError, acknowledgement.ProduceResponse.TopicsResponse[0].PartitionsData.First().ErrorCode);
        }

        [Test]
        public void TestProduceRequiredAcks()
        {
            var connection = new Mock<IConnection>();
            var count = new CountdownEvent(1);
            connection.Setup(c => c.ConnectAsync()).Returns(Success);
            connection.Setup(c => c.SendAsync(It.IsAny<int>(), It.IsAny<ReusableMemoryStream>(), It.IsAny<bool>()))
                .Returns(Success)
                .Callback(() => count.Signal());
            var node = new Node("Node", () => connection.Object,
                new ProduceSerialization(new CommonResponse<ProducePartitionResponse>()),
                new Configuration {ProduceBufferingTime = TimeSpan.FromMilliseconds(1), RequiredAcks = RequiredAcks.None}, 1);

            node.Produce(ProduceMessage.New("test", 0, new Message(), DateTime.UtcNow.AddDays(1)));
            count.Wait();

            connection.Verify(c => c.SendAsync(It.IsAny<int>(), It.IsAny<ReusableMemoryStream>(), false), Times.Once());
            connection.Verify(c => c.SendAsync(It.IsAny<int>(), It.IsAny<ReusableMemoryStream>(), true), Times.Never());

            node = new Node("Node", () => connection.Object,
                new ProduceSerialization(new CommonResponse<ProducePartitionResponse>()),
                new Configuration {ProduceBufferingTime = TimeSpan.FromMilliseconds(1), RequiredAcks = RequiredAcks.Leader}, 1);
            count.Reset();
            node.Produce(ProduceMessage.New("test", 0, new Message(), DateTime.UtcNow.AddDays(1)));
            count.Wait();

            connection.Verify(c => c.SendAsync(It.IsAny<int>(), It.IsAny<ReusableMemoryStream>(), true), Times.Once());

            node = new Node("Node", () => connection.Object,
                new ProduceSerialization(new CommonResponse<ProducePartitionResponse>()),
                new Configuration { ProduceBufferingTime = TimeSpan.FromMilliseconds(1), RequiredAcks = RequiredAcks.AllInSyncReplicas }, 1);
            count.Reset();
            node.Produce(ProduceMessage.New("test", 0, new Message(), DateTime.UtcNow.AddDays(1)));
            count.Wait();

            connection.Verify(c => c.SendAsync(It.IsAny<int>(), It.IsAny<ReusableMemoryStream>(), true), Times.Exactly(2));
            connection.Verify(c => c.SendAsync(It.IsAny<int>(), It.IsAny<ReusableMemoryStream>(), false), Times.Once());
        }

        [Test]
        public void TestProduceDecodeErrorAreAcknowledged()
        {
            var node = new Node("Pepitomustogussimo", () => new EchoConnectionMock(),
                                new Node.Serialization(null, Pool, new byte[0], RequiredAcks.Leader, 1, CompressionCodec.None, 0, 100),
                                new Configuration
                                    {
                                        ErrorStrategy = ErrorStrategy.Discard,
                                        ProduceBufferingTime = TimeSpan.FromMilliseconds(15)
                                    }, 1);
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
                    Assert.AreEqual(default(CommonResponse<ProducePartitionResponse>), ack.ProduceResponse);
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
        public void TestProduceTimeout()
        {
            // Prepare
            var connection = new Mock<IConnection>();
            var node = new Node("Node", () => connection.Object,
                new Node.Serialization(null, Pool, new byte[0], RequiredAcks.Leader, 1, CompressionCodec.None, 0, 100),
                new Configuration
                {
                    TaskScheduler = new CurrentThreadTaskScheduler(),
                    ErrorStrategy = ErrorStrategy.Discard,
                    ProduceBufferingTime = TimeSpan.FromMilliseconds(15),
                    ProduceBatchSize = 1,
                    ClientRequestTimeoutMs = 2,
                    MaxInFlightRequests = -1
                },
                new TimeoutScheduler(3), 1);
            var ackEvent = new ManualResetEvent(false);
            var timeoutEvent = new ManualResetEvent(false);
            connection.Setup(c => c.SendAsync(It.IsAny<int>(), It.IsAny<ReusableMemoryStream>(), It.IsAny<bool>()))
                .Returns(Success);
            connection.Setup(c => c.ConnectAsync()).Returns(Success);

            node.ProduceAcknowledgement += (n, ack) =>
            {
                Assert.AreSame(node, n);
                Assert.AreEqual(default(CommonResponse<ProducePartitionResponse>), ack.ProduceResponse);
                Assert.AreNotEqual(default(DateTime), ack.ReceiveDate);
                ackEvent.Set();
            };
            node.RequestTimeout += n =>
            {
                Assert.AreSame(node, n);
                timeoutEvent.Set();
            };

            var exception = new Exception();
            int ex = 0;
            node.ConnectionError += (n, e) =>
            {
                ++ex;
                exception = e;
            };

            node.Produce(ProduceMessage.New("test", 0, new Message(), DateTime.UtcNow.AddDays(1)));
            WaitHandle.WaitAll(new [] { ackEvent, timeoutEvent });

            Assert.IsInstanceOf<TimeoutException>(exception);
        }

        [Test]
        public async Task TestNodeFailingConnectionsMakesMetadataRequestCancelled()
        {
            var config = new Configuration {ProduceBatchSize = 1, ProduceBufferingTime = TimeSpan.FromMilliseconds(15)};
            var node =
                new Node("[Failing node]", () => new ConnectFailingConnectionMock(), new DummySerialization(), config, 1);
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
        public void TestNodeFailingSendMakesMetadataRequestCancelled()
        {
            var config = new Configuration { ProduceBatchSize = 1, ProduceBufferingTime = TimeSpan.FromMilliseconds(15) };
            var node =
                new Node("[Failing node]", () => new SendFailingConnectionMock(), new DummySerialization(), config, 1);

            Assert.ThrowsAsync<TransportException>(async () => await node.FetchMetadata());
        }

        [Test]
        public void TestNodeFailingResponseMakesMetadataRequestError()
        {
            var config = new Configuration { ProduceBatchSize = 1, ProduceBufferingTime = TimeSpan.FromMilliseconds(15) };
            var node =
                new Node("[Failing node]", () => new ReceiveFailingConnectionMock(), new DummySerialization(), config, 1);

            Assert.ThrowsAsync<TransportException>(async () => await node.FetchMetadata());
        }

        [Test]
        public void TestNodeFailToConnectReRouteMessages()
        {
            int rec = 0;
            var ev = new ManualResetEvent(false);
            var config = new Configuration { ProduceBatchSize = 1, ProduceBufferingTime = TimeSpan.FromMilliseconds(15) };
            var node =
                new Node("[Failing node]", () => new ConnectFailingConnectionMock(), new DummySerialization(), config, 1);
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

        [Test]
        public void TestMaxInFlightRequests()
        {
            // Prepare
            var connection = new Mock<IConnection>();
            var node = new Node("Node", () => connection.Object,
                new Node.Serialization(null, Pool, new byte[0], RequiredAcks.Leader, 1, CompressionCodec.None, 0, 100),
                new Configuration
                {
                    TaskScheduler = new CurrentThreadTaskScheduler(),
                    ErrorStrategy = ErrorStrategy.Discard,
                    ProduceBufferingTime = TimeSpan.FromMilliseconds(15),
                    ProduceBatchSize = 1,
                    ClientRequestTimeoutMs = 60000,
                    MaxInFlightRequests = 1
                },
                new TimeoutScheduler(3), 1);
            var ev = new ManualResetEvent(false);
            var corrs = new Queue<int>();
            connection.Setup(c => c.SendAsync(It.IsAny<int>(), It.IsAny<ReusableMemoryStream>(), It.IsAny<bool>()))
                .Returns((int c, ReusableMemoryStream d, bool a) =>
                {
                    corrs.Enqueue(c);
                    return Success;
                });
            connection.Setup(c => c.ConnectAsync()).Returns(Success);

            node.RequestSent += n =>
            {
                ev.Set();
            };

            node.Produce(ProduceMessage.New("test", 0, new Message(), DateTime.UtcNow.AddDays(1)));
            ev.WaitOne();
            ev.Reset();

            node.Produce(ProduceMessage.New("test", 0, new Message(), DateTime.UtcNow.AddDays(1)));
            Assert.IsFalse(ev.WaitOne(50));

            var corr = corrs.Dequeue();
            connection.Raise(c => c.Response += null, connection.Object, corr, new ReusableMemoryStream(null));
            node.Produce(ProduceMessage.New("test", 0, new Message(), DateTime.UtcNow.AddDays(1)));
            ev.WaitOne();
        }
    }
}
