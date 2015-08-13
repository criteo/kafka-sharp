using System;
using System.Linq;
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
