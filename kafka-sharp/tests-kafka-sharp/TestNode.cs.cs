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
        private readonly byte[] _clientId = new[] {(byte) 0};

        [Test]
        public async Task TestNodeFailingConnectionsMakesMetadataRequestCancelled()
        {
            var config = new Configuration {BatchSize = 1, BufferingTime = TimeSpan.FromMilliseconds(15)};
            var node =
                new Node("[Failing node]", _clientId, () => new ConnectFailingConnectionMock(), new RouterMock(), config).SetResolution(1);
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
                new Node("[Failing node]", _clientId, () => new SendFailingConnectionMock(), new RouterMock(), config).SetResolution(1);
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
                new Node("[Failing node]", _clientId, () => new ReceiveFailingConnectionMock(), new RouterMock(), config).SetResolution(1);
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
                new Node("[Failing node]", _clientId, () => new ConnectFailingConnectionMock(), router, config).SetResolution(1);
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
