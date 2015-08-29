using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Text;
using System.Threading.Tasks;
using Kafka.Cluster;
using Kafka.Protocol;
using Kafka.Public;
using Kafka.Routing;
using Moq;
using NUnit.Framework;

namespace tests_kafka_sharp
{
    [TestFixture]
    internal class TestClient
    {
        private ClusterClient _client;
        private Mock<IProduceRouter> _producer;
        private Mock<IConsumeRouter> _consumer;

        [SetUp]
        public void SetUp()
        {
            var node = new Mock<INode>();
            node.Setup(n => n.FetchMetadata()).Returns(Task.FromResult(new MetadataResponse()));
            _producer = new Mock<IProduceRouter>();
            _consumer = new Mock<IConsumeRouter>();
            var configuration = new Configuration {Seeds = "localhost:1", TaskScheduler = new CurrentThreadTaskScheduler()};
            var logger = new Mock<ILogger>();
            _client = new ClusterClient(configuration, logger.Object,
                new Cluster(configuration, logger.Object, (h, p) => node.Object, () => _producer.Object,
                    () => _consumer.Object));
        }

        [TearDown]
        public void TearDown()
        {
            _client.Dispose();
        }

        private const string Topic = "topic";
        private const string Key = "Key";
        private const string Value = "Value";
        private readonly byte[] KeyB = Encoding.UTF8.GetBytes(Key);
        private readonly byte[] ValueB = Encoding.UTF8.GetBytes(Value);

        private static bool AreEqual<T>(IEnumerable<T> expected, IEnumerable<T> compared)
        {
            CollectionAssert.AreEqual(expected, compared);
            return true;
        }

        [Test]
        public void TestProduceValue()
        {
            _client.Produce(Topic, ValueB);
            _producer.Verify(p => p.Route(It.IsAny<string>(), It.IsAny<Message>(), It.IsAny<DateTime>()), Times.Once());
            _producer.Verify(
                p =>
                    p.Route(
                        It.Is<string>(s => s == Topic),
                        It.Is<Message>(m => m.Key == null && AreEqual(ValueB, m.Value)),
                        It.Is<DateTime>(d => d != default(DateTime))));
        }

        [Test]
        public void TestProduceKeyValue()
        {
            _client.Produce(Topic, KeyB, ValueB);
            _producer.Verify(p => p.Route(It.IsAny<string>(), It.IsAny<Message>(), It.IsAny<DateTime>()), Times.Once());
            _producer.Verify(
                p =>
                    p.Route(
                        It.Is<string>(s => s == Topic),
                        It.Is<Message>(m => AreEqual(KeyB, m.Key) && AreEqual(ValueB, m.Value)),
                        It.Is<DateTime>(d => d != default(DateTime))));
        }

        [Test]
        public void TestProduceStringValue()
        {
            _client.Produce(Topic, Value);
            _producer.Verify(p => p.Route(It.IsAny<string>(), It.IsAny<Message>(), It.IsAny<DateTime>()), Times.Once());
            _producer.Verify(
                p =>
                    p.Route(
                        It.Is<string>(s => s == Topic),
                        It.Is<Message>(m => m.Key == null && AreEqual(ValueB, m.Value)),
                        It.Is<DateTime>(d => d != default(DateTime))));
        }

        [Test]
        public void TestProduceStringKeyValue()
        {
            _client.Produce(Topic, Key, Value);
            _producer.Verify(p => p.Route(It.IsAny<string>(), It.IsAny<Message>(), It.IsAny<DateTime>()), Times.Once());
            _producer.Verify(
                p =>
                    p.Route(
                        It.Is<string>(s => s == Topic),
                        It.Is<Message>(m => AreEqual(KeyB, m.Key) && AreEqual(ValueB, m.Value)),
                        It.Is<DateTime>(d => d != default(DateTime))));
        }

        private void VerifyConsume(string topic, int partition, long offset)
        {
            _consumer.Verify(c => c.StartConsume(It.IsAny<string>(), It.IsAny<int>(), It.IsAny<long>()), Times.Once());
            _consumer.Verify(c => c.StartConsume(It.Is<string>(s => s == topic), It.Is<int>(p => p == partition), It.Is<long>(o => o == offset)));
        }

        [Test]
        public void TestConsume()
        {
            const int P = 1235;
            const long O = 76158134069;
            _client.Consume(Topic, P, O);

            VerifyConsume(Topic, P, O);
        }

        [Test]
        public void TestConsumeFromLatest()
        {
            const int P = 1235;
            _client.ConsumeFromLatest(Topic, P);

            VerifyConsume(Topic, P, Offsets.Latest);
        }

        [Test]
        public void TestConsumeAllFromLatest()
        {
            _client.ConsumeFromLatest(Topic);

            VerifyConsume(Topic, Partition.All.Id, Offsets.Latest);
        }

        [Test]
        public void TestConsumeFromEarliest()
        {
            const int P = 1235;
            _client.ConsumeFromEarliest(Topic, P);

            VerifyConsume(Topic, P, Offsets.Earliest);
        }

        [Test]
        public void TestConsumeAllFromEarliest()
        {
            _client.ConsumeFromEarliest(Topic);

            VerifyConsume(Topic, Partition.All.Id, Offsets.Earliest);
        }

        private void VerifyStopConsume(string topic, int partition, long offset)
        {
            _consumer.Verify(c => c.StopConsume(It.IsAny<string>(), It.IsAny<int>(), It.IsAny<long>()), Times.Once());
            _consumer.Verify(c => c.StopConsume(It.Is<string>(s => s == topic), It.Is<int>(p => p == partition), It.Is<long>(o => o == offset)));
        }

        [Test]
        public void TestStopConsumePartition()
        {
            const int P = 1235;
            const long O = 76158134069;
            _client.StopConsume(Topic, P, O);

            VerifyStopConsume(Topic, P, O);
        }

        [Test]
        public void TestStopConsumePartitionNow()
        {
            const int P = 1235;
            _client.StopConsume(Topic, P);

            VerifyStopConsume(Topic, P, Offsets.Now);
        }

        [Test]
        public void TestStopConsumeAllNow()
        {
            _client.StopConsume(Topic);

            VerifyStopConsume(Topic, Partition.All.Id, Offsets.Now);
        }

        [Test]
        public void TestConsumeErrors()
        {
            Assert.Throws<ArgumentException>(() => _client.Consume(Topic, -8, 213423));
            Assert.Throws<ArgumentException>(() => _client.Consume(Topic, 3, -213423));
            Assert.Throws<ArgumentException>(() => _client.ConsumeFromLatest(Topic, -8));
            Assert.Throws<ArgumentException>(() => _client.ConsumeFromEarliest(Topic, -8));
            Assert.Throws<ArgumentException>(() => _client.StopConsume(Topic, -8, 215));
            Assert.Throws<ArgumentException>(() => _client.StopConsume(Topic, 1, -215));
            Assert.Throws<ArgumentException>(() => _client.StopConsume(Topic, -8));
        }

        [Test]
        public void TestStats()
        {
            Assert.IsNotNull(_client.Statistics);
        }
    }
}