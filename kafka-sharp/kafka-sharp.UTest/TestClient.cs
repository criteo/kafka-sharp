using System;
using System.Collections.Generic;
using System.Reactive.Linq;
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
        private Mock<INode> _node;

        private void Init(Configuration configuration)
        {
            _node = new Mock<INode>();
            _node.Setup(n => n.FetchMetadata()).Returns(Task.FromResult(new MetadataResponse()));
            _producer = new Mock<IProduceRouter>();
            _consumer = new Mock<IConsumeRouter>();
            var logger = new Mock<ILogger>();
            _client = new ClusterClient(configuration, logger.Object,
                new Cluster(configuration, logger.Object, (h, p) => _node.Object, () => _producer.Object,
                    () => _consumer.Object));
        }

        [SetUp]
        public void SetUp()
        {
            var configuration = new Configuration { Seeds = "localhost:1", TaskScheduler = new CurrentThreadTaskScheduler() };
            Init(configuration);
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
            Assert.IsTrue(_client.Produce(Topic, ValueB));
            _producer.Verify(p => p.Route(It.IsAny<string>(), It.IsAny<Message>(), It.IsAny<int>(), It.IsAny<DateTime>()), Times.Once());
            _producer.Verify(
                p =>
                    p.Route(
                        Topic,
                        It.Is<Message>(m => m.Key == null && AreEqual(ValueB, m.Value as byte[])),
                        Partitions.Any,
                        It.Is<DateTime>(d => d != default(DateTime))));
        }

        [Test]
        public void TestProduceKeyValue()
        {
            Assert.IsTrue(_client.Produce(Topic, KeyB, ValueB));
            _producer.Verify(p => p.Route(It.IsAny<string>(), It.IsAny<Message>(), It.IsAny<int>(), It.IsAny<DateTime>()), Times.Once());
            _producer.Verify(
                p =>
                    p.Route(
                        Topic,
                        It.Is<Message>(m => AreEqual(KeyB, m.Key as byte[]) && AreEqual(ValueB, m.Value as byte[])),
                        Partitions.Any,
                        It.Is<DateTime>(d => d != default(DateTime))));
        }

        [Test]
        public void TestProduceKeyValuePartition()
        {
            Assert.IsTrue(_client.Produce(Topic, KeyB, ValueB, 28));
            _producer.Verify(p => p.Route(It.IsAny<string>(), It.IsAny<Message>(), It.IsAny<int>(), It.IsAny<DateTime>()), Times.Once());
            _producer.Verify(
                p =>
                    p.Route(
                        Topic,
                        It.Is<Message>(m => AreEqual(KeyB, m.Key as byte[]) && AreEqual(ValueB, m.Value as byte[])),
                        28,
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

            VerifyConsume(Topic, Partitions.All, Offsets.Latest);
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

            VerifyConsume(Topic, Partitions.All, Offsets.Earliest);
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

            VerifyStopConsume(Topic, Partitions.All, Offsets.Now);
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
            Assert.IsNotNull(_client.Statistics.ToString());
        }

        void AssertRecords(RawKafkaRecord expected, RawKafkaRecord record)
        {
            Assert.That(record, Is.Not.Null);
            Assert.That(record.Topic, Is.EqualTo(expected.Topic));
            CollectionAssert.AreEqual(expected.Key as byte[], record.Key as byte[]);
            CollectionAssert.AreEqual(expected.Value as byte[], record.Value as byte[]);
            Assert.That(record.Partition, Is.EqualTo(expected.Partition));
            Assert.That(record.Offset, Is.EqualTo(expected.Offset));
        }

        [Test]
        public void TestMessageReceived()
        {
            RawKafkaRecord record = null;
            _client.MessageReceived += kr => record = kr;
            var expected = new RawKafkaRecord {Topic = Topic, Key = KeyB, Value = ValueB, Partition = 1, Offset = 123};
            _consumer.Raise(c => c.MessageReceived += null, expected);
            AssertRecords(expected, record);
        }

        [Test]
        public void TestMessageReceivedObservable()
        {
            RawKafkaRecord record = null;
            _client.Messages.Subscribe(kr => record = kr);
            var expected = new RawKafkaRecord { Topic = Topic, Key = KeyB, Value = ValueB, Partition = 1, Offset = 123 };
            _consumer.Raise(c => c.MessageReceived += null, expected);
            AssertRecords(expected, record);
        }

        [Test]
        public void TestMessageExpired()
        {
            RawKafkaRecord record = null;
            _client.MessageExpired += kr => record = kr;
            _producer.Raise(c => c.MessageExpired += null, Topic, new Message {Key = KeyB, Value = ValueB});
            AssertRecords(
                new RawKafkaRecord {Topic = Topic, Key = KeyB, Value = ValueB, Partition = Partitions.None, Offset = 0},
                record);
        }

        [Test]
        public void TestMessageExpiredObservable()
        {
            RawKafkaRecord record = null;
            _client.ExpiredMessages.Subscribe(kr => record = kr);
            _producer.Raise(c => c.MessageExpired += null, Topic, new Message {Key = KeyB, Value = ValueB});
            AssertRecords(
                new RawKafkaRecord {Topic = Topic, Key = KeyB, Value = ValueB, Partition = Partitions.None, Offset = 0},
                record);
        }

        [Test]
        public void TestMessageDiscarded()
        {
            RawKafkaRecord record = null;
            _client.MessageDiscarded += kr => record = kr;
            _producer.Raise(c => c.MessageDiscarded += null, Topic, new Message { Key = KeyB, Value = ValueB });
            AssertRecords(
                new RawKafkaRecord { Topic = Topic, Key = KeyB, Value = ValueB, Partition = Partitions.None, Offset = 0 },
                record);
        }

        [Test]
        public void TestMessageDiscardedObservable()
        {
            RawKafkaRecord record = null;
            _client.DiscardedMessages.Subscribe(kr => record = kr);
            _producer.Raise(c => c.MessageDiscarded += null, Topic, new Message { Key = KeyB, Value = ValueB });
            AssertRecords(
                new RawKafkaRecord { Topic = Topic, Key = KeyB, Value = ValueB, Partition = Partitions.None, Offset = 0 },
                record);
        }

        [Test]
        public void TestProduceAcknowledged()
        {
            int count = 0;
            string topic = "";
            _client.ProduceAcknowledged += (t, n) =>
            {
                topic = t;
                count = n;
            };
            _producer.Raise(c => c.MessagesAcknowledged += null, Topic, 28);
            Assert.AreEqual(28, count);
            Assert.AreEqual(Topic, topic);
        }

        [Test]
        public void TestProducer()
        {
            var client = new Mock<IClusterClient>();
            client.SetupGet(c => c.DiscardedMessages)
                .Returns(Observable.FromEvent<RawKafkaRecord>(a => client.Object.MessageDiscarded += a,
                    a => client.Object.MessageDiscarded -= a));
            client.SetupGet(c => c.ExpiredMessages)
                .Returns(Observable.FromEvent<RawKafkaRecord>(a => client.Object.MessageExpired += a,
                    a => client.Object.MessageExpired -= a));

            // Bad arguments
            Assert.That(() => new KafkaProducer<string, string>(null, client.Object), Throws.ArgumentException);
            Assert.That(() => new KafkaProducer<string, string>("", client.Object), Throws.ArgumentException);
            Assert.That(() => new KafkaProducer<string, string>("toto", null), Throws.InstanceOf<ArgumentNullException>());

            using (var producer = new KafkaProducer<string, string>("topic", client.Object))
            {
                // Double new on same topic/TKey/TValue
                Assert.That(() => new KafkaProducer<string, string>("topic", client.Object), Throws.ArgumentException);

                // Produce are forwarded to underlying cluster client
                producer.Produce("data");
                producer.Produce("key", "data");
                producer.Produce("key", "data", 42);

                client.Verify(c => c.Produce(It.IsAny<string>(), It.IsAny<object>(), It.IsAny<object>(), It.IsAny<int>()), Times.Exactly(3));
                client.Verify(c => c.Produce("topic", null, "data", Partitions.Any), Times.Once());
                client.Verify(c => c.Produce("topic", "key", "data", Partitions.Any), Times.Once());
                client.Verify(c => c.Produce("topic", "key", "data", 42), Times.Once());

                // Discarded/Expired messages are correctly transmitted
                bool discardedThroughEvent = false;
                bool observedDiscard = false;
                bool expiredThroughEvent = false;
                bool observedExpired = false;

                Action<KafkaRecord<string, string>> checkRecord = kr =>
                {
                    Assert.AreEqual("topic", kr.Topic);
                    Assert.AreEqual("key", kr.Key);
                    Assert.AreEqual("data", kr.Value);
                    Assert.AreEqual(Partitions.None, kr.Partition);
                    Assert.AreEqual(0, kr.Offset);
                };

                producer.MessageDiscarded += kr =>
                {
                    checkRecord(kr);
                    discardedThroughEvent = true;
                };

                producer.MessageExpired += kr =>
                {
                    checkRecord(kr);
                    expiredThroughEvent = true;
                };

                producer.DiscardedMessages.Subscribe(kr =>
                {
                    checkRecord(kr);
                    observedDiscard = true;
                });

                producer.ExpiredMessages.Subscribe(kr =>
                {
                    checkRecord(kr);
                    observedExpired = true;
                });

                var record = new RawKafkaRecord
                {
                    Topic = "topic",
                    Key = "key",
                    Value = "data",
                    Partition = Partitions.None
                };
                client.Raise(c => c.MessageDiscarded += null, record);
                client.Raise(c => c.MessageExpired += null, record);

                Assert.IsTrue(discardedThroughEvent);
                Assert.IsTrue(expiredThroughEvent);
                Assert.IsTrue(observedDiscard);
                Assert.IsTrue(observedExpired);
            }

            // Dispose: can register another producer with same Topic/TKey/TValue once
            // the previous one has been disposed.
            client = new Mock<IClusterClient>();
            client.SetupGet(c => c.DiscardedMessages)
                .Returns(Observable.FromEvent<RawKafkaRecord>(a => client.Object.MessageDiscarded += a,
                    a => client.Object.MessageDiscarded -= a));
            client.SetupGet(c => c.ExpiredMessages)
                .Returns(Observable.FromEvent<RawKafkaRecord>(a => client.Object.MessageExpired += a,
                    a => client.Object.MessageExpired -= a));
            var producer2 = new KafkaProducer<string, string>("topic", client.Object);

            // Dispose: observable are completed and events no longer subscribed
            bool discardedCompleted = false;
            bool expiredCompleted = false;
            bool discardedEvent = false;
            bool expiredEvent = false;
            producer2.DiscardedMessages.Subscribe(kr => { }, () => discardedCompleted = true);
            producer2.ExpiredMessages.Subscribe(kr => { }, () => expiredCompleted = true);
            producer2.MessageDiscarded += _ => discardedEvent = true;
            producer2.MessageExpired += _ => expiredEvent = true;
            producer2.Dispose();

            var record2 = new RawKafkaRecord
            {
                Topic = "topic",
                Key = "key",
                Value = "data",
                Partition = Partitions.None
            };
            client.Raise(c => c.MessageDiscarded += null, record2);
            client.Raise(c => c.MessageExpired += null, record2);

            Assert.IsTrue(discardedCompleted);
            Assert.IsTrue(expiredCompleted);
            Assert.IsFalse(discardedEvent);
            Assert.IsFalse(expiredEvent);

            // Dispose: Produce do no longer work
            producer2.Produce("data");
            client.Verify(c => c.Produce(It.IsAny<string>(), It.IsAny<object>(), It.IsAny<object>(), It.IsAny<int>()), Times.Never());

            // Dispose: can dispose the same producer multiple times with no effect
            Assert.That(() => producer2.Dispose(), Throws.Nothing);
        }

        [Test]
        public void TestConsumer()
        {
            var client = new Mock<IClusterClient>();
            client.SetupGet(c => c.Messages)
                .Returns(Observable.FromEvent<RawKafkaRecord>(a => client.Object.MessageReceived += a,
                    a => client.Object.MessageReceived -= a));

            // Bad arguments
            Assert.That(() => new KafkaConsumer<string, string>(null, client.Object), Throws.ArgumentException);
            Assert.That(() => new KafkaConsumer<string, string>("", client.Object), Throws.ArgumentException);
            Assert.That(() => new KafkaConsumer<string, string>("toto", null), Throws.InstanceOf<ArgumentNullException>());

            using (var consumer = new KafkaConsumer<string, string>("topic", client.Object))
            {
                // Double new on same topic/TKey/TValue
                Assert.That(() => new KafkaConsumer<string, string>("topic", client.Object), Throws.ArgumentException);

                // Consume / Stop
                consumer.Consume(2, 42);
                consumer.ConsumeFromLatest();
                consumer.ConsumeFromLatest(2);
                consumer.ConsumeFromEarliest();
                consumer.ConsumeFromEarliest(2);
                consumer.StopConsume();
                consumer.StopConsume(2);
                consumer.StopConsume(2, 42);

                client.Verify(c => c.Consume(It.IsAny<string>(), It.IsAny<int>(), It.IsAny<long>()), Times.Once());
                client.Verify(c => c.StopConsume("topic", 2, 42));
                client.Verify(c => c.ConsumeFromLatest(It.IsAny<string>()), Times.Once());
                client.Verify(c => c.ConsumeFromLatest("topic"));
                client.Verify(c => c.ConsumeFromLatest(It.IsAny<string>(), It.IsAny<int>()), Times.Once());
                client.Verify(c => c.ConsumeFromLatest("topic", 2));
                client.Verify(c => c.ConsumeFromEarliest(It.IsAny<string>()), Times.Once());
                client.Verify(c => c.ConsumeFromEarliest("topic"));
                client.Verify(c => c.ConsumeFromEarliest(It.IsAny<string>(), It.IsAny<int>()), Times.Once());
                client.Verify(c => c.ConsumeFromEarliest("topic", 2));

                client.Verify(c => c.StopConsume(It.IsAny<string>()), Times.Once());
                client.Verify(c => c.StopConsume("topic"));
                client.Verify(c => c.StopConsume(It.IsAny<string>(), It.IsAny<int>()), Times.Once());
                client.Verify(c => c.StopConsume("topic", 2));
                client.Verify(c => c.StopConsume(It.IsAny<string>(), It.IsAny<int>(), It.IsAny<long>()), Times.Once());
                client.Verify(c => c.StopConsume("topic", 2, 42));

                // Message stream
                Action<KafkaRecord<string, string>> checkRecord = kr =>
                {
                    Assert.AreEqual("topic", kr.Topic);
                    Assert.AreEqual("key", kr.Key);
                    Assert.AreEqual("data", kr.Value);
                    Assert.AreEqual(2, kr.Partition);
                    Assert.AreEqual(42, kr.Offset);
                };

                bool messageObserved = false;
                bool messageEvent = false;

                consumer.MessageReceived += kr =>
                {
                    checkRecord(kr);
                    messageEvent = true;
                };
                consumer.Messages.Subscribe(kr =>
                {
                    checkRecord(kr);
                    messageObserved = true;
                });

                var record = new RawKafkaRecord
                {
                    Topic = "topic",
                    Key = "key",
                    Value = "data",
                    Partition = 2,
                    Offset = 42
                };

                client.Raise(c => c.MessageReceived += null, record);

                Assert.IsTrue(messageEvent);
                Assert.IsTrue(messageObserved);
            }

            // Dispose: can register another producer with same Topic/TKey/TValue once
            // the previous one has been disposed.
            client = new Mock<IClusterClient>();
            client.SetupGet(c => c.Messages)
                .Returns(Observable.FromEvent<RawKafkaRecord>(a => client.Object.MessageReceived += a,
                    a => client.Object.MessageReceived -= a));
            var consumer2 = new KafkaConsumer<string, string>("topic", client.Object);

            // Dispose: observable are completed and events no longer subscribed
            bool messageCompleted = false;
            bool messageEvent2 = false;
            consumer2.Messages.Subscribe(kr => { }, () => messageCompleted = true);
            consumer2.MessageReceived += _ => messageEvent2 = true;
            consumer2.Dispose();

            client.Verify(c => c.StopConsume(It.IsAny<string>()), Times.Once()); // Dispose stops all
            client.Verify(c => c.StopConsume("topic"), Times.Once());

            var record2 = new RawKafkaRecord
            {
                Topic = "topic",
                Key = "key",
                Value = "data",
                Partition = 2,
                Offset = 42
            };
            client.Raise(c => c.MessageReceived += null, record2);

            Assert.IsTrue(messageCompleted);
            Assert.IsFalse(messageEvent2);

            // Consume / Stop no longer work
            consumer2.Consume(2, 42);
            consumer2.ConsumeFromLatest();
            consumer2.ConsumeFromLatest(2);
            consumer2.ConsumeFromEarliest();
            consumer2.ConsumeFromEarliest(2);
            consumer2.StopConsume();
            consumer2.StopConsume(2);
            consumer2.StopConsume(2, 42);

            client.Verify(c => c.Consume(It.IsAny<string>(), It.IsAny<int>(), It.IsAny<long>()), Times.Never());
            client.Verify(c => c.ConsumeFromLatest(It.IsAny<string>()), Times.Never());
            client.Verify(c => c.ConsumeFromLatest(It.IsAny<string>(), It.IsAny<int>()), Times.Never());
            client.Verify(c => c.ConsumeFromEarliest(It.IsAny<string>()), Times.Never());
            client.Verify(c => c.ConsumeFromEarliest(It.IsAny<string>(), It.IsAny<int>()), Times.Never());

            client.Verify(c => c.StopConsume(It.IsAny<string>()), Times.Once());
            client.Verify(c => c.StopConsume(It.IsAny<string>(), It.IsAny<int>()), Times.Never());
            client.Verify(c => c.StopConsume(It.IsAny<string>(), It.IsAny<int>(), It.IsAny<long>()), Times.Never());

            // Dispose: can dispose the same consumer multiple times with no effect
            Assert.That(() => consumer2.Dispose(), Throws.Nothing);
        }

        [Test]
        public void TestGetAllPartitionsForTopic()
        {
            _client.GetPartitionforTopicAsync("toto");
            _node.Verify(n => n.FetchMetadata("toto"));
        }

        [Test]
        public void TestOverflowDiscard()
        {
            var configuration = new Configuration
            {
                Seeds = "localhost:1",
                TaskScheduler = new CurrentThreadTaskScheduler(),
                OverflowStrategy = OverflowStrategy.Discard,
                MaxBufferedMessages = 2
            };
            Init(configuration);
            Assert.IsTrue(_client.Produce(Topic, ValueB));
            Assert.IsTrue(_client.Produce(Topic, ValueB));
            Assert.IsFalse(_client.Produce(Topic, ValueB));
            Assert.AreEqual(2, _client.Statistics.Entered);
            Assert.AreEqual(0, _client.Statistics.Exited);
        }
    }
}