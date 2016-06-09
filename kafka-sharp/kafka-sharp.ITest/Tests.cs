using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Kafka.Public;
using Newtonsoft.Json;
using NUnit.Framework;

namespace Kafka.ITest
{
    [TestFixture]
    public class Tests
    {
        private string kafkaRootPath;

        private LocalKafkaCommander commander;

        [TestFixtureSetUp]
        public void TestFixtureSetUp()
        {
            /*this.kafkaRootPath = Path.Combine(
                AppDomain.CurrentDomain.BaseDirectory,
                @"..\..\..\..\kafka_2.10-0.8.2.1\bin\windows\");
            this.commander = new LocalKafkaCommander(this.kafkaRootPath);

            this.commander.StartZookeeper();
            Thread.Sleep(TimeSpan.FromSeconds(1));
            broker0 = commander.CreateBroker(0);
            broker1 = commander.CreateBroker(1);
            broker2 = commander.CreateBroker(2);

            Task.WhenAll(
                new[] { commander.StartBroker(broker0),
                        commander.StartBroker(broker1),
                        commander.StartBroker(broker2) })
                .Wait(TimeSpan.FromSeconds(30));

            Thread.Sleep(TimeSpan.FromSeconds(2));*/
        }

        [TestFixtureTearDown]
        public void TestFixtureTearDown()
        {
            /*commander.StopBroker(broker0);
            commander.StopBroker(broker1);
            commander.StopBroker(broker2);
            this.commander.StopZookeeper();

            Thread.Sleep(TimeSpan.FromSeconds(2));

            this.commander.Dispose();*/
        }

        private static readonly Random Random = new Random();

        public static string GetRandomString(int length)
        {
            return new String(Enumerable.Range(0, length).Select(n => (Char)(Random.Next('a', 'z'))).ToArray());
        }

        public static string GetRandomTopic()
        {
            return "topic_" + GetRandomString(6);
        }

        private LocalBroker broker0;
        private LocalBroker broker1;
        private LocalBroker broker2;

        private string topic;

        [SetUp]
        public void SetUp()
        {
            topic = GetRandomTopic();
        }

        IClusterClient NewCluster(
            CompressionCodec codec = CompressionCodec.None,
            string injectedSeedBrokers = null)
        {
            var seeds = injectedSeedBrokers ?? string.Format("127.0.0.1:{0}", broker0.ListenPort);
            return new ClusterClient(new Configuration
            {
                Seeds = seeds,
                RequiredAcks = RequiredAcks.AllInSyncReplicas,
                ErrorStrategy = ErrorStrategy.Retry,
                CompressionCodec = codec,
                MaxBufferedMessages = 1,
            },
                new FakeLogger());
        }

        async Task LaunchNewSendTask(
            IClusterClient cluster,
            int durationInSec,
            ConcurrentQueue<int> sentQueue,
            Func<int> generateMessage)
        {
            var shouldStop = new ManualResetEvent(false);
            var timeout = Task.Delay(TimeSpan.FromSeconds(durationInSec));
            var task = Task.Run(
                () =>
                {
                    while (!shouldStop.WaitOne(0))
                    {
                        var msg = generateMessage();
                        cluster.Produce(topic, Encoding.ASCII.GetBytes(msg.ToString()));
                        sentQueue.Enqueue(msg);
                        Thread.Sleep(5);
                    }
                });
            await timeout;
            shouldStop.Set();
            await task;
        }

        static Action<string> ParseAsIntAndAddToCollection(ICollection<int> received)
        {
            return line =>
            {
                int i;
                if (int.TryParse(line, out i) ||
                    int.TryParse(string.Concat(line.TakeWhile(char.IsDigit)), out i))
                {
                    received.Add(i);
                }
                else
                {
                    Console.WriteLine(line);
                }
            };
        }

        [Ignore("Takes too much time and random")]
        [TestCase(CompressionCodec.None)]
        [TestCase(CompressionCodec.Gzip)]
        [TestCase(CompressionCodec.Snappy)]
        public async void TestMessagesRoundTrip(CompressionCodec codec)
        {
            commander.CreateTopic(topic, 3, 2);
            var consumer = commander.StartConsumer(topic);

            var cluster = NewCluster(codec);
            var received = new List<int>();
            var sent = new ConcurrentQueue<int>();
            var i = 0;
            await this.LaunchNewSendTask(cluster, 5, sent, () => i++);

            commander.StopConsumer(consumer)
                .ToList()
                .ForEach(ParseAsIntAndAddToCollection(received));

            this.Assess(sent, received);
            await cluster.Shutdown();
        }

        [Ignore("Takes too much time and random")]
        [Test]
        public async void TestConcurrentMessagesRoundTrip()
        {
            commander.CreateTopic(topic, 3, 2);
            var consumer = commander.StartConsumer(topic);

            var cluster = NewCluster();
            var received = new List<int>();
            var sent = new ConcurrentQueue<int>();
            var i = 0;
            var task1 = this.LaunchNewSendTask(cluster, 5, sent, () => Interlocked.Increment(ref i));
            var task2 = this.LaunchNewSendTask(cluster, 5, sent, () => Interlocked.Increment(ref i));

            await Task.WhenAll(task1, task2);

            commander.StopConsumer(consumer)
                .ToList()
                .ForEach(ParseAsIntAndAddToCollection(received));

            this.Assess(sent, received);
            await cluster.Shutdown();
        }

        [Ignore("Takes too much time and random")]
        [Test]
        public async void TestBrokerDown()
        {
            commander.CreateTopic(topic, 3, 2);
            var consumer = commander.StartConsumer(topic);

            var cluster = NewCluster();
            var received = new List<int>();
            var sent = new ConcurrentQueue<int>();
            var i = 0;
            var task1 = this.LaunchNewSendTask(cluster, 5, sent, () => Interlocked.Increment(ref i));
            var task2 = this.LaunchNewSendTask(cluster, 5, sent, () => Interlocked.Increment(ref i));
            var task3 = Task.Run(
                async () =>
                {
                    await Task.Delay(TimeSpan.FromSeconds(2));
                    commander.StopBroker(broker1);
                });

            await Task.WhenAll(task1, task2, task3);
            await Task.Delay(TimeSpan.FromSeconds(20));

            commander.StopConsumer(consumer)
                .ToList()
                .ForEach(ParseAsIntAndAddToCollection(received));

            this.Assess(sent, received);
            await cluster.Shutdown();

            await commander.StartBroker(broker1);
            Thread.Sleep(TimeSpan.FromSeconds(5));
        }

        [Ignore("Takes too much time and random")]
        [Test]
        public async void TestSeedBrokerDown()
        {
            commander.CreateTopic(topic, 3, 2);
            var consumer = commander.StartConsumer(topic);

            await Task.Delay(TimeSpan.FromSeconds(2));
            commander.StopBroker(broker0);
            await Task.Delay(TimeSpan.FromSeconds(5));

            var cluster = NewCluster();
            var received = new List<int>();
            var sent = new ConcurrentQueue<int>();
            var i = 0;
            var task1 = this.LaunchNewSendTask(cluster, 8, sent, () => Interlocked.Increment(ref i));
            var task2 = this.LaunchNewSendTask(cluster, 8, sent, () => Interlocked.Increment(ref i));
            var task3 = Task.Run(
                async () =>
                {
                    await Task.Delay(TimeSpan.FromSeconds(1));
                    await commander.StartBroker(broker0);
                    Console.WriteLine("Broker restarted");
                });

            await Task.WhenAll(task1, task2, task3);
            await Task.Delay(TimeSpan.FromSeconds(20));

            commander.StopConsumer(consumer)
                .ToList()
                .ForEach(ParseAsIntAndAddToCollection(received));

            this.Assess(sent, received);
            await cluster.Shutdown();
        }

        [Ignore("Fails too often for now due to kafka# seed broker random choice strategy")]
        [Test]
        public async void TestBadSeedBroker()
        {
            commander.CreateTopic(topic, 3, 2);
            var consumer = commander.StartConsumer(topic);

            var cluster = NewCluster(
                CompressionCodec.None,
                string.Format("127.0.0.1:1027,127.0.0.1:{0}", broker0.ListenPort));
            var received = new List<int>();
            var sent = new ConcurrentQueue<int>();
            var i = 0;
            var task1 = this.LaunchNewSendTask(cluster, 5, sent, () => Interlocked.Increment(ref i));
            var task2 = this.LaunchNewSendTask(cluster, 5, sent, () => Interlocked.Increment(ref i));

            await Task.WhenAll(task1, task2);
            await Task.Delay(TimeSpan.FromSeconds(60));

            commander.StopConsumer(consumer)
                .ToList()
                .ForEach(ParseAsIntAndAddToCollection(received));

            this.Assess(sent, received);
            await cluster.Shutdown();
        }

        private void Assess(ConcurrentQueue<int> sent, List<int> received, Action<int[], int[], IDictionary<int, int>> callback = null)
        {
            var receivedArr = received.ToArray();
            var uniqueReceived = new HashSet<int>(receivedArr).ToArray();
            var missed = sent.Except(uniqueReceived);
            var missedArr = missed.ToArray();

            Array.Sort(uniqueReceived);
            Console.WriteLine("{0} =/=> {1} (missed {2})", sent.Count, received.Count, missedArr.Length);

            var dups = received
                .GroupBy(i => i)
                .Where(g => g.Count() > 1)
                .ToDictionary(g => g.Key, g => g.Count());
            Console.WriteLine("{0} duplicates found: {1}", dups.Count(), JsonConvert.SerializeObject(dups));

            Array.Sort(missedArr);
            Console.WriteLine("Missing ids: {0}", missedArr.Aggregate("", (s, i) => s + i + ","));

            Console.WriteLine("{0} messages did the roundtrip", uniqueReceived.Length);
            Assert.Greater(uniqueReceived.Length, 0);
            CollectionAssert.AreEquivalent(sent, uniqueReceived);

            if (callback != null)
            {
                callback(uniqueReceived, missedArr, dups);
            }
        }

        class FakeLogger : ILogger
        {
            public void LogInformation(string message)
            {
                Console.WriteLine(message);
            }

            public void LogWarning(string message)
            {
                Console.WriteLine(message);
            }

            public void LogError(string message)
            {
                Console.WriteLine(message);
            }
        }
    }
}
