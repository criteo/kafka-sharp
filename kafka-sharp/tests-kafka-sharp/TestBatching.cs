using System;
using System.Linq;
using System.Threading;
using Kafka.Batching;
using Moq;
using NUnit.Framework;

namespace tests_kafka_sharp
{
    using Group = IGrouping<string, Tuple<string, int>>;

    [TestFixture]
    internal class TestBatching
    {
        [Test]
        public void TestAccumulatorByTopicCountReached()
        {
            using (var accumulator = new AccumulatorByTopic<Tuple<string, int>>(t => t.Item1, 5,
                TimeSpan.FromMilliseconds(1000000)))
            {
                IBatchByTopic<Tuple<string, int>> batch = null;
                accumulator.NewBatch += b => batch = b;
                accumulator.Add(Tuple.Create("a", 1));
                accumulator.Add(Tuple.Create("a", 2));
                accumulator.Add(Tuple.Create("b", 8));
                accumulator.Add(Tuple.Create("a", 3));
                accumulator.Add(Tuple.Create("c", 1));
                accumulator.Add(Tuple.Create("a", 1));

                Assert.That(batch.Count, Is.EqualTo(5));
                Assert.That(batch.Count(g => g.Key == "a"), Is.EqualTo(1));
                Assert.That(batch.Count(g => g.Key == "b"), Is.EqualTo(1));
                Assert.That(batch.Count(g => g.Key == "c"), Is.EqualTo(1));
                CollectionAssert.AreEquivalent(new[] {1, 2, 3}, batch.First(g => g.Key == "a").Select(t => t.Item2));
                CollectionAssert.AreEquivalent(new[] {8}, batch.First(g => g.Key == "b").Select(t => t.Item2));
                CollectionAssert.AreEquivalent(new[] {1}, batch.First(g => g.Key == "c").Select(t => t.Item2));

                accumulator.Add(Tuple.Create("a", 1));
                accumulator.Add(Tuple.Create("a", 1));
                accumulator.Add(Tuple.Create("a", 1));
                accumulator.Add(Tuple.Create("a", 1));
                Assert.That(batch.Count, Is.EqualTo(5));
                Assert.That(batch.Count(g => g.Key == "a"), Is.EqualTo(1));
                Assert.That(batch.Count(), Is.EqualTo(1));
                Assert.That(batch.First().Count(), Is.EqualTo(5));
                Assert.That(batch.First().Count(t => t.Item2 == 1), Is.EqualTo(5));
                batch.Dispose();
            }
        }

        [Test]
        public void TestAccumulatorByTopicTimeElapsed()
        {
            using (var accumulator = new AccumulatorByTopic<Tuple<string, int>>(t => t.Item1, 5,
                TimeSpan.FromMilliseconds(15)))
            {
                IBatchByTopic<Tuple<string, int>> batch = null;
                accumulator.NewBatch += b => batch = b;
                Assert.IsTrue(accumulator.Add(Tuple.Create("a", 1)));
                Assert.IsTrue(accumulator.Add(Tuple.Create("b", 2)));
                Assert.IsTrue(accumulator.Add(Tuple.Create("c", 3)));

                while (batch == null) ;

                Assert.That(batch.Count, Is.EqualTo(3));
                Assert.That(batch.Count(g => g.Key == "a"), Is.EqualTo(1));
                Assert.That(batch.Count(g => g.Key == "b"), Is.EqualTo(1));
                Assert.That(batch.Count(g => g.Key == "c"), Is.EqualTo(1));
                CollectionAssert.AreEquivalent(new[] {1}, batch.First(g => g.Key == "a").Select(t => t.Item2));
                CollectionAssert.AreEquivalent(new[] {2}, batch.First(g => g.Key == "b").Select(t => t.Item2));
                CollectionAssert.AreEquivalent(new[] {3}, batch.First(g => g.Key == "c").Select(t => t.Item2));
                batch.Dispose();
            }
        }

        [Test]
        public void TestAccumulatorByTopicByPartitionCountReached()
        {
            using (
                var accumulator = new AccumulatorByTopicByPartition<Tuple<string, int, int>>(t => t.Item1, t => t.Item2,
                    5,
                    TimeSpan.FromMilliseconds(1000000)))
            {
                IBatchByTopicByPartition<Tuple<string, int, int>> batch = null;
                accumulator.NewBatch += b => batch = b;
                accumulator.Add(Tuple.Create("a", 1, 1));
                accumulator.Add(Tuple.Create("a", 1, 2));
                accumulator.Add(Tuple.Create("b", 1, 8));
                accumulator.Add(Tuple.Create("a", 2, 3));
                accumulator.Add(Tuple.Create("c", 1, 1));
                accumulator.Add(Tuple.Create("a", 1, 1));

                Assert.That(batch.Count, Is.EqualTo(5));
                Assert.That(batch.Count(g => g.Key == "a"), Is.EqualTo(1));
                Assert.That(batch.Count(g => g.Key == "b"), Is.EqualTo(1));
                Assert.That(batch.Count(g => g.Key == "c"), Is.EqualTo(1));
                CollectionAssert.AreEquivalent(new[] {1, 2},
                    batch.First(g => g.Key == "a").Where(g => g.Key == 1).SelectMany(g => g).Select(t => t.Item3));
                CollectionAssert.AreEquivalent(new[] {1, 2, 3},
                    batch.First(g => g.Key == "a").SelectMany(g => g).Select(t => t.Item3));
                CollectionAssert.AreEquivalent(new[] {8},
                    batch.First(g => g.Key == "b").SelectMany(g => g.Select(t => t.Item3)));
                CollectionAssert.AreEquivalent(new[] {1},
                    batch.First(g => g.Key == "c").SelectMany(g => g.Select(t => t.Item3)));

                accumulator.Add(Tuple.Create("a", 1, 1));
                accumulator.Add(Tuple.Create("a", 1, 1));
                accumulator.Add(Tuple.Create("a", 2, 1));
                accumulator.Add(Tuple.Create("a", 2, 1));
                Assert.That(batch.Count, Is.EqualTo(5));
                Assert.That(batch.Count(g => g.Key == "a"), Is.EqualTo(1));
                Assert.That(batch.Count(), Is.EqualTo(1));
                Assert.That(batch.First().Count(), Is.EqualTo(2));
                Assert.That(batch.First().Where(g => g.Key == 1).SelectMany(g => g).Count(t => t.Item3 == 1),
                    Is.EqualTo(3));
                Assert.That(batch.First().Where(g => g.Key == 2).SelectMany(g => g).Count(t => t.Item3 == 1),
                    Is.EqualTo(2));
                Assert.That(batch.First().SelectMany(g => g).Count(t => t.Item3 == 1), Is.EqualTo(5));
                batch.Dispose();
            }
        }

        [Test]
        public void TestAccumulatorByTopicByPartitionTimeElapsed()
        {
            using (
                var accumulator = new AccumulatorByTopicByPartition<Tuple<string, int, int>>(t => t.Item1, t => t.Item2,
                    5,
                    TimeSpan.FromMilliseconds(15)))
            {
                IBatchByTopicByPartition<Tuple<string, int, int>> batch = null;
                accumulator.NewBatch += b => batch = b;
                Assert.IsTrue(accumulator.Add(Tuple.Create("a", 1, 1)));
                Assert.IsTrue(accumulator.Add(Tuple.Create("b", 1, 2)));
                Assert.IsTrue(accumulator.Add(Tuple.Create("c", 1, 3)));

                while (batch == null) ;

                Assert.That(batch.Count, Is.EqualTo(3));
                Assert.That(batch.Count(g => g.Key == "a"), Is.EqualTo(1));
                Assert.That(batch.Count(g => g.Key == "b"), Is.EqualTo(1));
                Assert.That(batch.Count(g => g.Key == "c"), Is.EqualTo(1));
                CollectionAssert.AreEquivalent(new[] {1},
                    batch.First(g => g.Key == "a").SelectMany(g => g).Select(t => t.Item3));
                CollectionAssert.AreEquivalent(new[] {2},
                    batch.First(g => g.Key == "b").SelectMany(g => g).Select(t => t.Item3));
                CollectionAssert.AreEquivalent(new[] {3},
                    batch.First(g => g.Key == "c").SelectMany(g => g).Select(t => t.Item3));
                batch.Dispose();
            }
        }

        [Test]
        public void TestDisposeAccumulator()
        {
            var accumulator = new AccumulatorByTopic<Tuple<string, int>>(t => t.Item1, 5,
                TimeSpan.FromMilliseconds(1000000));
            IBatchByTopic<Tuple<string, int>> batch = null;
            accumulator.NewBatch += b => batch = b;
            Assert.IsTrue(accumulator.Add(Tuple.Create("a", 1)));
            Assert.IsTrue(accumulator.Add(Tuple.Create("a", 2)));
            Assert.IsTrue(accumulator.Add(Tuple.Create("a", 3)));
            Assert.IsNull(batch);

            accumulator.Dispose();

            Assert.IsNotNull(batch);
            Assert.That(batch.Count, Is.EqualTo(3));
            Assert.IsFalse(accumulator.Add(Tuple.Create("a", 1)));
        }
    }
}