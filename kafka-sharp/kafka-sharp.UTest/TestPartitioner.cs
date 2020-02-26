using System;
using System.Collections.Generic;
using System.Linq;
using Kafka.Cluster;
using Kafka.Protocol;
using Kafka.Public;
using Kafka.Routing;
using Kafka.Routing.PartitionSelection;
using NUnit.Framework;

namespace tests_kafka_sharp
{
    [TestFixture]
    class TestPartitioner
    {
        [Test]
        [TestCase(1)]
        [TestCase(2)]
        [TestCase(0)]
        [TestCase(-1)]
        [TestCase(42)]
        public void TestRoundRobinPartitionAssign(int delay)
        {
            var nodeMock = new NodeMock();
            var partitions = new[]
                {
                    new Partition {Id = 0, Leader = nodeMock},
                    new Partition {Id = 1, Leader = nodeMock},
                    new Partition {Id = 2, Leader = nodeMock},
                    new Partition {Id = 3, Leader = nodeMock},
                    new Partition {Id = 4, Leader = nodeMock},
                };
            var partitionStrategy = new RoundRobinPartitionSelection(delay);
            var partitioner = new PartitionSelector(partitionStrategy);
            delay = delay <= 0 ? 1 : delay;
            foreach (var partition in partitions)
            {
                for (var j = 0; j < delay; ++j)
                {
                    Assert.AreEqual(partition.Id, partitioner
                        .GetPartition(ProduceMessage.New(string.Empty, Partitions.Any, new Message(), new DateTime()), partitions)
                        .Id);
                }
            }
        }

        [Test]
        public void TestRoundRobinPartitionAssignNoPartitionReturnsNone()
        {
            var partitions = new Partition[0];
            var partitionStrategy = new RoundRobinPartitionSelection();
            var partitioner = new PartitionSelector(partitionStrategy);
            Assert.AreEqual(0, Partition.None.CompareTo(partitioner.GetPartition(
                ProduceMessage.New(string.Empty, Partitions.Any, new Message(), new DateTime()), partitions)));
        }

        [Test]
        public void TestFilter()
        {
            var nodeMock = new NodeMock();
            var partitions = new[]
                {
                    new Partition {Id = 0, Leader = nodeMock},
                    new Partition {Id = 1, Leader = nodeMock},
                    new Partition {Id = 2, Leader = nodeMock},
                    new Partition {Id = 3, Leader = nodeMock},
                    new Partition {Id = 4, Leader = nodeMock},
                };
            var filter = new Dictionary<int, DateTime>();
            filter[0] = DateTime.UtcNow;
            filter[2] = DateTime.UtcNow;
            filter[4] = DateTime.UtcNow;
            var partitionStrategy = new RoundRobinPartitionSelection();
            var partitioner = new PartitionSelector(partitionStrategy);

            var partition = partitioner.GetPartition(ProduceMessage.New(string.Empty, Partitions.Any, new Message(), new DateTime()),partitions, filter);
            Assert.AreEqual(1, partition.Id);

            partition = partitioner.GetPartition(ProduceMessage.New(string.Empty, Partitions.Any, new Message(), new DateTime()), partitions, filter);
            Assert.AreEqual(3, partition.Id);

            partition = partitioner.GetPartition(ProduceMessage.New(string.Empty, Partitions.Any, new Message(), new DateTime()), partitions, filter);
            Assert.AreEqual(1, partition.Id);
        }

        /// <summary>
        /// Make sure the round-robin threshold is reset when a partition is blacklisted half-way
        /// </summary>
        [Test]
        public void TestRobinPartitionAssignWhenFiltered()
        {
            var nodeMock = new NodeMock();

            var partitions = new[]
            {
                new Partition { Id = 1, Leader = nodeMock },
                new Partition { Id = 2, Leader = nodeMock },
                new Partition { Id = 3, Leader = nodeMock },
            };

            var filter = new Dictionary<int, DateTime>();

            int delay = partitions.Length + 2;

            var partitionStrategy = new RoundRobinPartitionSelection(delay);
            var partitioner = new PartitionSelector(partitionStrategy);

            var partition = partitioner.GetPartition(ProduceMessage.New(string.Empty, Partitions.Any, new Message(), new DateTime()), partitions, filter);

            Assert.AreEqual(1, partition.Id);

            filter.Add(1, DateTime.UtcNow);

            var batch = GetPartitions(delay, partitioner, partitions, filter);

            Assert.AreEqual(delay, batch.Count);
            Assert.IsTrue(batch.All(p => p.Id == 2), "The round-robin threshold wasn't properly reset after previous partition was blacklisted");
        }

        /// <summary>
        /// Regression test: in some cases, the PartitionSelector could fail to find an available partition
        /// if current partition was blacklisted and delay was greater than the number of partition
        /// </summary>
        [Test]
        public void TestFilterWithHighDelay()
        {
            var nodeMock = new NodeMock();

            var partitions = new[]
            {
                new Partition { Id = 1, Leader = nodeMock },
                new Partition { Id = 2, Leader = nodeMock },
                new Partition { Id = 3, Leader = nodeMock }
            };

            var filter = new Dictionary<int, DateTime> { { 2, DateTime.UtcNow } };

            // Pick a delay greater than the number of partitions
            int delay = partitions.Length + 2;

            var partitionStrategy = new RoundRobinPartitionSelection(delay);
            var partitioner = new PartitionSelector(partitionStrategy);

            var firstBatch = GetPartitions(delay, partitioner, partitions, filter);

            Assert.AreEqual(delay, firstBatch.Count);
            Assert.IsTrue(firstBatch.All(p => p.Id == 1));

            var secondBatch = GetPartitions(delay, partitioner, partitions, filter);

            Assert.AreEqual(delay, secondBatch.Count);
            Assert.IsTrue(secondBatch.All(p => p.Id == 3));
        }

        /// <summary>
        /// Test what happens when startSeed is high enough that _next will get bigger than int.MaxValue
        /// This is purely meant as regression testing, as this condition caused issues in previous versions of the code
        /// </summary>
        [Test]
        public void TestOverflow()
        {
            var nodeMock = new NodeMock();

            var partitions = Enumerable.Range(0, 10).Select(i => new Partition { Id = i, Leader = nodeMock }).ToArray();

            var partitionStrategy = new RoundRobinPartitionSelection(delay: 1, startSeed: int.MaxValue);
            var partitioner = new PartitionSelector(partitionStrategy);

            var batch = GetPartitions(partitions.Length, partitioner, partitions, null);

            var ids = batch.Select(p => p.Id).ToArray();

            var expectedIds = new[] { 7, 8, 9, 0, 1, 2, 3, 4, 5, 6 };

            Assert.IsTrue(expectedIds.SequenceEqual(ids));
        }

        [Test]
        [TestCase(0, 1)]
        [TestCase(1, 1)]
        [TestCase(1, 5)]
        [TestCase(42, 1)]
        [TestCase(42, 2)]
        public void TestRoundRobinPartitionWithStartSeed(int startSeed, int delay)
        {
            var nodeMock = new NodeMock();
            var partitions = new[]
                {
                    new Partition {Id = 0, Leader = nodeMock},
                    new Partition {Id = 1, Leader = nodeMock},
                    new Partition {Id = 2, Leader = nodeMock},
                    new Partition {Id = 3, Leader = nodeMock},
                    new Partition {Id = 4, Leader = nodeMock},
                };
            var partitionStrategy = new RoundRobinPartitionSelection(delay: delay, startSeed: startSeed);
            var partitioner = new PartitionSelector(partitionStrategy);
            foreach (var partition in partitions)
            {
                for (var j = 0; j < delay; ++j)
                {
                    Assert.AreEqual((partition.Id + startSeed) % partitions.Length, partitioner.GetPartition(
                        ProduceMessage.New(string.Empty, Partitions.Any, new Message(), new DateTime()), partitions).Id);
                }
            }
        }

        private static List<Partition> GetPartitions(int count, PartitionSelector partitioner, Partition[] partitions, Dictionary<int, DateTime> filter)
        {
            var result = new List<Partition>(count);

            for (int i = 0; i < count; i++)
            {
                result.Add(partitioner.GetPartition(ProduceMessage.New(string.Empty, Partitions.Any, new Message(), new DateTime()), partitions, filter));
            }

            return result;
        }
    }
}
