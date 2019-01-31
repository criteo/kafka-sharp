using System;
using System.Collections.Generic;
using Kafka.Cluster;
using Kafka.Protocol;
using Kafka.Public;
using Kafka.Routing;
using Kafka.Routing.PartitionSelection;
using NUnit.Framework;

namespace tests_kafka_sharp
{
    internal class TestMessageKeyPartitionSelection
    {
        private static readonly ISerializer Serializer = new StringSerializer();
        private static readonly RoundRobinPartitionSelection RoundRobinPartitionSelection = new RoundRobinPartitionSelection();

        [Test]
        public void Test_MessageKeyPartitionSelection_Is_Consistent()
        {
            var nodeMock = new NodeMock();
            var partitions = new[]
            {
                new Partition {Id = 0, Leader = nodeMock},
                new Partition {Id = 1, Leader = nodeMock},
                new Partition {Id = 2, Leader = nodeMock},
            };
            var partitionStrategy = new MessageKeyPartitionSelection(Serializer, RoundRobinPartitionSelection);
            var partitioner = new PartitionSelector(partitionStrategy);
            var message1 = ProduceMessage.New(string.Empty, Partitions.Any, new Message { Key = "ThisIsMyKey" }, new DateTime());
            var message2 = ProduceMessage.New(string.Empty, Partitions.Any, new Message { Key = "ThisIsMyOtherKey" }, new DateTime());

            var expectedPartition1 = partitioner.GetPartition(message1, partitions);
            var expectedPartition2 = partitioner.GetPartition(message2, partitions);
            for (var i = 0; i < 300; i++)
            {
                var currentPartition1 = partitioner.GetPartition(message1, partitions);
                var currentPartition2 = partitioner.GetPartition(message2, partitions);
                Assert.AreEqual(expectedPartition1.Id, currentPartition1.Id);
                Assert.AreEqual(expectedPartition2.Id, currentPartition2.Id);
            }
        }

        [Test]
        public void Test_MessageKeyPartitionSelection_Fallbacks_To_RoundRobin_If_MessageKey_Null()
        {
            var nodeMock = new NodeMock();
            var partitions = new[]
            {
                new Partition {Id = 0, Leader = nodeMock},
                new Partition {Id = 1, Leader = nodeMock},
                new Partition {Id = 2, Leader = nodeMock},
            };
            var partitionStrategy = new MessageKeyPartitionSelection(Serializer, RoundRobinPartitionSelection);
            var partitioner = new PartitionSelector(partitionStrategy);
            var message = ProduceMessage.New(string.Empty, Partitions.Any, new Message { Key = null }, new DateTime());

            var partition = partitioner.GetPartition(message, partitions);
            Assert.IsTrue(partition.Id != Partition.None.Id);
        }

        [TestCase(0)]
        [TestCase(1)]
        [TestCase(2)]
        public void Test_MessageKeyPartitionSelection_Fallbacks_To_RoundRobin_If_Partition_Blacklisted(int partitionIdBlacklisted)
        {
            var nodeMock = new NodeMock();
            var partitions = new[]
            {
                new Partition {Id = 0, Leader = nodeMock},
                new Partition {Id = 1, Leader = nodeMock},
                new Partition {Id = 2, Leader = nodeMock},
            };
            var blacklistedPartitions = new Dictionary<int, DateTime> { { partitionIdBlacklisted, DateTime.MaxValue } };
            var partitionStrategy = new MessageKeyPartitionSelection(Serializer, RoundRobinPartitionSelection);
            var partitioner = new PartitionSelector(partitionStrategy);
            var message = ProduceMessage.New(string.Empty, Partitions.Any, new Message { Key = "ThisIsMyKey" }, new DateTime());

            for (var i = 0; i < 300; i++)
            {
                var partition = partitioner.GetPartition(message, partitions, blacklistedPartitions);
                Assert.IsTrue(partition.Id != Partition.None.Id);
                Assert.IsTrue(partition.Id != partitionIdBlacklisted);
            }
        }
    }
}
