using Kafka.Protocol;
using Kafka.Public;
using Kafka.Routing;
using NUnit.Framework;

namespace tests_kafka_sharp
{
    [TestFixture]
    class TestPartitioner
    {
        [Test]
        public void TestRoundRobinPartitionAssign()
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
            var partitioner = new PartitionSelector();
            Assert.AreEqual(partitions[0], partitioner.GetPartition(Partitions.Any, partitions));
            Assert.AreEqual(partitions[1], partitioner.GetPartition(Partitions.Any, partitions));
            Assert.AreEqual(partitions[2], partitioner.GetPartition(Partitions.Any, partitions));
            Assert.AreEqual(partitions[3], partitioner.GetPartition(Partitions.Any, partitions));
            Assert.AreEqual(partitions[4], partitioner.GetPartition(Partitions.Any, partitions));
            Assert.AreEqual(partitions[0], partitioner.GetPartition(Partitions.Any, partitions));
        }


        [Test]
        public void TestRoundRobinPartitionAssignNoPartitionReturnsNone()
        {
            var partitions = new Partition[0];
            var partitioner = new PartitionSelector();
            Assert.AreEqual(0, Partition.None.CompareTo(partitioner.GetPartition(Partitions.Any, partitions)));
        }
    }
}
