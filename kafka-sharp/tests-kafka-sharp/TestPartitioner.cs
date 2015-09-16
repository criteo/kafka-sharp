using Kafka.Public;
using Kafka.Routing;
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
            var partitioner = new PartitionSelector(delay);
            delay = delay <= 0 ? 1 : delay;
            foreach (var partition in partitions)
            {
                for (var j = 0; j < delay; ++j)
                {
                    Assert.AreEqual(partition.Id, partitioner.GetPartition(Partitions.Any, partitions).Id);
                }
            }
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
