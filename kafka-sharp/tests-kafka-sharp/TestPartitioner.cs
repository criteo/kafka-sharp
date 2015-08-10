using Kafka.Protocol;
using Kafka.Routing;
using NUnit.Framework;

namespace tests_kafka_sharp
{
    [TestFixture]
    class TestPartitioner
    {
        [Test]
        public void TestDefaultPartitioner()
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
            var partitioner = new DefaultPartitioner();
            Assert.AreEqual(partitions[0], partitioner.GetPartition(new Message(), partitions));
            Assert.AreEqual(partitions[1], partitioner.GetPartition(new Message(), partitions));
            Assert.AreEqual(partitions[2], partitioner.GetPartition(new Message(), partitions));
            Assert.AreEqual(partitions[3], partitioner.GetPartition(new Message(), partitions));
            Assert.AreEqual(partitions[4], partitioner.GetPartition(new Message(), partitions));
            Assert.AreEqual(partitions[0], partitioner.GetPartition(new Message(), partitions));
        }


        [Test]
        public void TestDefaultPartitionerNoPartitionReturnsNull()
        {
            var partitions = new Partition[0];
            var partitioner = new DefaultPartitioner();
            Assert.IsNull(partitioner.GetPartition(new Message(), partitions));
        }
    }
}
