using System.Collections.Generic;
using Kafka.Routing;
using NUnit.Framework;

namespace tests_kafka_sharp
{
    [TestFixture]
    class TestRoutingTable
    {
        [Test]
        public void TestRoutingTableReturnsPartitions()
        {
            var node = new NodeMock();
            var routes = new Dictionary<string, Partition[]>
                {
                    {"test1p", new[] {new Partition {Id = 0, Leader = node}}},
                };
            var routingTable = new RoutingTable(routes);

            var partitions = routingTable.GetPartitions("test1p");
            Assert.AreEqual(1, partitions.Length);
            Assert.AreEqual(0, partitions[0].Id);
            Assert.AreSame(node, partitions[0].Leader);
        }

        [Test]
        public void TestRoutingTableReturnsEmptyForAbsentTopic()
        {
            var node = new NodeMock();
            var routes = new Dictionary<string, Partition[]>
                {
                    {"test1p", new[] {new Partition {Id = 0, Leader = node}}},
                };
            var routingTable = new RoutingTable(routes);

            Assert.Less(0, routingTable.GetPartitions("test1p").Length);
            Assert.AreEqual(0, routingTable.GetPartitions("tortemoque").Length);
        }
    }
}
