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

        [Test]
        public void TestSignalDeadNode()
        {
            var node = new NodeMock();
            var routes = new Dictionary<string, Partition[]>
            {
                {"test1p", new[] {new Partition {Id = 0, Leader = node}}},
                {"test2p", new[] {new Partition {Id = 1, Leader = new NodeMock()}, new Partition {Id = 2, Leader = node}, new Partition {Id = 3, Leader = new NodeMock()}}},
            };
            var routingTable = new RoutingTable(routes);

            Assert.AreEqual(1, routingTable.GetPartitions("test1p").Length);
            Assert.AreEqual(3, routingTable.GetPartitions("test2p").Length);

            routingTable = new RoutingTable(routingTable, node);

            Assert.AreEqual(0, routingTable.GetPartitions("test1p").Length);
            Assert.AreEqual(2, routingTable.GetPartitions("test2p").Length);
        }

        [Test]
        public void TestFilterMinInSync()
        {
            var node = new NodeMock();
            var routes = new Dictionary<string, Partition[]>
            {
                {"test1p", new[] {new Partition {Id = 0, Leader = node}}},
                {"test2p", new[] {new Partition {Id = 1, Leader = new NodeMock(), NbIsr = 1}, new Partition {Id = 2, Leader = node}, new Partition {Id = 3, Leader = new NodeMock()}}},
            };
            var routingTable = new RoutingTable(routes);

            Assert.AreEqual(1, routingTable.GetPartitions("test1p").Length);
            Assert.AreEqual(3, routingTable.GetPartitions("test2p").Length);

            routingTable = new RoutingTable(routingTable, 1);

            Assert.AreEqual(0, routingTable.GetPartitions("test1p").Length);
            Assert.AreEqual(1, routingTable.GetPartitions("test2p").Length);
        }
    }
}
