// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. 
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

using System.Collections.Generic;
using Kafka.Cluster;

namespace Kafka.Routing
{
    class Partition
    {
        public int Id { get; set; }
        public INode Leader { get; set; }
    }

    class RoutingTable
    {
        private readonly Dictionary<string, Partition[]> _routes;
        private static Partition[] NullPartition = new Partition[0];

        public RoutingTable(Dictionary<string, Partition[]> routes)
        {
            _routes = routes;
        }

        public Partition[] GetPartitions(string topic)
        {
            Partition[] partitions;
            _routes.TryGetValue(topic, out partitions);
            return partitions ?? NullPartition;
        }
    }
}
