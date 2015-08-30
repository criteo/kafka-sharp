// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

using System;
using System.Collections.Generic;
using Kafka.Cluster;
using Kafka.Public;

namespace Kafka.Routing
{
    struct Partition : IComparable<Partition>
    {
        public int Id { get; set; }
        public INode Leader { get; set; }

        public static Partition None = new Partition {Id = Partitions.None};

        public int CompareTo(Partition other)
        {
            return Id - other.Id;
        }
    }

    class RoutingTable
    {
        private readonly Dictionary<string, Partition[]> _routes;
        private static Partition[] NullPartition = new Partition[0];

        public RoutingTable(Dictionary<string, Partition[]> routes)
        {
            _routes = routes;
            LastRefreshed = DateTime.UtcNow;
        }

        public Partition[] GetPartitions(string topic)
        {
            Partition[] partitions;
            _routes.TryGetValue(topic, out partitions);
            return partitions ?? NullPartition;
        }

        public DateTime LastRefreshed { get; internal set; }
    }
}
