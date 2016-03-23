// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

using System;
using System.Collections.Generic;
using System.Linq;
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
        private static readonly Partition[] NullPartition = new Partition[0];

        /// <summary>
        /// Initialize from map topic -> partitions
        /// </summary>
        /// <param name="routes"></param>
        public RoutingTable(Dictionary<string, Partition[]> routes)
        {
            _routes = routes;
            LastRefreshed = DateTime.UtcNow;
        }

        /// <summary>
        /// Initialize from existing routing table, removing partitions with dead nodes
        /// </summary>
        /// <param name="fromTable"></param>
        /// <param name="deadNode"></param>
        public RoutingTable(RoutingTable fromTable, INode deadNode)
        {
            _routes = new Dictionary<string, Partition[]>();
            var tmp = new List<Partition>();
            foreach (var kv in fromTable._routes)
            {
                tmp.AddRange(kv.Value.Where(partition => partition.Leader != deadNode));
                _routes.Add(kv.Key, tmp.ToArray());
                tmp.Clear();
            }
            LastRefreshed = DateTime.UtcNow;
        }

        /// <summary>
        /// Returns the arrays of partitions for a given topic
        /// </summary>
        /// <param name="topic"></param>
        /// <returns></returns>
        public Partition[] GetPartitions(string topic)
        {
            Partition[] partitions;
            _routes.TryGetValue(topic, out partitions);
            return partitions ?? NullPartition;
        }

        public DateTime LastRefreshed { get; internal set; }
    }
}
