// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

using System;
using System.Collections.Generic;
using Kafka.Public;

namespace Kafka.Routing
{
    // Select a partition from an array of Partition.
    internal class PartitionSelector
    {
        private ulong _next;
        private ulong _cursor = 1;
        private readonly ulong _delay;
        private static readonly Dictionary<int, DateTime> EmptyBlackList = new Dictionary<int, DateTime>();

        public PartitionSelector(int delay = 1)
        {
            _delay = delay <= 0 ? 1UL : (ulong) delay;
        }

        public Partition GetPartition(int partition, Partition[] partitions, Dictionary<int, DateTime> blacklist = null)
        {
            blacklist = blacklist ?? EmptyBlackList;
            switch (partition)
            {
                case Partitions.None:
                    return Partition.None;

                case Partitions.Any:
                case Partitions.All:
                    var index = 0;
                    while (index < partitions.Length)
                    {
                        var p = partitions[(int) (_cursor++%_delay == 0 ? _next++ : _next)%partitions.Length];
                        if (!blacklist.ContainsKey(p.Id))
                        {
                            return p;
                        }
                        ++index;
                    }
                    return Partition.None;

                default:
                    var found = Array.BinarySearch(partitions, new Partition {Id = partition});
                    return found >= 0 ? partitions[found] : Partition.None;
            }
        }
    }
}
