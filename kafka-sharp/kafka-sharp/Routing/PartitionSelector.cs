// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

using System;
using Kafka.Public;

namespace Kafka.Routing
{
    // Select a partition from an array of Partition.
    class PartitionSelector
    {
        private ulong _next;

        public Partition GetPartition(int partition, Partition[] partitions)
        {
            switch (partition)
            {
                case Partitions.None:
                    return Partition.None;

                case Partitions.Any:
                case Partitions.All:
                    return partitions.Length == 0 ? Partition.None : partitions[(int) (_next++)%partitions.Length];

                default:
                    int found = Array.BinarySearch(partitions, new Partition {Id = partition});
                    return found >= 0 ? partitions[found] : Partition.None;
            }
        }
    }
}
