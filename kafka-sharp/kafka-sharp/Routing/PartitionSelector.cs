// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

using System;
using Kafka.Public;

namespace Kafka.Routing
{
    // Select a partition from an array of Partition.
    internal class PartitionSelector
    {
        private ulong _next;
        private ulong _index = 1;
        private readonly ulong _delay;

        public PartitionSelector(int delay = 1)
        {
            _delay = delay <= 0 ? 1UL : (ulong) delay;
        }

        public Partition GetPartition(int partition, Partition[] partitions)
        {
            switch (partition)
            {
                case Partitions.None:
                    return Partition.None;

                case Partitions.Any:
                case Partitions.All:
                    return partitions.Length == 0
                        ? Partition.None
                        : partitions[(int) (_index++%_delay == 0 ? _next++ : _next)%partitions.Length];

                default:
                    int found = Array.BinarySearch(partitions, new Partition {Id = partition});
                    return found >= 0 ? partitions[found] : Partition.None;
            }
        }
    }
}
