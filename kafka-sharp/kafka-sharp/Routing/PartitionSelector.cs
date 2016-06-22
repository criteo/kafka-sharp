// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

using System;
using System.Collections.Generic;
using Kafka.Public;

namespace Kafka.Routing
{
    /// <summary>
    /// Select a partition from an array of Partition.
    /// </summary>
    internal class PartitionSelector
    {
        private long _next;
        private int _cursor;
        private readonly int _delay;
        private static readonly IReadOnlyDictionary<int, DateTime> EmptyBlackList = new Dictionary<int, DateTime>();

        public PartitionSelector(int delay = 1, int startSeed = 0)
        {
            _delay = delay <= 0 ? 1 : delay;
            _next = startSeed <= 0 ? 0L : startSeed;
        }

        public Partition GetPartition(int partitionId, Partition[] partitions, IReadOnlyDictionary<int, DateTime> blacklist = null)
        {
            blacklist = blacklist ?? EmptyBlackList;

            switch (partitionId)
            {
                case Partitions.None:
                    return Partition.None;

                case Partitions.Any:
                case Partitions.All:
                    for (int retryCount = 0; retryCount < partitions.Length; retryCount++)
                    {
                        var partition = GetActivePartition(partitions);

                        if (blacklist.ContainsKey(partition.Id))
                        {
                            SelectNextPartition();
                            continue;
                        }

                        _cursor++;

                        if (_cursor >= _delay)
                        {
                            // Round-robin threshold met, switch to next partition for the next call
                            SelectNextPartition();
                        }

                        return partition;
                    }

                    return Partition.None;

                default:
                    var found = Array.BinarySearch(partitions, new Partition { Id = partitionId });
                    return found >= 0 ? partitions[found] : Partition.None;
            }
        }

        private Partition GetActivePartition(Partition[] partitions)
        {
            return partitions[_next % partitions.Length];
        }

        private void SelectNextPartition()
        {
            _next++;

            // Reset the hit count for current partition
            _cursor = 0;
        }
    }
}
