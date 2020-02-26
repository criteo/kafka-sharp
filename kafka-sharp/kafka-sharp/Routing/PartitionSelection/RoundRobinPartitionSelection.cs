using System;
using System.Collections.Generic;
using Kafka.Cluster;

namespace Kafka.Routing.PartitionSelection
{
    internal class RoundRobinPartitionSelection : IPartitionSelection
    {
        private long _next;
        private int _cursor;
        private readonly int _delay;

        public RoundRobinPartitionSelection(int delay = 1, int startSeed = 0)
        {
            _delay = delay <= 0 ? 1 : delay;
            _next = startSeed <= 0 ? 0L : startSeed;
        }

        public Partition GetPartition(ProduceMessage produceMessage, Partition[] partitions, IReadOnlyDictionary<int, DateTime> blacklist)
        {
            return GetRandomPartition(partitions, blacklist);
        }

        private Partition GetRandomPartition(Partition[] partitions, IReadOnlyDictionary<int, DateTime> blacklist)
        {
            for (var retryCount = 0; retryCount < partitions.Length; retryCount++)
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
