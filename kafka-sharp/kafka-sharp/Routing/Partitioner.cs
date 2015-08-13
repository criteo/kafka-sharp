// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

using Kafka.Protocol;

namespace Kafka.Routing
{
    interface IPartitioner
    {
        /// <summary>
        /// Assign a message to a partition for produce requests.
        /// If no partition is available this must return null.
        /// </summary>
        /// <param name="message">Message to assign to a partition</param>
        /// <param name="partitions">The list of available partitions. IT should not be null.</param>
        /// <returns>An available partition or Partition.None if none found.</returns>
        Partition GetPartition(Message message, Partition[] partitions);
    }

    /// <summary>
    /// Round robin partitioner.
    /// </summary>
    class DefaultPartitioner : IPartitioner
    {
        private ulong _next;

        public Partition GetPartition(Message dummy, Partition[] partitions)
        {
            return partitions.Length == 0 ? Partition.None : partitions[(int) (_next++)%partitions.Length];
        }
    }
}
