// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

using System;
using System.Collections.Generic;
using Kafka.Cluster;
using Kafka.Public;

namespace Kafka.Routing.PartitionSelection
{
    /// <summary>
    /// Select a partition from an array of Partition.
    /// </summary>
    internal class PartitionSelector
    {
        private readonly IPartitionSelection _partitionSelection;
        private static readonly IReadOnlyDictionary<int, DateTime> EmptyBlackList = new Dictionary<int, DateTime>();

        public PartitionSelector(IPartitionSelection partitionSelection)
        {
            _partitionSelection = partitionSelection;
        }

        /// <summary>
        /// Get the partition the message will be sent to.
        /// Actual selection strategy depends on the IPartitionSelection implementation
        /// that was chosen for this topic.
        /// </summary>
        /// <param name="produceMessage">The ProduceMessage to send</param>
        /// <param name="partitions">List of all available partitions</param>
        /// <param name="blacklist">Dictionary of partition ids that are currently blacklisted</param>
        /// <returns></returns>
        public Partition GetPartition(ProduceMessage produceMessage, Partition[] partitions, IReadOnlyDictionary<int, DateTime> blacklist = null)
        {
            blacklist = blacklist ?? EmptyBlackList;

            switch (produceMessage.RequiredPartition)
            {
                case Partitions.None:
                    return Partition.None;

                case Partitions.Any:
                case Partitions.All:
                    return _partitionSelection.GetPartition(produceMessage, partitions, blacklist);

                default:
                    var found = Array.BinarySearch(partitions, new Partition { Id = produceMessage.RequiredPartition });
                    return found >= 0 ? partitions[found] : Partition.None;
            }
        }
    }
}
