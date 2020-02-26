using System;
using System.Collections.Generic;
using Kafka.Cluster;

namespace Kafka.Routing.PartitionSelection
{
    /// <summary>
    /// A class responsible of selecting a partition to send a kafka message.
    /// </summary>
    internal interface IPartitionSelection
    {
        /// <summary>
        /// Select a partition to send the message to, taking into account that some partitions might
        /// be temporary blacklisted.
        /// </summary>
        /// <param name="produceMessage">Encapsulation of a message sent to Kafka brokers. It contains additional
        ///   information like the RequiredPartition that indicates we want to target a specific partition</param>
        /// <param name="partitions">The list of all available partitions</param>
        /// <param name="blacklist">Dictionary of partition ids that are temporary blacklisted</param>
        /// <returns>The partition the message will be sent to</returns>
        Partition GetPartition(ProduceMessage produceMessage, Partition[] partitions, IReadOnlyDictionary<int, DateTime> blacklist);
    }
}
