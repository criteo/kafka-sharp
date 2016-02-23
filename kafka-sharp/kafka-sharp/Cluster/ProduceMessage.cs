// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

using System;
using Kafka.Protocol;

namespace Kafka.Cluster
{
    /// <summary>
    /// Encapsulation of a message sent to Kafka brokers.
    /// </summary>
    struct ProduceMessage
    {
        public string Topic;
        public Message Message;
        public DateTime ExpirationDate;
        public int RequiredPartition;
        public int Partition;

        public static ProduceMessage New(string topic, int partition, Message message, DateTime expirationDate)
        {
            return new ProduceMessage
            {
                Topic = topic,
                Partition = partition,
                RequiredPartition = partition,
                Message = message,
                ExpirationDate = expirationDate
            };
        }
    }
}
