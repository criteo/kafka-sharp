// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

using System;
using Kafka.Common;
using Kafka.Protocol;
using Kafka.Public;

namespace Kafka.Cluster
{
    /// <summary>
    /// Those objects are pooled to minimize stress on the GC.
    /// Use New/Release for managing lifecycle.
    /// </summary>
    class ProduceMessage
    {
        public string Topic;
        public Message Message;
        public DateTime ExpirationDate;
        public int RequiredPartition = Partitions.Any;
        public int Partition = Partitions.None;

        private ProduceMessage() { }

        public static ProduceMessage New(string topic, int partition, Message message, DateTime expirationDate)
        {
            var reserved = _produceMessagePool.Reserve();
            reserved.Topic = topic;
            reserved.Partition = reserved.RequiredPartition = partition;
            reserved.Message = message;
            reserved.ExpirationDate = expirationDate;
            return reserved;
        }

        public static void Release(ProduceMessage message)
        {
            _produceMessagePool.Release(message);
        }

        static readonly Pool<ProduceMessage> _produceMessagePool = new Pool<ProduceMessage>(() => new ProduceMessage(),
            pm =>
            {
                pm.Message = new Message();
                pm.Topic = null;
            });
    }
}
