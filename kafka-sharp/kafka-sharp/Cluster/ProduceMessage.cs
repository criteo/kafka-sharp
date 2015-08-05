// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. 
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

using System;
using System.Collections.Concurrent;
using Kafka.Protocol;

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
        public int Partition;

        public static readonly int PartitionNotSet = -1;

        private ProduceMessage() { }

        public static ProduceMessage New(string topic, Message message, DateTime expirationDate)
        {
            return New(topic, PartitionNotSet, message, expirationDate);
        }

        public static ProduceMessage New(string topic, int partition, Message message, DateTime expirationDate)
        {
            ProduceMessage reserved;
            if (!_produceMessagePool.TryDequeue(out reserved))
            {
                reserved = new ProduceMessage();
            }
            reserved.Topic = topic;
            reserved.Partition = partition;
            reserved.Message = message;
            reserved.ExpirationDate = expirationDate;
            return reserved;
        }

        public static void Release(ProduceMessage message)
        {
            _produceMessagePool.Enqueue(message);
        }

        static readonly ConcurrentQueue<ProduceMessage> _produceMessagePool = new ConcurrentQueue<ProduceMessage>();
    }
}
