// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. 
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

using Kafka.Protocol;

namespace Kafka.Routing
{
    interface IPartitioner
    {
        Partition GetPartition(Message message, Partition[] partitions);
    }

    class DefaultPartitioner : IPartitioner
    {
        private ulong _next;

        public Partition GetPartition(Message dummy, Partition[] partitions)
        {
            return partitions[(int)(_next++)%partitions.Length];
        }
    }
}
