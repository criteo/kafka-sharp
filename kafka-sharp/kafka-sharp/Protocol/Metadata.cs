// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. 
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

using System.IO;
using Kafka.Common;

namespace Kafka.Protocol
{
    class BrokerMeta
    {
        public readonly static int Unknown = -42;

        public int Id = Unknown;
        public string Host;
        public int Port;

        public override string ToString()
        {
            return string.Format("[Id:{0} Host:{1} Port:{2}]", Id, Host, Port);
        }

        public static BrokerMeta Deserialize(MemoryStream stream)
        {
            return new BrokerMeta
            {
                Id = BigEndianConverter.ReadInt32(stream),
                Host = Basics.DeserializeString(stream),
                Port = BigEndianConverter.ReadInt32(stream)
            };
        }
    }

    class TopicMeta
    {
        public ErrorCode ErrorCode;
        public string TopicName;
        public PartitionMeta[] Partitions;

        public static TopicMeta Deserialize(MemoryStream stream)
        {
            var ret = new TopicMeta
                {
                    ErrorCode = (ErrorCode) BigEndianConverter.ReadInt16(stream),
                    TopicName = Basics.DeserializeString(stream)
                };

            var count = BigEndianConverter.ReadInt32(stream);
            ret.Partitions = new PartitionMeta[count];
            for (int i = 0; i < count; i++)
                ret.Partitions[i] = PartitionMeta.Deserialize(stream);

            return ret;
        }
    }

    class PartitionMeta
    {
        public ErrorCode ErrorCode;
        public int Id;
        public int Leader;
        public int[] Replicas;
        public int[] Isr;

        public static PartitionMeta Deserialize(MemoryStream stream)
        {
            var ret = new PartitionMeta
                {
                    ErrorCode = (ErrorCode) BigEndianConverter.ReadInt16(stream),
                    Id = BigEndianConverter.ReadInt32(stream),
                    Leader = BigEndianConverter.ReadInt32(stream)
                };

            var count = BigEndianConverter.ReadInt32(stream);
            ret.Replicas = new int[count];
            for (int i = 0; i < count; i++)
                ret.Replicas[i] = BigEndianConverter.ReadInt32(stream);

            count = BigEndianConverter.ReadInt32(stream);
            ret.Isr = new int[count];
            for (int i = 0; i < count; i++)
                ret.Isr[i] = BigEndianConverter.ReadInt32(stream);

            return ret;
        }
    }


}
