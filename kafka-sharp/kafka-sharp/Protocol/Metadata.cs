// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

using Kafka.Common;

namespace Kafka.Protocol
{
    class BrokerMeta : IMemoryStreamSerializable
    {
        public string Host;
        public int Port;
        public int Id;

        public override string ToString()
        {
            return string.Format("[Id:{0} Host:{1} Port:{2}]", Id, Host, Port);
        }

        public void Deserialize(ReusableMemoryStream stream, object noextra = null)
        {
            Id = BigEndianConverter.ReadInt32(stream);
            Host = Basics.DeserializeString(stream);
            Port = BigEndianConverter.ReadInt32(stream);
        }

        // Used only in tests
        public void Serialize(ReusableMemoryStream stream, object noextra = null)
        {
            BigEndianConverter.Write(stream, Id);
            Basics.SerializeString(stream, Host);
            BigEndianConverter.Write(stream, Port);
        }
    }

    class TopicMeta : IMemoryStreamSerializable
    {
        public ErrorCode ErrorCode;
        public string TopicName;
        public PartitionMeta[] Partitions;

        public void Deserialize(ReusableMemoryStream stream, object noextra = null)
        {
            ErrorCode = (ErrorCode) BigEndianConverter.ReadInt16(stream);
            TopicName = Basics.DeserializeString(stream);
            Partitions = Basics.DeserializeArray<PartitionMeta>(stream);
        }

        // Used only in tests
        public void Serialize(ReusableMemoryStream stream, object noextra = null)
        {
            BigEndianConverter.Write(stream, (short) ErrorCode);
            Basics.SerializeString(stream, TopicName);
            Basics.WriteArray(stream, Partitions);
        }
    }

    class PartitionMeta : IMemoryStreamSerializable
    {
        public ErrorCode ErrorCode;
        public int Id;
        public int Leader;
        public int[] Replicas;
        public int[] Isr;

        public void Deserialize(ReusableMemoryStream stream, object noextra = null)
        {
            ErrorCode = (ErrorCode) BigEndianConverter.ReadInt16(stream);
            Id = BigEndianConverter.ReadInt32(stream);
            Leader = BigEndianConverter.ReadInt32(stream);
            Replicas = Basics.DeserializeArray(stream, BigEndianConverter.ReadInt32);
            Isr = Basics.DeserializeArray(stream, BigEndianConverter.ReadInt32);
        }

        // Used only in tests
        public void Serialize(ReusableMemoryStream stream, object noextra = null)
        {
            BigEndianConverter.Write(stream, (short) ErrorCode);
            BigEndianConverter.Write(stream, Id);
            BigEndianConverter.Write(stream, Leader);
            Basics.WriteArray(stream, Replicas, BigEndianConverter.Write);
            Basics.WriteArray(stream, Isr, BigEndianConverter.Write);
        }
    }


}
