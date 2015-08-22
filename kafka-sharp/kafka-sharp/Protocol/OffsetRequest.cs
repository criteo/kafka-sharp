// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

using System.Collections.Generic;
using Kafka.Common;

namespace Kafka.Protocol
{
    struct OffsetRequest : ISerializableRequest
    {
        public IEnumerable<TopicData<OffsetPartitionData>> TopicsData;

        #region Serialization

        public ReusableMemoryStream Serialize(int correlationId, byte[] clientId)
        {
            return CommonRequest.Serialize(this, correlationId, clientId, Basics.ApiKey.OffsetRequest);
        }

        public void SerializeBody(ReusableMemoryStream stream)
        {
            stream.Write(Basics.MinusOne32, 0, 4); // ReplicaId, non clients that are not a broker must use -1
            Basics.WriteArray(stream, TopicsData);
        }

        #endregion
    }

    struct OffsetPartitionData : IMemoryStreamSerializable
    {
        public int Partition;
        public int MaxNumberOfOffsets;
        public long Time;

        #region Serialization

        public void Serialize(ReusableMemoryStream stream)
        {
            BigEndianConverter.Write(stream, Partition);
            BigEndianConverter.Write(stream, Time);
            BigEndianConverter.Write(stream, MaxNumberOfOffsets);
        }

        public void Deserialize(ReusableMemoryStream stream)
        {
            Partition = BigEndianConverter.ReadInt32(stream);
            Time = BigEndianConverter.ReadInt64(stream);
            MaxNumberOfOffsets = BigEndianConverter.ReadInt32(stream);
        }

        #endregion
    }
}
