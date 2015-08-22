// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

using System.Collections.Generic;
using Kafka.Common;

namespace Kafka.Protocol
{
    struct FetchRequest : ISerializableRequest
    {
        public int MaxWaitTime;
        public int MinBytes;
        public IEnumerable<TopicData<FetchPartitionData>> TopicsData;

        #region Serialization

        public ReusableMemoryStream Serialize(int correlationId, byte[] clientId)
        {
            return CommonRequest.Serialize(this, correlationId, clientId, Basics.ApiKey.FetchRequest);
        }

        public void SerializeBody(ReusableMemoryStream stream)
        {
            stream.Write(Basics.MinusOne32, 0, 4); // ReplicaId, non clients that are not a broker must use -1
            BigEndianConverter.Write(stream, MaxWaitTime);
            BigEndianConverter.Write(stream, MinBytes);
            Basics.WriteArray(stream, TopicsData);
        }

        #endregion
    }

    struct FetchPartitionData : IMemoryStreamSerializable
    {
        public int Partition;
        public int MaxBytes;
        public long FetchOffset;

        #region Serialization

        public void Serialize(ReusableMemoryStream stream)
        {
            BigEndianConverter.Write(stream, Partition);
            BigEndianConverter.Write(stream, FetchOffset);
            BigEndianConverter.Write(stream, MaxBytes);
        }

        // Used only in tests
        public void Deserialize(ReusableMemoryStream stream)
        {
            Partition = BigEndianConverter.ReadInt32(stream);
            FetchOffset = BigEndianConverter.ReadInt64(stream);
            MaxBytes = BigEndianConverter.ReadInt32(stream);
        }

        #endregion
    }
}

