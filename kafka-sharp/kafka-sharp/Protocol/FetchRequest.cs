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
        public int MaxBytes;
        public Basics.IsolationLevel IsolationLevel;
        public IEnumerable<TopicData<FetchPartitionData>> TopicsData;

        #region Serialization

        public ReusableMemoryStream Serialize(ReusableMemoryStream target, int correlationId, byte[] clientId, object extra, Basics.ApiVersion version)
        {
            return CommonRequest.Serialize(target, this, correlationId, clientId, Basics.ApiKey.FetchRequest, version, extra);
        }

        public void SerializeBody(ReusableMemoryStream stream, object extra, Basics.ApiVersion version)
        {
            stream.Write(Basics.MinusOne32, 0, 4); // ReplicaId, non clients that are not a broker must use -1
            BigEndianConverter.Write(stream, MaxWaitTime);
            BigEndianConverter.Write(stream, MinBytes);
            if (version >= Basics.ApiVersion.V3)
            {
                BigEndianConverter.Write(stream, MaxBytes);
            }
            if (version >= Basics.ApiVersion.V4)
            {
                stream.WriteByte((byte) IsolationLevel);
            }
            Basics.WriteArray(stream, TopicsData, extra, version);
        }

        #endregion
    }

    struct FetchPartitionData : IMemoryStreamSerializable
    {
        public int Partition;
        public int MaxBytes;
        public long FetchOffset;
        public long LogStartOffset; // Required by the protocol, but will always be zero in our case (i.e. we are consumers, not brokers)

        #region Serialization

        public void Serialize(ReusableMemoryStream stream, object _, Basics.ApiVersion version)
        {
            BigEndianConverter.Write(stream, Partition);
            BigEndianConverter.Write(stream, FetchOffset);
            BigEndianConverter.Write(stream, MaxBytes);
            if (version >= Basics.ApiVersion.V5)
            {
                stream.Write(Basics.Zero64, 0, 8); // log_start_offset is 0 for consumer, only used by follower.
            }
        }

        // Used only in tests
        public void Deserialize(ReusableMemoryStream stream, object _, Basics.ApiVersion version)
        {
            Partition = BigEndianConverter.ReadInt32(stream);
            FetchOffset = BigEndianConverter.ReadInt64(stream);
            MaxBytes = BigEndianConverter.ReadInt32(stream);
            if (version >= Basics.ApiVersion.V5)
            {
                LogStartOffset = BigEndianConverter.ReadInt64(stream);
            }
        }

        #endregion
    }
}

