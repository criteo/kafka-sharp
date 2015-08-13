// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

using System;
using System.Collections.Generic;
using System.IO;
using Kafka.Common;

namespace Kafka.Protocol
{
    struct FetchRequest
    {
        public int MaxWaitTime;
        public int MinBytes;
        public IEnumerable<TopicData<FetchPartitionData>> TopicsData;

        #region Serialization

        public byte[] Serialize(int correlationId, byte[] clientId)
        {
            using (var stream = new MemoryStream())
            {
                Basics.WriteRequestHeader(stream, correlationId, Basics.ApiKey.FetchRequest, clientId);
                stream.Write(Basics.MinusOne32, 0, 4); // ReplicaId, non clients that are not a broker must use -1
                BigEndianConverter.Write(stream, MaxWaitTime);
                BigEndianConverter.Write(stream, MinBytes);
                Basics.WriteArray(stream, TopicsData, SerializeTopicData);
                return Basics.WriteMessageLength(stream);
            }
        }

        // Dumb trick to minimize closure allocations
        private static readonly Action<MemoryStream, TopicData<FetchPartitionData>> SerializeTopicData = _SerializeTopicData;

        static void _SerializeTopicData(MemoryStream s, TopicData<FetchPartitionData> t)
        {
            t.Serialize(s);
        }

        #endregion
    }

    struct FetchPartitionData : IMemoryStreamSerializable
    {
        public int Partition;
        public int MaxBytes;
        public long FetchOffset;

        #region Serialization

        public void Serialize(MemoryStream stream)
        {
            BigEndianConverter.Write(stream, Partition);
            BigEndianConverter.Write(stream, FetchOffset);
            BigEndianConverter.Write(stream, MaxBytes);
        }

        // Used only in tests
        public void Deserialize(MemoryStream stream)
        {
            Partition = BigEndianConverter.ReadInt32(stream);
            MaxBytes = BigEndianConverter.ReadInt32(stream);
            FetchOffset = BigEndianConverter.ReadInt64(stream);
        }

        #endregion
    }
}

