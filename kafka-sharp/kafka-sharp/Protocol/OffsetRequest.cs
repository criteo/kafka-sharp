// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

using System;
using System.Collections.Generic;
using System.IO;
using Kafka.Common;

namespace Kafka.Protocol
{
    struct OffsetRequest
    {
        public IEnumerable<TopicData<OffsetPartitionData>> TopicsData;

        #region Serialization

        public byte[] Serialize(int correlationId, byte[] clientId)
        {
            using (var stream = new MemoryStream())
            {
                Basics.WriteRequestHeader(stream, correlationId, Basics.ApiKey.OffsetRequest, clientId);
                stream.Write(Basics.MinusOne32, 0, 4); // ReplicaId, non clients that are not a broker must use -1
                Basics.WriteArray(stream, TopicsData, SerializeTopicData);
                return Basics.WriteMessageLength(stream);
            }
        }

        // Dumb trick to minimize closure allocations
        private static readonly Action<MemoryStream, TopicData<OffsetPartitionData>> SerializeTopicData = _SerializeTopicData;

        static void _SerializeTopicData(MemoryStream s, TopicData<OffsetPartitionData> t)
        {
            t.Serialize(s);
        }

        #endregion
    }

    struct OffsetPartitionData : IMemoryStreamSerializable
    {
        public int Partition;
        public int MaxNumberOfOffsets;
        public long Time;

        #region Serialization

        public void Serialize(MemoryStream stream)
        {
            BigEndianConverter.Write(stream, Partition);
            BigEndianConverter.Write(stream, Time);
            BigEndianConverter.Write(stream, MaxNumberOfOffsets);
        }

        public void Deserialize(MemoryStream stream)
        {
            throw new NotImplementedException();
        }

        #endregion
    }
}
