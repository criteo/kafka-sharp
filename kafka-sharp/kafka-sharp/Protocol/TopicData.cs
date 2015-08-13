// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

using System;
using System.Collections.Generic;
using System.IO;
using Kafka.Common;

namespace Kafka.Protocol
{
    struct TopicData<TPartitionData> : IMemoryStreamSerializable where TPartitionData : IMemoryStreamSerializable, new()
    {
        public string TopicName;
        public IEnumerable<TPartitionData> PartitionsData;

        #region Serialization

        public void Serialize(MemoryStream stream)
        {
            Basics.SerializeString(stream, TopicName);
            Basics.WriteArray(stream, PartitionsData, SerializePartitionData);
        }

        // Dumb trick to minimize closure allocations
        private static Action<MemoryStream, TPartitionData> SerializePartitionData = _SerializePartitionData;

        static void _SerializePartitionData(MemoryStream s, TPartitionData p)
        {
            p.Serialize(s);
        }

        public void Deserialize(MemoryStream stream)
        {
            TopicName = Basics.DeserializeString(stream);
            var count = BigEndianConverter.ReadInt32(stream);
            var array = new TPartitionData[count];
            for (int i = 0; i < count; ++i)
            {
                array[i] = new TPartitionData();
                array[i].Deserialize(stream);
            }
            PartitionsData = array;
        }

        #endregion
    }
}