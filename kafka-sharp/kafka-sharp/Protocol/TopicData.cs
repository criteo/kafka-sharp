// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

using System.Collections.Generic;
using Kafka.Common;
using Kafka.Public;

namespace Kafka.Protocol
{
    struct TopicData<TPartitionData> : IMemoryStreamSerializable where TPartitionData : IMemoryStreamSerializable, new()
    {
        public string TopicName;
        public IEnumerable<TPartitionData> PartitionsData;

        #region Serialization

        public void Serialize(ReusableMemoryStream stream, object extra, Basics.ApiVersion version)
        {
            Basics.SerializeString(stream, TopicName);
            object pdExtra = null;
            if (extra != null)
            {
                var config = extra as SerializationConfig;
                pdExtra = config.GetSerializersForTopic(TopicName);
            }
            Basics.WriteArray(stream, PartitionsData, pdExtra, version);
        }

        public void Deserialize(ReusableMemoryStream stream, object extra, Basics.ApiVersion version)
        {
            TopicName = Basics.DeserializeString(stream);
            var count = BigEndianConverter.ReadInt32(stream);
            var array = new TPartitionData[count];
            object pdExtra = null;
            if (extra != null)
            {
                var config = extra as SerializationConfig;
                pdExtra = config.GetDeserializersForTopic(TopicName);
            }
            for (int i = 0; i < count; ++i)
            {
                array[i] = new TPartitionData();
                array[i].Deserialize(stream, pdExtra, version);
            }
            PartitionsData = array;
        }

        #endregion
    }
}