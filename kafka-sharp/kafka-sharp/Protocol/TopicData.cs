// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

using System.Collections.Generic;
using Kafka.Common;

namespace Kafka.Protocol
{
    struct TopicData<TPartitionData> : IMemoryStreamSerializable where TPartitionData : IMemoryStreamSerializable, new()
    {
        public string TopicName;
        public IEnumerable<TPartitionData> PartitionsData;

        #region Serialization

        public void Serialize(ReusableMemoryStream stream)
        {
            Basics.SerializeString(stream, TopicName);
            Basics.WriteArray(stream, PartitionsData);
        }

        public void Deserialize(ReusableMemoryStream stream)
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