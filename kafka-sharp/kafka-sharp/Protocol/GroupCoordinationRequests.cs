// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

using System.Collections.Generic;
using Kafka.Common;
namespace Kafka.Protocol
{
    #region GroupCoordinator

    class GroupCoordinatorRequest : ISerializableRequest
    {
        public string GroupId;

        public ReusableMemoryStream Serialize(ReusableMemoryStream target, int correlationId, byte[] clientId,
            object extra)
        {
            return CommonRequest.Serialize(target, this, correlationId, clientId, Basics.ApiKey.GroupCoordinatorRequest,
                Basics.ApiVersion.V0, null);
        }

        public void SerializeBody(ReusableMemoryStream stream, object extra)
        {
            Basics.SerializeString(stream, GroupId);
        }
    }

    #endregion

    #region OffsetCommit

    struct OffsetCommitPartitionData : IMemoryStreamSerializable
    {
        public int Partition;
        public long Offset;
        public string Metadata;

        public void Serialize(ReusableMemoryStream stream, object extra)
        {
            BigEndianConverter.Write(stream, Partition);
            BigEndianConverter.Write(stream, Offset);
            Basics.SerializeString(stream, Metadata);
        }

        public void Deserialize(ReusableMemoryStream stream, object extra)
        {
            Partition = BigEndianConverter.ReadInt32(stream);
            Offset = BigEndianConverter.ReadInt64(stream);
            Metadata = Basics.DeserializeString(stream);
        }
    }

    class OffsetCommitRequest : ISerializableRequest
    {
        public string ConsumerGroupId;
        public int ConsumerGroupGenerationId;
        public string ConsumerId;
        public long RetentionTime;
        public IEnumerable<TopicData<OffsetCommitPartitionData>> TopicsData;

        public ReusableMemoryStream Serialize(ReusableMemoryStream target, int correlationId, byte[] clientId, object extra)
        {
            return CommonRequest.Serialize(target, this, correlationId, clientId, Basics.ApiKey.OffsetCommitRequest,
                Basics.ApiVersion.V2, null);
        }

        public void SerializeBody(ReusableMemoryStream stream, object extra)
        {
            Basics.SerializeString(stream, ConsumerGroupId);
            BigEndianConverter.Write(stream, ConsumerGroupGenerationId);
            Basics.SerializeString(stream, ConsumerId);
            BigEndianConverter.Write(stream, RetentionTime);
            Basics.WriteArray(stream, TopicsData);
        }
    }

    class OffsetFetchRequest : ISerializableRequest
    {
        public string ConsumerGroupId;
        public IEnumerable<TopicData<PartitionAssignment>> TopicsData;

        public ReusableMemoryStream Serialize(ReusableMemoryStream target, int correlationId, byte[] clientId, object extra)
        {
            return CommonRequest.Serialize(target, this, correlationId, clientId, Basics.ApiKey.OffsetFetchRequest,
                Basics.ApiVersion.V1, null);
        }

        public void SerializeBody(ReusableMemoryStream stream, object extra)
        {
            Basics.SerializeString(stream, ConsumerGroupId);
            Basics.WriteArray(stream, TopicsData);
        }
    }

    #endregion
}
