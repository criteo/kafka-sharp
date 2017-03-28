// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

using Kafka.Common;

namespace Kafka.Protocol
{
    struct ProduceResponse : IMemoryStreamSerializable
    {
        public int ThrottleTime;
        public CommonResponse<ProducePartitionResponse> ProducePartitionResponse;

        public void Serialize(ReusableMemoryStream stream, object extra, Basics.ApiVersion version)
        {
            ProducePartitionResponse.Serialize(stream, extra, version);
            if (version > Basics.ApiVersion.V0)
            {
                BigEndianConverter.Write(stream, ThrottleTime);
            }
        }

        public void Deserialize(ReusableMemoryStream stream, object extra, Basics.ApiVersion version)
        {
            ProducePartitionResponse.Deserialize(stream, extra, version);
            if (version > Basics.ApiVersion.V0)
            {
                ThrottleTime = BigEndianConverter.ReadInt32(stream);
            }
        }
    }

    struct ProducePartitionResponse : IMemoryStreamSerializable
    {
        public int Partition;
        public ErrorCode ErrorCode;
        public long Offset;
        public long Timestamp;

        // Used only in tests
        public void Serialize(ReusableMemoryStream stream, object _, Basics.ApiVersion version)
        {
            BigEndianConverter.Write(stream, Partition);
            BigEndianConverter.Write(stream, (short) ErrorCode);
            BigEndianConverter.Write(stream, Offset);
            if (version >= Basics.ApiVersion.V2)
            {
                BigEndianConverter.Write(stream, Timestamp);
            }
        }

        public void Deserialize(ReusableMemoryStream stream, object _, Basics.ApiVersion version)
        {
            Partition = BigEndianConverter.ReadInt32(stream);
            ErrorCode = (ErrorCode) BigEndianConverter.ReadInt16(stream);
            Offset = BigEndianConverter.ReadInt64(stream);
            if (version >= Basics.ApiVersion.V2)
            {
                Timestamp = BigEndianConverter.ReadInt64(stream);
            }
        }
    }
}
