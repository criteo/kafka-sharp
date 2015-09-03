// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

using Kafka.Common;

namespace Kafka.Protocol
{
    struct ProducePartitionResponse : IMemoryStreamSerializable
    {
        public int Partition;
        public ErrorCode ErrorCode;
        public long Offset;

        // Used only in tests
        public void Serialize(ReusableMemoryStream stream, object noextra = null)
        {
            BigEndianConverter.Write(stream, Partition);
            BigEndianConverter.Write(stream, (short) ErrorCode);
            BigEndianConverter.Write(stream, Offset);
        }

        public void Deserialize(ReusableMemoryStream stream, object noextra = null)
        {
            Partition = BigEndianConverter.ReadInt32(stream);
            ErrorCode = (ErrorCode) BigEndianConverter.ReadInt16(stream);
            Offset = BigEndianConverter.ReadInt64(stream);
        }
    }
}
