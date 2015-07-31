// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. 
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

using System.IO;
using Kafka.Common;

namespace Kafka.Protocol
{
    internal class ProduceResponse
    {
        public TopicResponse[] TopicsResponse;

        public static ProduceResponse Deserialize(byte[] body)
        {
            using (var stream = new MemoryStream(body))
            {
                var resp = new ProduceResponse();
                var count = BigEndianConverter.ReadInt32(stream);
                resp.TopicsResponse = new TopicResponse[count];
                for (int i = 0; i < count; i++)
                    resp.TopicsResponse[i] = TopicResponse.Deserialize(stream);

                return resp;
            }
        }
    }

    class TopicResponse
    {
        public string TopicName;
        public PartitionResponse[] Partitions;

        public static TopicResponse Deserialize(MemoryStream stream)
        {
            var resp = new TopicResponse();
            resp.TopicName = Basics.DeserializeString(stream);
            var count = BigEndianConverter.ReadInt32(stream);
            resp.Partitions = new PartitionResponse[count];
            for (int i = 0; i < count; i++)
                resp.Partitions[i] = PartitionResponse.Deserialize(stream);
            return resp;
        }
    }

    class PartitionResponse
    {
        public int Partition;
        public ErrorCode ErrorCode;
        public long Offset;

        public static PartitionResponse Deserialize(MemoryStream stream)
        {
            return new PartitionResponse
                {
                    Partition = BigEndianConverter.ReadInt32(stream),
                    ErrorCode = (ErrorCode) BigEndianConverter.ReadInt16(stream),
                    Offset = BigEndianConverter.ReadInt64(stream)
                };
        }
    }
}
