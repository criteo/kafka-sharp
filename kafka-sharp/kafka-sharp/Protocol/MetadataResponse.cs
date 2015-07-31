// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. 
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

using System.IO;
using Kafka.Common;

namespace Kafka.Protocol
{
    class MetadataResponse
    {
        public BrokerMeta[] BrokersMeta;
        public TopicMeta[] TopicsMeta;
        
        public static MetadataResponse Deserialize(byte[] body)
        {
            using (var stream = new MemoryStream(body))
            {
                var ret = new MetadataResponse();

                var count = BigEndianConverter.ReadInt32(stream);
                ret.BrokersMeta = new BrokerMeta[count];
                for (int i = 0; i < count; i++)
                    ret.BrokersMeta[i] = BrokerMeta.Deserialize(stream);

                count = BigEndianConverter.ReadInt32(stream);
                ret.TopicsMeta = new TopicMeta[count];
                for (int i = 0; i < count; i++)
                    ret.TopicsMeta[i] = TopicMeta.Deserialize(stream);

                return ret;
            }
        }
    }
}
