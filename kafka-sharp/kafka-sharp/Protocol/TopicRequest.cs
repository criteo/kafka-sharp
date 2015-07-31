// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. 
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

using System.IO;
using Kafka.Common;

namespace Kafka.Protocol
{
    class TopicRequest
    {
        public string[] Topics;

        public byte[] Serialize(int correlationId, byte[] clientId)
        {
            using (var stream = new MemoryStream())
            {
                Basics.WriteRequestHeader(stream, correlationId, Basics.ApiKey.MetadataRequest, clientId);
                if (Topics == null || Topics.Length == 0)
                {
                    stream.Write(Basics.Zero32, 0, 4);
                }
                else
                {
                    BigEndianConverter.Write(stream, Topics.Length);
                    foreach (var t in Topics)
                        Basics.SerializeString(stream, t);
                }

                stream.Close();

                return Basics.WriteMessageLength(stream);
            }
        }
    }
}