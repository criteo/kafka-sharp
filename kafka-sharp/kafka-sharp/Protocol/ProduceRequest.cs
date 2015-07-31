// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. 
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using Kafka.Common;
using Kafka.Public;

namespace Kafka.Protocol
{
    internal class ProduceRequest
    {
        public short RequiredAcks;
        public int Timeout;
        public CompressionCodec CompressionCodec;
        public IEnumerable<TopicData> TopicData;

        public byte[] Serialize(int correlationId, byte[] clientId)
        {
            using (var stream = new MemoryStream())
            {
                Basics.WriteRequestHeader(stream, correlationId, Basics.ApiKey.ProduceRequest, clientId);
                BigEndianConverter.Write(stream, RequiredAcks);
                BigEndianConverter.Write(stream, Timeout);
                Basics.WriteArray(stream, TopicData, t => t.Serialize(stream, CompressionCodec));
                return Basics.WriteMessageLength(stream);
            }
        }
    }

    class TopicData
    {
        public string TopicName;
        public IEnumerable<PartitionData> PartitionsData;

        public void Serialize(MemoryStream stream, CompressionCodec compressionCodec)
        {
            Basics.SerializeString(stream, TopicName);
            Basics.WriteArray(stream, PartitionsData, p => p.Serialize(stream, compressionCodec));
        }
    }

    class PartitionData
    {
        public int Partition;
        public IEnumerable<Message> Messages;

        public void Serialize(MemoryStream stream, CompressionCodec compressionCodec)
        {
            BigEndianConverter.Write(stream, Partition);
            Basics.WriteSizeInBytes(stream, () =>
            {
                if (compressionCodec != CompressionCodec.None)
                {
                    using (var msgsetStream = new MemoryStream())
                    {
                        foreach (var message in Messages)
                        {
                            msgsetStream.Write(Basics.Zero64, 0, 8); // producer does fake offset
                            Basics.WriteSizeInBytes(msgsetStream,
                                                    () => message.Serialize(msgsetStream, CompressionCodec.None));
                        }
                        byte[] buffer;
                        if (compressionCodec == CompressionCodec.Gzip)
                        {
                            using (var compressed = new MemoryStream())
                            {
                                using (var gzip = new GZipStream(compressed, CompressionMode.Compress, true))
                                {
                                    msgsetStream.CopyTo(gzip);
                                }
                                buffer = compressed.ToArray();
                            }
                        }
                        else // Snappy
                        {
                            buffer = Snappy.SnappyCodec.Compress(msgsetStream.ToArray());
                        }
                        var m = new Message
                            {
                                Key = null,
                                Value = buffer
                            };
                        stream.Write(Basics.Zero64, 0, 8);
                        Basics.WriteSizeInBytes(stream, () => m.Serialize(stream, compressionCodec));
                    }
                }
                else
                {
                    foreach (var message in Messages)
                    {
                        stream.Write(Basics.Zero64, 0, 8); // producer does fake offset
                        var m = message;
                        Basics.WriteSizeInBytes(stream, () => m.Serialize(stream, CompressionCodec.None));
                    }
                }
            });
        }
    }
}

