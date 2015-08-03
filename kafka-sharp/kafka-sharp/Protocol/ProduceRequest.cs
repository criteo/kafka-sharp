// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. 
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using Kafka.Common;
using Kafka.Public;

namespace Kafka.Protocol
{
    class ProduceRequest
    {
        public IEnumerable<TopicData> TopicData;
        public int Timeout;
        public short RequiredAcks;

        #region Serialization

        public byte[] Serialize(int correlationId, byte[] clientId)
        {
            using (var stream = new MemoryStream())
            {
                Basics.WriteRequestHeader(stream, correlationId, Basics.ApiKey.ProduceRequest, clientId);
                BigEndianConverter.Write(stream, RequiredAcks);
                BigEndianConverter.Write(stream, Timeout);
                Basics.WriteArray(stream, TopicData, SerializeTopicData);
                return Basics.WriteMessageLength(stream);
            }
        }

        // Dumb trick to minimize closure allocations
        private static Action<MemoryStream, TopicData> SerializeTopicData = _SerializeTopicData;

        static void _SerializeTopicData(MemoryStream s, TopicData t)
        {
            t.Serialize(s);
        }

        #endregion
    }

    class TopicData
    {
        public string TopicName;
        public IEnumerable<PartitionData> PartitionsData;

        #region Serialization

        public void Serialize(MemoryStream stream)
        {
            Basics.SerializeString(stream, TopicName);
            Basics.WriteArray(stream, PartitionsData, SerializePartitionData);
        }

        // Dumb trick to minimize closure allocations
        private static Action<MemoryStream, PartitionData> SerializePartitionData = _SerializePartitionData;

        static void _SerializePartitionData(MemoryStream s, PartitionData p)
        {
            p.Serialize(s);
        }

        #endregion
    }

    class PartitionData
    {
        public IEnumerable<Message> Messages;
        public int Partition;
        public CompressionCodec CompressionCodec;

        #region Serialization

        public void Serialize(MemoryStream stream)
        {
            BigEndianConverter.Write(stream, Partition);
            Basics.WriteSizeInBytes(stream, Messages, CompressionCodec, SerializeMessages);
        }

        private static void SerializeMessagesUncompressed(MemoryStream stream, IEnumerable<Message> messages)
        {
            foreach (var message in messages)
            {
                stream.Write(Basics.Zero64, 0, 8); // producer does fake offset
                Basics.WriteSizeInBytes(stream, message, CompressionCodec.None, SerializeMessageWithCodec);
            }
        }

        // Dumb trick to minimize closure allocations
        private static Action<MemoryStream, IEnumerable<Message>, CompressionCodec> SerializeMessages =
            _SerializeMessages;

        // Dumb trick to minimize closure allocations
        private static Action<MemoryStream, Message, CompressionCodec> SerializeMessageWithCodec =
            _SerializeMessageWithCodec;

        private static void _SerializeMessages(MemoryStream stream, IEnumerable<Message> messages, CompressionCodec compressionCodec)
        {
            if (compressionCodec != CompressionCodec.None)
            {
                using (var msgsetStream = new MemoryStream())
                {
                    SerializeMessagesUncompressed(msgsetStream, messages);
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
                    Basics.WriteSizeInBytes(stream, m, compressionCodec, SerializeMessageWithCodec);
                }
            }
            else
            {
                SerializeMessagesUncompressed(stream, messages);
            }
        }

        private static void _SerializeMessageWithCodec(MemoryStream stream, Message message, CompressionCodec codec)
        {
            message.Serialize(stream, codec);
        }

        #endregion
    }
}

