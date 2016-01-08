// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

using System;
using System.Collections.Generic;
using System.IO.Compression;
using Kafka.Common;
using Kafka.Public;
using Snappy;

namespace Kafka.Protocol
{
    using Serializers = Tuple<ISerializer, ISerializer>;

    class ProduceRequest : ISerializableRequest
    {
        public IEnumerable<TopicData<PartitionData>> TopicsData;
        public int Timeout;
        public short RequiredAcks;

        #region Serialization

        public ReusableMemoryStream Serialize(int correlationId, byte[] clientId, object extra)
        {
            return CommonRequest.Serialize(this, correlationId, clientId, Basics.ApiKey.ProduceRequest, extra);
        }

        public void SerializeBody(ReusableMemoryStream stream, object extra)
        {
            BigEndianConverter.Write(stream, RequiredAcks);
            BigEndianConverter.Write(stream, Timeout);
            Basics.WriteArray(stream, TopicsData, extra);
        }

        #endregion
    }

    class PartitionData : IMemoryStreamSerializable
    {
        public IEnumerable<Message> Messages;
        public int Partition;
        public CompressionCodec CompressionCodec;

        #region Serialization

        struct SerializationInfo
        {
            public Serializers Serializers;
            public CompressionCodec CompressionCodec;
        }

        public void Serialize(ReusableMemoryStream stream, object extra)
        {
            BigEndianConverter.Write(stream, Partition);
            Basics.WriteSizeInBytes(stream, Messages,
                new SerializationInfo {Serializers = extra as Serializers, CompressionCodec = CompressionCodec},
                SerializeMessages);
        }

        private static void SerializeMessagesUncompressed(ReusableMemoryStream stream, IEnumerable<Message> messages, Serializers serializers)
        {
            foreach (var message in messages)
            {
                stream.Write(Basics.Zero64, 0, 8); // producer puts a fake offset
                Basics.WriteSizeInBytes(stream, message,
                    new SerializationInfo {CompressionCodec = CompressionCodec.None, Serializers = serializers},
                    SerializeMessageWithCodec);
            }
        }

        // Dumb trick to minimize closure allocations
        private static readonly Action<ReusableMemoryStream, IEnumerable<Message>, SerializationInfo> SerializeMessages =
            _SerializeMessages;

        // Dumb trick to minimize closure allocations
        private static readonly Action<ReusableMemoryStream, Message, SerializationInfo> SerializeMessageWithCodec =
            _SerializeMessageWithCodec;

        private static void _SerializeMessages(ReusableMemoryStream stream, IEnumerable<Message> messages, SerializationInfo info)
        {
            if (info.CompressionCodec != CompressionCodec.None)
            {
                stream.Write(Basics.Zero64, 0, 8);
                using (var msgsetStream = ReusableMemoryStream.Reserve())
                {
                    SerializeMessagesUncompressed(msgsetStream, messages, info.Serializers);

                    using (var compressed = ReusableMemoryStream.Reserve())
                    {
                        if (info.CompressionCodec == CompressionCodec.Gzip)
                        {
                            using (var gzip = new GZipStream(compressed, CompressionMode.Compress, true))
                            {
                                msgsetStream.WriteTo(gzip);
                            }
                        }
                        else // Snappy
                        {
                            compressed.SetLength(SnappyCodec.GetMaxCompressedLength((int) msgsetStream.Length));
                            {
                                int size = SnappyCodec.Compress(msgsetStream.GetBuffer(), 0, (int) msgsetStream.Length,
                                    compressed.GetBuffer(), 0);
                                compressed.SetLength(size);
                            }
                        }

                        var m = new Message
                        {
                            Value = compressed
                        };
                        Basics.WriteSizeInBytes(stream, m,
                            new SerializationInfo
                            {
                                Serializers = SerializationConfig.ByteArraySerializers,
                                CompressionCodec = info.CompressionCodec
                            }, SerializeMessageWithCodec);
                    }
                }
            }
            else
            {
                SerializeMessagesUncompressed(stream, messages, info.Serializers);
            }
        }

        private static void _SerializeMessageWithCodec(ReusableMemoryStream stream, Message message, SerializationInfo info)
        {
            message.Serialize(stream, info.CompressionCodec, info.Serializers);
        }

        public void Deserialize(ReusableMemoryStream stream, object noextra)
        {
            throw new NotImplementedException();
        }

        #endregion
    }
}

