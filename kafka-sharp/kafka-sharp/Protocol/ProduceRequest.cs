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
    class ProduceRequest : ISerializableRequest
    {
        public IEnumerable<TopicData<PartitionData>> TopicsData;
        public int Timeout;
        public short RequiredAcks;

        #region Serialization

        public ReusableMemoryStream Serialize(int correlationId, byte[] clientId)
        {
            return CommonRequest.Serialize(this, correlationId, clientId, Basics.ApiKey.ProduceRequest);
        }

        public void SerializeBody(ReusableMemoryStream stream)
        {
            BigEndianConverter.Write(stream, RequiredAcks);
            BigEndianConverter.Write(stream, Timeout);
            Basics.WriteArray(stream, TopicsData);
        }

        #endregion
    }

    class PartitionData : IMemoryStreamSerializable
    {
        public IEnumerable<Message> Messages;
        public int Partition;
        public CompressionCodec CompressionCodec;

        #region Serialization

        public void Serialize(ReusableMemoryStream stream)
        {
            BigEndianConverter.Write(stream, Partition);
            Basics.WriteSizeInBytes(stream, Messages, CompressionCodec, SerializeMessages);
        }

        private static void SerializeMessagesUncompressed(ReusableMemoryStream stream, IEnumerable<Message> messages)
        {
            foreach (var message in messages)
            {
                stream.Write(Basics.Zero64, 0, 8); // producer does fake offset
                Basics.WriteSizeInBytes(stream, message, CompressionCodec.None, SerializeMessageWithCodec);
            }
        }

        // Dumb trick to minimize closure allocations
        private static readonly Action<ReusableMemoryStream, IEnumerable<Message>, CompressionCodec> SerializeMessages =
            _SerializeMessages;

        // Dumb trick to minimize closure allocations
        private static readonly Action<ReusableMemoryStream, Message, CompressionCodec> SerializeMessageWithCodec =
            _SerializeMessageWithCodec;

        private static void _SerializeMessages(ReusableMemoryStream stream, IEnumerable<Message> messages, CompressionCodec compressionCodec)
        {
            if (compressionCodec != CompressionCodec.None)
            {
                stream.Write(Basics.Zero64, 0, 8);
                using (var msgsetStream = ReusableMemoryStream.Reserve())
                {
                    SerializeMessagesUncompressed(msgsetStream, messages);

                    using (var compressed = ReusableMemoryStream.Reserve())
                    {
                        if (compressionCodec == CompressionCodec.Gzip)
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
                            Value = compressed.GetBuffer(),
                            ValueSize = (int) compressed.Length
                        };
                        Basics.WriteSizeInBytes(stream, m, compressionCodec, SerializeMessageWithCodec);
                    }
                }
            }
            else
            {
                SerializeMessagesUncompressed(stream, messages);
            }
        }

        private static void _SerializeMessageWithCodec(ReusableMemoryStream stream, Message message, CompressionCodec codec)
        {
            message.Serialize(stream, codec);
        }

        public void Deserialize(ReusableMemoryStream stream)
        {
            throw new NotImplementedException();
        }

        #endregion
    }
}

