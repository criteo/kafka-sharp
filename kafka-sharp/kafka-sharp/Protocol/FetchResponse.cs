// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using Kafka.Common;
using Kafka.Public;
using LZ4;
#if !NET_CORE
using Snappy;
#endif
namespace Kafka.Protocol
{
    using Deserializers = Tuple<IDeserializer, IDeserializer>;
    using Serializers = Tuple<ISerializer, ISerializer>;

    struct ResponseMessage
    {
        public long Offset;
        public Message Message;
    }

    static class ResponseMessageListPool
    {
        private static readonly Pool<List<ResponseMessage>> _pool =
            new Pool<List<ResponseMessage>>(() => new List<ResponseMessage>(), (l, _) => l.Clear());

        public static List<ResponseMessage> Reserve()
        {
            return _pool.Reserve();
        }

        public static void Release(List<ResponseMessage> list)
        {
            if (list != EmptyList)
            {
                _pool.Release(list);
            }
        }

        public static readonly List<ResponseMessage> EmptyList = new List<ResponseMessage>();
    }

    struct FetchResponse : IMemoryStreamSerializable
    {
        public CommonResponse<FetchPartitionResponse> FetchPartitionResponse;
        public int ThrottleTime;

        public void Serialize(ReusableMemoryStream stream, object extra, Basics.ApiVersion version)
        {
            if (version > Basics.ApiVersion.V0)
            {
                BigEndianConverter.Write(stream, ThrottleTime);
            }
            FetchPartitionResponse.Serialize(stream, extra, version);
        }

        public void Deserialize(ReusableMemoryStream stream, object extra, Basics.ApiVersion version)
        {
            if (version > Basics.ApiVersion.V0)
            {
                ThrottleTime = BigEndianConverter.ReadInt32(stream);
            }
            FetchPartitionResponse.Deserialize(stream, extra, version);
        }
    }

    struct FetchPartitionResponse : IMemoryStreamSerializable
    {
        public long HighWatermarkOffset;
        public List<ResponseMessage> Messages;
        public int Partition;
        public ErrorCode ErrorCode;

        #region Deserialization

        public void Deserialize(ReusableMemoryStream stream, object extra, Basics.ApiVersion version)
        {
            Partition = BigEndianConverter.ReadInt32(stream);
            ErrorCode = (ErrorCode) BigEndianConverter.ReadInt16(stream);
            HighWatermarkOffset = BigEndianConverter.ReadInt64(stream);
            try
            {
                Messages = DeserializeMessageSet(stream, extra as Deserializers);
            }
            catch (ProtocolException pEx)
            {
                pEx.Partition = Partition;
                throw;
            }
        }

        internal static List<ResponseMessage> DeserializeMessageSet(ReusableMemoryStream stream, Deserializers deserializers)
        {
            var list = ResponseMessageListPool.Reserve();
            list.AddRange(LazyDeserializeMessageSet(stream, BigEndianConverter.ReadInt32(stream), deserializers));
            return list;
        }

        // Deserialize a message set to a sequence of messages.
        // This handles the "partial message allowed at end of message set" from Kafka brokers
        // and compressed message sets (the method recursively calls itself in this case and
        // flatten the result). The returned enumeration must be enumerated for deserialization
        // effectiveley occuring.
        //
        // A message set can contain a mix of v0 and v1 messages.
        // In the case of compressed messages, offsets are returned differently by brokers.
        // Messages inside compressed message v0 will have absolute offsets assigned.
        // Messages inside compressed message v1 will have relative offset assigned, starting
        // from 0. The wrapping compressed message itself is assigned the absolute offset of the last
        // message in the set. That means in this case we can only assign offsets after having decompressing
        // all messages. Lazy deserialization won't be so lazy anymore...
        private static IEnumerable<ResponseMessage> LazyDeserializeMessageSet(ReusableMemoryStream stream, int messageSetSize, Deserializers deserializers)
        {
            var remainingMessageSetBytes = messageSetSize;

            while (remainingMessageSetBytes > 0)
            {
                const int offsetSize = 8;
                const int msgsizeSize = 4;
                if (remainingMessageSetBytes < offsetSize + msgsizeSize)
                {
                    // This is a partial message => skip to the end of the message set.
                    // TODO: unit test this
                    stream.Position += remainingMessageSetBytes;
                    yield break;
                }

                var offset = BigEndianConverter.ReadInt64(stream);
                var messageSize = BigEndianConverter.ReadInt32(stream);

                remainingMessageSetBytes -= offsetSize + msgsizeSize;
                if (remainingMessageSetBytes < messageSize)
                {
                    // This is a partial message => skip to the end of the message set.
                    stream.Position += remainingMessageSetBytes;
                    yield break;
                }

                // Message body
                var crc = BigEndianConverter.ReadInt32(stream);
                var crcStartPos = stream.Position; // crc is computed from this position
                var magic = stream.ReadByte();
                if ((uint)magic > 1)
                {
                    throw new UnsupportedMagicByteVersion((byte) magic);
                }
                var attributes = stream.ReadByte();
                long timestamp = 0;
                if (magic == 1)
                {
                    timestamp = BigEndianConverter.ReadInt64(stream);
                }

                // Check for compression
                var codec = (CompressionCodec)(attributes & 3); // Lowest 2 bits
                if (codec == CompressionCodec.None)
                {
                    var msg = new ResponseMessage
                    {
                        Offset = offset,
                        Message = new Message
                        {
                            Key = Basics.DeserializeByteArray(stream, deserializers.Item1),
                            Value = Basics.DeserializeByteArray(stream, deserializers.Item2),
                            TimeStamp = timestamp
                        }
                    };
                    CheckCrc(crc, stream, crcStartPos);
                    yield return msg;
                }
                else
                {
                    // Key is null, read/check/skip
                    if (BigEndianConverter.ReadInt32(stream) != -1)
                    {
                        throw new InvalidDataException("Compressed messages key should be null");
                    }

                    // Uncompress
                    var compressedLength = BigEndianConverter.ReadInt32(stream);
                    var dataPos = stream.Position;
                    stream.Position += compressedLength;
                    CheckCrc(crc, stream, crcStartPos);
                    using (var uncompressedStream = stream.Pool.Reserve())
                    {
                        Uncompress(uncompressedStream, stream.GetBuffer(), (int) dataPos, compressedLength, codec);
                        // Deserialize recursively
                        if (magic == 0) // v0 message
                        {
                            foreach (var m in
                                LazyDeserializeMessageSet(uncompressedStream, (int) uncompressedStream.Length,
                                    deserializers))
                            {
                                // Flatten
                                yield return m;
                            }
                        }
                        else // v1 message, we have to assign the absolute offsets
                        {
                            var innerMsgs = ResponseMessageListPool.Reserve();
                            // We need to deserialize all messages first, because the wrapper offset is the
                            // offset of the last messe in the set, so wee need to know how many messages there are
                            // before assigning offsets.
                            innerMsgs.AddRange(LazyDeserializeMessageSet(uncompressedStream,
                                (int) uncompressedStream.Length, deserializers));
                            var baseOffset = offset - innerMsgs.Count + 1;
                            foreach (var msg in innerMsgs)
                            {
                                yield return
                                    new ResponseMessage
                                    {
                                        Offset = msg.Offset + baseOffset,
                                        Message = msg.Message
                                    };
                            }
                            ResponseMessageListPool.Release(innerMsgs);
                        }
                    }
                }

                remainingMessageSetBytes -= messageSize;
            }
        }

        static void CheckCrc(int crc, ReusableMemoryStream stream, long crcStartPos)
        {
            var computedCrc = (int)Crc32.Compute(stream, crcStartPos, stream.Position - crcStartPos);
            if (computedCrc != crc)
            {
                throw new CrcException(
                    string.Format("Corrupt message: CRC32 does not match. Calculated {0} but got {1}", computedCrc,
                                  crc));
            }
        }

        #region Compression handling

        private static void Uncompress(ReusableMemoryStream uncompressed, byte[] body, int offset, int length, CompressionCodec codec)
        {
            try
            {
                if (codec == CompressionCodec.Snappy)
                {
#if NET_CORE
                    throw new NotImplementedException();
#else
                    uncompressed.SetLength(SnappyCodec.GetUncompressedLength(body, offset, length));
                    SnappyCodec.Uncompress(body, offset, length, uncompressed.GetBuffer(), 0);
#endif
                }
                else if (codec == CompressionCodec.Gzip)
                {
                    using (var compressed = new MemoryStream(body, offset, length, false))
                    {
                        using (var zipped = new GZipStream(compressed, CompressionMode.Decompress))
                        {
                            using (var tmp = uncompressed.Pool.Reserve())
                            {
                                zipped.ReusableCopyTo(uncompressed, tmp);
                            }
                        }
                    }
                }
                else // compression == CompressionCodec.Lz4
                {
                    KafkaLz4.Uncompress(uncompressed, body, offset);
                }

                uncompressed.Position = 0;
            }
            catch (Exception ex)
            {
                throw new UncompressException("Invalid compressed data.", codec, ex);
            }
        }

        #endregion

        // Used only in tests, and we only support byte array data
        public void Serialize(ReusableMemoryStream stream, object extra, Basics.ApiVersion version)
        {
            BigEndianConverter.Write(stream, Partition);
            BigEndianConverter.Write(stream, (short) ErrorCode);
            BigEndianConverter.Write(stream, HighWatermarkOffset);
            Basics.WriteSizeInBytes(stream, Messages, (s, l) =>
            {
                foreach (var m in l)
                {
                    BigEndianConverter.Write(s, m.Offset);
                    Basics.WriteSizeInBytes(s, m.Message,
                        (st, msg) =>
                            msg.Serialize(st, CompressionCodec.None, extra as Serializers,
                                version == Basics.ApiVersion.V2 ? MessageVersion.V1 : MessageVersion.V0));
                }
            });
        }

        #endregion
    }
}
