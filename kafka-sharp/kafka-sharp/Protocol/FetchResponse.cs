// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using Kafka.Common;
using Kafka.Public;
using Snappy;

namespace Kafka.Protocol
{
    struct ResponseMessage
    {
        public long Offset;
        public Message Message;
    }

    static class ResponseMessageListPool
    {
        private static readonly Pool<List<ResponseMessage>> _pool = new Pool<List<ResponseMessage>>(() => new List<ResponseMessage>(), l => l.Clear());

        public static List<ResponseMessage> Reserve()
        {
            return _pool.Reserve();
        }

        public static void Release(List<ResponseMessage> list)
        {
            _pool.Release(list);
        }
    }

    struct FetchPartitionResponse : IMemoryStreamSerializable
    {
        public long HighWatermarkOffset;
        public List<ResponseMessage> Messages;
        public int Partition;
        public ErrorCode ErrorCode;

        #region Deserialization

        public void Deserialize(ReusableMemoryStream stream)
        {
            Partition = BigEndianConverter.ReadInt32(stream);
            ErrorCode = (ErrorCode) BigEndianConverter.ReadInt16(stream);
            HighWatermarkOffset = BigEndianConverter.ReadInt64(stream);
            Messages = DeserializeMessageSet(stream);
        }

        internal static List<ResponseMessage> DeserializeMessageSet(ReusableMemoryStream stream)
        {
            var list = ResponseMessageListPool.Reserve();
            list.AddRange(LazyDeserializeMessageSet(stream, BigEndianConverter.ReadInt32(stream)));
            return list;
        }

        // Deserialize a message set to a sequence of messages.
        // This handles the "partial message allowed at end of message set" from Kafka brokers
        // and compressed message sets (the method recursively calls itself in this case and
        // flatten the result). The returned enumeration must be enumerated for deserialization
        // effectiveley occuring.
        private static IEnumerable<ResponseMessage> LazyDeserializeMessageSet(ReusableMemoryStream stream, int messageSetSize)
        {
            var remainingMessageSetBytes = messageSetSize;

            while (remainingMessageSetBytes > 0)
            {
                const int offsetSize = 8;
                const int msgsizeSize = 4;
                if (remainingMessageSetBytes < offsetSize + msgsizeSize)
                {
                    // This is a partial message => skip to the end of the message set.
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
                var crcPos = stream.Position; // crc is computed from this position
                var magic = stream.ReadByte(); // This is ignored, should be zero but who knows? TODO: check it is zero?
                var attributes = stream.ReadByte();
                var msg = new ResponseMessage
                {
                    Offset = offset,
                    Message = new Message
                    {
                        Key = Basics.DeserializeByteArray(stream),
                        Value = Basics.DeserializeByteArray(stream) // TODO: in case of compressed stream, avoid byte[] creation/copy
                    }
                };
                var pos = stream.Position;
                var computedCrc = (int)Crc32.Compute(stream, crcPos, pos - crcPos);
                if (computedCrc != crc)
                {
                    throw new CrcException(
                        string.Format("Corrupt message: CRC32 does not match. Calculated {0} but got {1}", computedCrc,
                                      crc));
                }

                // Check for compression
                var codec = (CompressionCodec)(attributes & 3); // Lowest 2 bits
                if (codec == CompressionCodec.None)
                {
                    yield return msg;
                }
                else
                {
                    // Uncompress
                    using (var uncompressedStream = Uncompress(msg.Message.Value, codec))
                    {
                        // Deserialize recursively
                        foreach (var m in LazyDeserializeMessageSet(uncompressedStream, (int) uncompressedStream.Length))
                        {
                            // Flatten
                            yield return m;
                        }
                    }
                }

                remainingMessageSetBytes -= messageSize;
            }
        }

        #region Compression handling

        private static ReusableMemoryStream Uncompress(byte[] body, CompressionCodec codec)
        {
            try
            {
                ReusableMemoryStream uncompressed;
                if (codec == CompressionCodec.Snappy)
                {
                    uncompressed = ReusableMemoryStream.Reserve(SnappyCodec.GetUncompressedLength(body));
                    SnappyCodec.Uncompress(body, 0, body.Length, uncompressed.GetBuffer(), 0);
                }
                else // compression == CompressionCodec.Gzip
                {
                    uncompressed = ReusableMemoryStream.Reserve();
                    using (var compressed = new MemoryStream(body))
                    {
                        using (var gzip = new GZipStream(compressed, CompressionMode.Decompress))
                        {
                            gzip.ReusableCopyTo(uncompressed);
                        }
                    }
                }
                uncompressed.Position = 0;
                return uncompressed;
            }
            catch (Exception ex)
            {
                throw new UncompressException("Invalid compressed data.", codec, ex);
            }
        }

        #endregion

        // Used only in tests
        public void Serialize(ReusableMemoryStream stream)
        {
            BigEndianConverter.Write(stream, Partition);
            BigEndianConverter.Write(stream, (short) ErrorCode);
            BigEndianConverter.Write(stream, HighWatermarkOffset);
            Basics.WriteSizeInBytes(stream, Messages, (s, l) =>
            {
                foreach (var m in l)
                {
                    BigEndianConverter.Write(s, m.Offset);
                    Basics.WriteSizeInBytes(s, m.Message, (st, msg) => msg.Serialize(st, CompressionCodec.None));
                }
            });
        }

        #endregion
    }

    /// <summary>
    /// TODO: inherit from KafkaException (to define)
    /// </summary>
    class CrcException : Exception
    {
        public CrcException(string message)
            : base(message)
        {
        }
    }

    class UncompressException : Exception
    {
        public CompressionCodec Codec { get; internal set; }

        public UncompressException(string message, CompressionCodec codec, Exception ex)
            : base(message, ex)
        {
            Codec = codec;
        }
    }


}
