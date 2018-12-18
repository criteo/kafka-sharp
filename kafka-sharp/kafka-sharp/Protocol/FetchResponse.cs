// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Kafka.Common;
using Kafka.Public;

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

    struct AbortedTransaction : IMemoryStreamSerializable
    {
        public long ProducerId;
        public long FirstOffset;
        public void Serialize(ReusableMemoryStream stream, object extra, Basics.ApiVersion version)
        {
            BigEndianConverter.Write(stream, ProducerId);
            BigEndianConverter.Write(stream, FirstOffset);
        }

        public void Deserialize(ReusableMemoryStream stream, object extra, Basics.ApiVersion version)
        {
            ProducerId = BigEndianConverter.ReadInt64(stream);
            FirstOffset = BigEndianConverter.ReadInt64(stream);
        }
    }

    struct FetchPartitionResponse : IMemoryStreamSerializable
    {
        public long HighWatermarkOffset;
        public List<ResponseMessage> Messages;
        public int Partition;
        public ErrorCode ErrorCode;
        public long LastStableOffset;
        public long LogStartOffset;
        public AbortedTransaction[] AbortedTransactions;

        private const int HeaderSize = 4 // size
            + 8 // baseOffset
            + 4 // batchLength
            + 4; // PartitionLeaderEpoch

        internal CompressionCodec Compression; // Only used in test for serializing
        #region Deserialization

        public void Deserialize(ReusableMemoryStream stream, object extra, Basics.ApiVersion version)
        {
            Partition = BigEndianConverter.ReadInt32(stream);
            ErrorCode = (ErrorCode) BigEndianConverter.ReadInt16(stream);
            HighWatermarkOffset = BigEndianConverter.ReadInt64(stream);
            if (version >= Basics.ApiVersion.V4)
            {
                LastStableOffset = BigEndianConverter.ReadInt64(stream);
                AbortedTransactions = Basics.DeserializeArray<AbortedTransaction>(stream);
            }
            if (version >= Basics.ApiVersion.V5)
            {
                LogStartOffset = BigEndianConverter.ReadInt64(stream);
            }
            try
            {
                Messages = version >= Basics.ApiVersion.V3
                    ? DeserializeRecordBatch(stream, extra as Deserializers)
                    : DeserializeMessageSet(stream, extra as Deserializers);
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

        internal static List<ResponseMessage> DeserializeRecordBatch(ReusableMemoryStream stream,
            Deserializers deserializers)
        {
            var list = ResponseMessageListPool.Reserve();

            // First of all, we need to check if it is really a recordBatch (i.e. magic byte = 2)
            // For that, we need to fast-forward to the magic byte. Depending on its value, we go back
            // and deserialize following the corresponding format. (Kafka is amazing).
            var startOfBatchOffset = stream.Position;
            stream.Position += HeaderSize;
            var magic = stream.ReadByte();

            stream.Position = startOfBatchOffset;
            if (magic < 2)
            {
                // We were mistaken, this is not a recordBatch but a message set!
                return DeserializeMessageSet(stream, deserializers);
            }
            // Now we know what we received is a proper recordBatch
            var recordBatch = new RecordBatch();
            var size = BigEndianConverter.ReadInt32(stream);
            var endOfAllBatches = stream.Position + size;
            if (stream.Length < endOfAllBatches)
            {
                throw new ProtocolException($"Fetch response advertise record batches of total size {size},"
                    + $" but the stream only contains {stream.Length - stream.Position} byte remaining");
            }
            while (stream.Position < endOfAllBatches)
            {
                recordBatch.Deserialize(stream, deserializers, endOfAllBatches);
                list.AddRange(recordBatch.Records.Select(record => new ResponseMessage
                {
                    Message = new Message { Key = record.Key, Value = record.Value, TimeStamp = record.Timestamp },
                    Offset = record.Offset
                }));
            }
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
                    throw new UnsupportedMagicByteVersion((byte) magic, "0 or 1");
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
                    Crc32.CheckCrc(crc, stream, crcStartPos);
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
                    Crc32.CheckCrc(crc, stream, crcStartPos);
                    using (var uncompressedStream = stream.Pool.Reserve())
                    {
                        Basics.Uncompress(uncompressedStream, stream.GetBuffer(), (int) dataPos, compressedLength, codec);
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

        // Used only in tests, and we only support byte array data
        public void Serialize(ReusableMemoryStream stream, object extra, Basics.ApiVersion version)
        {
            BigEndianConverter.Write(stream, Partition);
            BigEndianConverter.Write(stream, (short) ErrorCode);
            BigEndianConverter.Write(stream, HighWatermarkOffset);
            if (version >= Basics.ApiVersion.V4)
            {
                BigEndianConverter.Write(stream, LastStableOffset);
                Basics.WriteArray(stream, AbortedTransactions);
            }
            if (version >= Basics.ApiVersion.V5)
            {
                BigEndianConverter.Write(stream, LogStartOffset);
            }

            if (version >= Basics.ApiVersion.V3)
            {
                var batch = new RecordBatch
                {
                    CompressionCodec = Compression,
                    Records = Messages.Select(responseMessage => new Record
                    {
                        Key = responseMessage.Message.Key,
                        Value = responseMessage.Message.Value,
                        Timestamp = responseMessage.Message.TimeStamp,
                        KeySerializer = null,
                        ValueSerializer = null
                    }),
                };
                Basics.WriteSizeInBytes(stream, batch.Serialize);
                return;
            }
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
