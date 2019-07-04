// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

using System.Collections.Generic;
using Kafka.Common;
using Kafka.Public;
using Deserializers = System.Tuple<Kafka.Public.IDeserializer, Kafka.Public.IDeserializer>;


namespace Kafka.Protocol
{
    internal enum TimestampType
    {
        // This should never happen, as it used to convert V0 message sets into RecordBatch
        // and thus, there's no timestamp known.
        LogAppendTime = 1,
        // Means that the timestamp is set when the record is created.
        CreateTime = 2,
    }

    internal class RecordBatch
    {
        private static readonly short CompressionCodecMask = 0x07;
        private static readonly short TransactionalFlagMask = 0x10;
        private static readonly short ControlFlagMask = 0x20;
        private static readonly short TimestampTypeMask = 0x08;

        public static readonly long NoProducerId = -1L;
        public static readonly short NoProducerEpoch = -1;
        // Represents the BaseSequence value when we don't use the feature of idempotent writes.
        public static readonly int NoSequence = -1;
        // Represents the PartitionLeaderEpoch when it's unknown, which is the case when a client produces messages
        public static readonly int NoPartitionLeaderEpoch = -1;

        // Provided by a ProducerIdRequest, can be set to a default value.
        public long ProducerId = NoProducerId;
        public short ProducerEpoch = NoProducerEpoch;

        // Currently, a ProduceRequest can only hold a single RecordBatch, the BaseOffset is thus 0.?
        public long BaseOffset = 0;
        public int BaseSequence = NoSequence;
        public int PartitionLeaderEpoch = NoPartitionLeaderEpoch;
        public bool IsControl = false;
        public bool IsTransactional = false;

        public CompressionCodec CompressionCodec;
        public TimestampType TimestampType;

        public IEnumerable<Record> Records;

        private const int BytesNecessaryToGetLength = 8 // baseOffset
            + 4; // batchLength

        /// <remarks>
        /// From the official protocol documentation available at
        /// https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-FetchAPI
        /// "As an optimization the server is allowed to return a partial message at the end of the message set. Clients should handle this case."
        /// If the end of the RecordBatch exceeds the length of the whole response (= endOfAllBatches), we should discard this RecordBatch.
        /// </remarks>
        public static RecordBatch Deserialize(ReusableMemoryStream input, Deserializers deserializers, long endOfAllBatches)
        {
            var recordBatch = new RecordBatch();

            if (input.Position + BytesNecessaryToGetLength > endOfAllBatches)
            {
                throw new ProtocolException(
                    $"Trying to read a batch record at {input.Position} and the end of all batches is {endOfAllBatches}."
                    + $" There is not enough bytes remaining to even read the first fields...");
            }
            recordBatch.BaseOffset = BigEndianConverter.ReadInt64(input);
            var batchLength = BigEndianConverter.ReadInt32(input);
            var endOfBatch = input.Position + batchLength;
            if (endOfAllBatches < endOfBatch)
            {
                // Partial message, CRCs won't match, return here so the CRC check doesn't throw
                return null;
            }
            recordBatch.PartitionLeaderEpoch = BigEndianConverter.ReadInt32(input);
            var magic = input.ReadByte();
            // Current magic value is 2
            if ((uint) magic != 2)
            {
                throw new UnsupportedMagicByteVersion((byte) magic, "2");
            }

            var crc = (uint) BigEndianConverter.ReadInt32(input);
            var afterCrcPosition = input.Position; // The crc is calculated starting from this position
            Crc32.CheckCrcCastagnoli((int)crc, input, afterCrcPosition, endOfBatch - afterCrcPosition);

            var attributes = BigEndianConverter.ReadInt16(input);

            recordBatch.CompressionCodec = (CompressionCodec) (attributes & CompressionCodecMask);
            recordBatch.IsTransactional = (attributes & TransactionalFlagMask) != 0;
            recordBatch.IsControl = (attributes & ControlFlagMask) != 0;
            recordBatch.TimestampType = (attributes & TimestampTypeMask) > 0
                ? TimestampType.LogAppendTime
                : TimestampType.CreateTime;

            var lastOffsetDelta = BigEndianConverter.ReadInt32(input);

            var firstTimestamp = BigEndianConverter.ReadInt64(input);
            var maxTimestamp = BigEndianConverter.ReadInt64(input);

            recordBatch.ProducerId = BigEndianConverter.ReadInt64(input);
            recordBatch.ProducerEpoch = BigEndianConverter.ReadInt16(input);
            recordBatch.BaseSequence = BigEndianConverter.ReadInt32(input);

            var numberOfRecords = BigEndianConverter.ReadInt32(input);
            recordBatch.Records = DeserializeRecords(recordBatch, input, numberOfRecords, endOfBatch, firstTimestamp, deserializers);

            return recordBatch;
        }

        public static IEnumerable<Record> DeserializeRecords(RecordBatch recordBatch, ReusableMemoryStream input, int numberOfRecords, long endOfBatch,
            long firstTimeStamp, Deserializers deserializers)
        {
            if (recordBatch.CompressionCodec == CompressionCodec.None)
            {
                return DeserializeRecordsUncompressed(recordBatch, input, numberOfRecords, endOfBatch, firstTimeStamp,
                    deserializers);
            }
            using (var uncompressedStream = input.Pool.Reserve())
            {
                Basics.Uncompress(uncompressedStream, input.GetBuffer(), (int) input.Position,
                    (int) (endOfBatch - input.Position), recordBatch.CompressionCodec);
                input.Position = endOfBatch;
                return new List<Record>(DeserializeRecordsUncompressed(recordBatch, uncompressedStream, numberOfRecords, endOfBatch, firstTimeStamp,
                    deserializers)); // We use a list here to force iteration to take place, so that we can release uncompressedStream
            }
        }

        public static IEnumerable<Record> DeserializeRecordsUncompressed(RecordBatch recordBatch, ReusableMemoryStream input, int numberOfRecords, long endOfBatch,
            long firstTimeStamp, Deserializers deserializers)
        {
            for (var i = 0; i < numberOfRecords; i++)
            {
                var length = VarIntConverter.ReadAsInt32(input);
                if (input.Length - input.Position < length)
                {
                    throw new ProtocolException(
                        $"Record said it was of length {length}, but actually only {input.Length - input.Position} bytes remain");
                }

                var attributes = input.ReadByte(); // ignored for now
                var timeStampDelta = VarIntConverter.ReadAsInt64(input);
                var offsetDelta = VarIntConverter.ReadAsInt32(input);

                var keyLength = VarIntConverter.ReadAsInt32(input);
                var key = keyLength == -1 ? null : deserializers.Item1.Deserialize(input, keyLength);
                var valueLength = VarIntConverter.ReadAsInt32(input);
                var value = valueLength == -1 ? null : deserializers.Item1.Deserialize(input, valueLength);

                var headersCount = VarIntConverter.ReadAsInt32(input);
                var headers = new List<KafkaRecordHeader>(headersCount);
                for (var j = 0; j < headersCount; j++)
                {
                    headers.Add(Record.DeserializeHeader(input));
                }

                yield return new Record
                {
                    Headers = headers,
                    Key = key,
                    Timestamp = firstTimeStamp + timeStampDelta,
                    Value = value,
                    Offset = recordBatch.BaseOffset + offsetDelta
                };
            }
        }

        public void Serialize(ReusableMemoryStream target)
        {
            BigEndianConverter.Write(target, BaseOffset);
            var batchLengthPosition = target.Position;
            BigEndianConverter.Write(target, -1); // placeholder for batchLength: int32
            // The value of batchLength is the number of bytes required to read after the
            // batchLength field, we retain the current buffer position to compute it later.
            var afterBatchLengthPosition = target.Position;
            BigEndianConverter.Write(target, PartitionLeaderEpoch);

            // Current magic value is 2
            target.WriteByte(2);

            var crcPosition = target.Position;
            BigEndianConverter.Write(target, 0); // placeholder for CRC: int32
            var afterCrcPosition = target.Position;

            short attributes = 0x0;
            if (CompressionCodec != CompressionCodec.None)
                attributes |= (short)((short)CompressionCodec & CompressionCodecMask);
            if (IsTransactional)
                attributes |= TransactionalFlagMask;
            if (IsControl)
                attributes |= ControlFlagMask;
            if (TimestampType == TimestampType.LogAppendTime)
                attributes |= TimestampTypeMask;

            BigEndianConverter.Write(target, attributes);

            var lastOffsetDeltaPosition = target.Position;
            BigEndianConverter.Write(target, -1); // placeholder for LastOffsetDelta: int32

            var firstTimestampPosition = target.Position;
            BigEndianConverter.Write(target, -1L); // placeholder for FirstTimestamp: int64

            var maxTimestampPosition = target.Position;
            BigEndianConverter.Write(target, -1L); // placeholder for MaxTimestamp: int64

            BigEndianConverter.Write(target, ProducerId);
            BigEndianConverter.Write(target, ProducerEpoch);
            BigEndianConverter.Write(target, BaseSequence); // BaseSequence: int32

            var recordsCountPosition = target.Position;
            BigEndianConverter.Write(target, -1);  // RecordsCount: int32

            (int lastOffsetDelta, long firstTimestamp, long maxTimestamp) = CompressionCodec == CompressionCodec.None
                ? SerializeRecords(target)
                : SerializeRecordsWithCompression(target, CompressionCodec);

            var endPosition = target.Position;

            int batchLength = (int)(target.Position - afterBatchLengthPosition);
            target.Position = batchLengthPosition;
            BigEndianConverter.Write(target, batchLength);

            target.Position = lastOffsetDeltaPosition;
            BigEndianConverter.Write(target, lastOffsetDelta);

            target.Position = firstTimestampPosition;
            BigEndianConverter.Write(target, firstTimestamp);

            target.Position = maxTimestampPosition;
            BigEndianConverter.Write(target, maxTimestamp);

            target.Position = recordsCountPosition;
            BigEndianConverter.Write(target, lastOffsetDelta + 1); // actual records count

            // Crc is computed on the object from right after the CRC to the end of the buffer
            uint crc = Crc32.ComputeCastagnoli(target, afterCrcPosition, endPosition - afterCrcPosition);
            target.Position = crcPosition;
            BigEndianConverter.Write(target, (int)crc);

            target.Position = endPosition;
        }

        private (int, long, long) SerializeRecords(ReusableMemoryStream target)
        {
            int lastOffsetDelta = -1;
            long firstTimestamp = -1L, maxTimestamp = -1L;
            foreach (var record in Records)
            {
                ++lastOffsetDelta;

                if (firstTimestamp == -1L)
                {
                    firstTimestamp = record.Timestamp;
                }

                if (record.Timestamp > maxTimestamp)
                {
                    maxTimestamp = record.Timestamp;
                }

                record.Serialize(target, firstTimestamp, lastOffsetDelta);
            }

            return (lastOffsetDelta, firstTimestamp, maxTimestamp);
        }

        private (int, long, long) SerializeRecordsWithCompression(ReusableMemoryStream target,
            CompressionCodec compression)
        {
            using (var uncompressedStream = target.Pool.Reserve())
            {
                var resultOffsetAndTimestamps = SerializeRecords(uncompressedStream);
                using (var compressed = target.Pool.Reserve())
                {
                    Basics.CompressStream(uncompressedStream, compressed, compression);
                    using (var tmpBuffer = target.Pool.Reserve())
                    {
                        compressed.Position = 0;
                        compressed.ReusableCopyTo(target, tmpBuffer);
                    }

                    return resultOffsetAndTimestamps;
                }
            }
        }
    }

    internal struct Record
    {
        public object Key;
        public object Value;

        public ISizableSerializer KeySerializer;
        public ISizableSerializer ValueSerializer;

        public long Timestamp;

        public long Offset;

        public ICollection<KafkaRecordHeader> Headers;

        public ReusableMemoryStream SerializedKeyValue;

        public ReusableMemoryStream Serialize(ReusableMemoryStream target, long baseTimestamp, long offsetDelta)
        {
            long timestampDelta = Timestamp - baseTimestamp;

            VarIntConverter.Write(target, SizeOfBodyInBytes(offsetDelta, timestampDelta));

            // Record attributes are always null.
            target.WriteByte(0x00);

            VarIntConverter.Write(target, timestampDelta);
            VarIntConverter.Write(target, offsetDelta);

            if (SerializedKeyValue == null)
            {
                Basics.WriteObject(target, Key, KeySerializer);
                Basics.WriteObject(target, Value, ValueSerializer);
            }
            else
            {
                target.Write(SerializedKeyValue.GetBuffer(), 0, (int)SerializedKeyValue.Length);
            }

            if (Headers == null)
            {
                target.Write(Basics.ZeroVarInt, 0, Basics.ZeroVarInt.Length);
            }
            else
            {
                VarIntConverter.Write(target, Headers.Count);
                foreach (KafkaRecordHeader header in Headers)
                {
                    SerializeHeader(target, header);
                }
            }

            return target;
        }

        /// <summary>
        /// Returns the size of the Serialized object without the attributes.
        /// </summary>
        /// <returns>Serialized size of the body in bytes</returns>
        public long SizeOfBodyInBytes(long offsetDelta, long timestampDelta)
        {
            var size = 1L; // always 1 byte for attributes

            size += VarIntConverter.SizeOfVarInt(offsetDelta);
            size += VarIntConverter.SizeOfVarInt(timestampDelta);


            if (SerializedKeyValue == null)
            {
                if (Key == null)
                {
                    size += Basics.MinusOneVarInt.Length;
                }
                else
                {
                    var keySize = Basics.SizeOfSerializedObject(Key, KeySerializer);
                    size += keySize + VarIntConverter.SizeOfVarInt(keySize);
                }

                if (Value == null)
                {
                    size += Basics.MinusOneVarInt.Length;
                }
                else
                {
                    var valueSize = Basics.SizeOfSerializedObject(Value, ValueSerializer);
                    size += valueSize + VarIntConverter.SizeOfVarInt(valueSize);
                }
            }
            else
            {
                size += SerializedKeyValue.Length;
            }

            if (Headers == null)
            {
                size += Basics.ZeroVarInt.Length;
            }
            else
            {
                var headersCount = 0;
                foreach (KafkaRecordHeader header in Headers)
                {
                    size += (int)SerializedSizeOfHeader(header);
                    ++headersCount;
                }

                size += VarIntConverter.SizeOfVarInt(headersCount);
            }

            return size;
        }

        public static ReusableMemoryStream SerializeHeader(ReusableMemoryStream target, KafkaRecordHeader header)
        {
            header.Validate();
            Basics.SerializeStringWithVarIntSize(target, header.Key);
            Basics.SerializeBytesWithVarIntSize(target, header.Value);
            return target;
        }

        public static KafkaRecordHeader DeserializeHeader(ReusableMemoryStream stream)
        {
            return new KafkaRecordHeader()
            {
                Key = Basics.DeserializeStringWithVarIntSize(stream),
                Value = Basics.DeserializeBytesWithVarIntSize(stream)
            };
        }

        public static long SerializedSizeOfHeader(KafkaRecordHeader header)
        {
            header.Validate();
            var keySize = Basics.SizeOfSerializedString(header.Key);
            return keySize + header.Value.Length + VarIntConverter.SizeOfVarInt(keySize) + VarIntConverter.SizeOfVarInt(header.Value.Length);
        }
    }
}
