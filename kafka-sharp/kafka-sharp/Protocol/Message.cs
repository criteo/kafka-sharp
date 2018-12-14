// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

using System;
using System.Collections.Generic;
using Kafka.Common;
using Kafka.Public;

namespace Kafka.Protocol
{
    internal enum MessageVersion
    {
        V0 = 0,
        V1 = 1,
        // Message V2 is implemented in RecordBatch and shall not be used here.
    }

    internal struct Message
    {
        public object Key;
        public object Value;
        public ICollection<KafkaRecordHeader> Headers;
        public long TimeStamp;

        // Visible for tests
        internal ReusableMemoryStream SerializedKeyValue;

        private const int MinimumValidSizeForSerializedKeyValue = 2 * 4; // At least 4 bytes for key size and 4 bytes for value size

        public void SerializeKeyValue(ReusableMemoryStream target, Tuple<ISerializer, ISerializer> serializers)
        {
            SerializedKeyValue = target;
            DoSerializeKeyValue(SerializedKeyValue, serializers);
            Key = null;
            Value = null;
        }

        public void ReleaseSerializedKeyValue()
        {
            // Make sure that the buffer cannot be disposed twice (not good for buffer pooling)
            SerializedKeyValue?.Dispose();
            SerializedKeyValue = null;
        }

        public void Serialize(ReusableMemoryStream stream, CompressionCodec compressionCodec,
            Tuple<ISerializer, ISerializer> serializers, MessageVersion msgVersion)
        {
            var crcPos = stream.Position;
            stream.Write(Basics.MinusOne32, 0, 4); // crc placeholder
            var bodyPos = stream.Position;

            // V0 message format
            if (msgVersion == MessageVersion.V0)
            {
                stream.WriteByte(0); // magic byte
                stream.WriteByte((byte) compressionCodec); // attributes
            }
            else // V1 message format
            {
                stream.WriteByte(1); // magic byte
                stream.WriteByte((byte) compressionCodec); // attributes
                BigEndianConverter.Write(stream, TimeStamp);
            }

            if (SerializedKeyValue != null)
            {
                if (SerializedKeyValue.Length < MinimumValidSizeForSerializedKeyValue)
                {
                    HandleInvalidSerializedKeyValue(stream);
                }
                else
                {
                    stream.Write(SerializedKeyValue.GetBuffer(), 0, (int) SerializedKeyValue.Length);
                }
            }
            else
            {
                DoSerializeKeyValue(stream, serializers);
            }

            // update crc
            var crc = Crc32.Compute(stream, bodyPos, stream.Position - bodyPos);
            var curPos = stream.Position;
            stream.Position = crcPos;
            BigEndianConverter.Write(stream, (int) crc);
            stream.Position = curPos;
        }

        private void HandleInvalidSerializedKeyValue(ReusableMemoryStream stream)
        {
            stream.Logger?.LogError("Invalid SerializedKeyValue. Length is only " + SerializedKeyValue.Length
                + " bytes. Message cannot be serialized : " + SerializedKeyValue.GetBuffer());

            // Simulate an empty key & message to not send a corrupted message
            stream.Write(Basics.MinusOne32, 0, 4);
            stream.Write(Basics.MinusOne32, 0, 4);
        }

        private void DoSerializeKeyValue(ReusableMemoryStream stream, Tuple<ISerializer, ISerializer> serializers)
        {
            if (Key == null)
            {
                stream.Write(Basics.MinusOne32, 0, 4);
            }
            else
            {
                SerializeObject(stream, serializers.Item1, Key);
            }

            if (Value == null)
            {
                stream.Write(Basics.MinusOne32, 0, 4);
            }
            else
            {
                SerializeObject(stream, serializers.Item2, Value);
            }
        }

        private static void SerializeObject(ReusableMemoryStream stream, ISerializer serializer, object theValue)
        {
            // byte[] are just copied
            var bytes = theValue as byte[];
            if (bytes != null)
            {
                byte[] array = bytes;
                BigEndianConverter.Write(stream, array.Length);
                stream.Write(array, 0, array.Length);
            }
            else
            {
                Basics.WriteSizeInBytes(stream, theValue, serializer, SerializerWrite);
            }
        }

        private static void SerializerWrite(ReusableMemoryStream stream, object m, ISerializer ser)
        {
            var serializable = m as IMemorySerializable;
            if (serializable != null)
            {
                serializable.Serialize(stream);
            }
            else
            {
                ser.Serialize(m, stream);
            }
        }
    }
}
