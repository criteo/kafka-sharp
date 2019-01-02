// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

using System;
using System.IO;

namespace Kafka.Common
{
    /// <summary>
    /// Implement encoding and decoding for VarInt format.
    /// Values are encoded in ZigZag format (a format which allows  to keep small
    /// binary values for negative integers).
    ///
    /// When reading a VarInt, an OverflowError is thrown if the value doesn't fit
    /// in the expected return type.
    ///
    /// Note: we only deal with signed types, since Kafka is implemented in
    /// Java, which doesn't have unsigned integers.
    ///
    /// See: https://developers.google.com/protocol-buffers/docs/encoding#varints
    /// </summary>
    internal static class VarIntConverter
    {
        private static ulong ToZigZag(long i)
        {
            return unchecked((ulong)((i << 1) ^ (i >> 63)));
        }

        /// <summary>
        /// Returns the minimum required size in bytes to store the given value as a VarInt.
        /// </summary>
        /// <param name="value">value to be represented as a VarInt</param>
        /// <returns></returns>
        public static int SizeOfVarInt(long value)
        {
            var asZigZag = ToZigZag(value);
            int nbBytes = 1;
            while ((asZigZag & 0xffffffffffffff80L) != 0L)
            {
                nbBytes += 1;
                asZigZag = asZigZag >> 7;
            }

            return nbBytes;
        }

        public static void Write(MemoryStream stream, long i)
        {
            var asZigZag = ToZigZag(i);

            // value & 1111 1111 ... 1000 0000 will zero the last 7 bytes,
            // if the result is zero, it means we only have those last 7 bytes
            // to write.
            while((asZigZag & 0xffffffffffffff80L) != 0L)
            {
                // keep only the 7 most significant bytes:
                // value = (value & 0111 1111)
                // and add a 1 in the most significant bit of the byte, meaning
                // it's not the last byte of the VarInt:
                // value = (value | 1000 0000)
                stream.WriteByte((byte)((asZigZag & 0x7f) | 0x80));
                // Shift the 7 bits we just wrote to the stream and continue:
                asZigZag = asZigZag >> 7;
            }
            stream.WriteByte((byte)asZigZag);
        }

        public static void Write(MemoryStream stream, int i)
        {
            Write(stream, (long)i);
        }

        public static void Write(MemoryStream stream, short i)
        {
            Write(stream, (long)i);
        }

        public static void Write(MemoryStream stream, byte i)
        {
            Write(stream, (long)i);
        }

        public static void Write(MemoryStream stream, bool i)
        {
            Write(stream, i ? 1L : 0L);
        }

        public static long ReadInt64(MemoryStream stream)
        {
            ulong asZigZag = 0L; // Result value
            int i = 0; // Number of bits written
            long b; // Byte read

            // Check if the 8th bit of the byte is 1, meaning there will be more to read:
            // b & 1000 0000
            while (((b = stream.ReadByte()) & 0x80) != 0) {
                // Take the 7 bits of the byte we want to add and insert them at the
                // right location (offset i)
                asZigZag |= (ulong)(b & 0x7f) << i;
                i += 7;
                if (i > 63)
                    throw new OverflowException();
            }

            if (i == 63 && b != 0x01)
            {
                // We read 63 bits, we can only read one more (the most significant bit, MSB),
                // or it means that the VarInt can't fit in a long.
                // If the bit to read was 0, we would not have read it (as it's the MSB), thus, it must be 1.
                throw new OverflowException();
            }

            asZigZag |= (ulong)b << i;

            // The value is signed
            if ((asZigZag & 0x1) == 0x1)
            {
                return (-1 * ((long)(asZigZag >> 1) + 1));
            }


            return (long)(asZigZag >> 1);
        }

        public static int ReadInt32(MemoryStream stream)
        {
            return checked((int)ReadInt64(stream));
        }

        public static short ReadInt16(MemoryStream stream)
        {
            return checked((short)ReadInt64(stream));
        }

        public static byte ReadByte(MemoryStream stream)
        {
            return checked((byte)ReadInt64(stream));
        }

        public static bool ReadBool(MemoryStream stream)
        {
            return ReadInt64(stream) != 0;
        }
    }
}
