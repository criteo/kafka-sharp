// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

using System;
using System.IO;

namespace Kafka.Common
{
    /// <summary>
    ///
    /// Originally comes from kafka4net project: https://github.com/ntent-ad/kafka4net
    /// (https://github.com/ntent-ad/kafka4net/blob/master/src/Utils/BigEndianConverter.cs)
    ///
    /// License was Apache 2: http://www.apache.org/licenses/LICENSE-2.0
    ///
    /// </summary>
    internal static class BigEndianConverter
    {
        public static int ReadInt32(MemoryStream s)
        {
            if (s.Position + 4 > s.Length)
                throw new Exception(string.Format("ReadInt32 needs 4 bytes but got only {0}", s.Length - s.Position));
            return s.ReadByte() << 3*8 | s.ReadByte() << 2*8 | s.ReadByte() << 8 | s.ReadByte();
        }

        public static short ReadInt16(MemoryStream s)
        {
            if (s.Position + 2 > s.Length)
                throw new Exception(string.Format("ReadInt16 needs 2 bytes but got only {0}", s.Length - s.Position));
            return (short) ((s.ReadByte() << 8) | s.ReadByte());
        }

        public static long ReadInt64(MemoryStream stream)
        {
            if (stream.Position + 8 > stream.Length)
                throw new Exception(string.Format("ReadInt64 needs 8 bytes but got only {0}",
                                                  stream.Length - stream.Position));

            var res = 0L;
            for (int i = 0; i < 8; i++)
                res = res << 8 | (uint) stream.ReadByte();
            return res;
        }

        public static void Write(MemoryStream stream, long i)
        {
            ulong ui = (ulong) i;
            for (int j = 7; j >= 0; j--)
                stream.WriteByte((byte) (ui >> j*8 & 0xff));
        }

        public static void Write(MemoryStream stream, int i)
        {
            WriteByte(stream, i >> 8*3);
            WriteByte(stream, i >> 8*2);
            WriteByte(stream, i >> 8);
            WriteByte(stream, i);
        }

        public static void Write(MemoryStream stream, short i)
        {
            WriteByte(stream, i >> 8);
            WriteByte(stream, i);
        }

        public static void WriteByte(MemoryStream stream, int i)
        {
            stream.WriteByte((byte) (i & 0xff));
        }

        public static void Write(byte[] buff, int i)
        {
            Write(buff, i, 0);
        }

        public static void Write(byte[] buff, int i, int offset)
        {
            buff[offset + 0] = (byte)(i >> 8 * 3);
            buff[offset + 1] = (byte)((i & 0xff0000) >> 8 * 2);
            buff[offset + 2] = (byte)((i & 0xff00) >> 8);
            buff[offset + 3] = (byte)(i & 0xff);
        }

        public static int ToInt32(byte[] buff)
        {
            return ToInt32(buff, 0);
        }

        public static int ToInt32(byte[] buff, int offset)
        {
            return (buff[offset] << 8*3) | (buff[offset + 1] << 8*2) | (buff[offset + 2] << 8) | buff[offset + 3];
        }
    }
}

