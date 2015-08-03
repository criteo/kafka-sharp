// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. 
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Kafka.Common;

namespace Kafka.Protocol
{
    static class Basics
    {
        public static readonly byte[] MinusOne32 = { 0xff, 0xff, 0xff, 0xff };
        static readonly byte[] One32 = { 0x00, 0x00, 0x00, 0x01 };
        static readonly byte[] Two32 = { 0x00, 0x00, 0x00, 0x02 };
        static readonly byte[] Eight32 = { 0x00, 0x00, 0x00, 0x08 };
        static readonly byte[] MinusOne16 = { 0xff, 0xff };
        static readonly byte[] ApiVersion = { 0x00, 0x00 };
        public static readonly byte[] Zero64 = { 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 };
        public static readonly byte[] Zero32 = { 0x00, 0x00, 0x00, 0x00 };

        public enum ApiKey : short
        {
            ProduceRequest = 0,
            FetchRequest = 1,
            OffsetRequest = 2,
            MetadataRequest = 3,
            // Non-user facing control APIs 4-7
            OffsetCommitRequest = 8,
            OffsetFetchRequest = 9,
            ConsumerMetadataRequest = 10
        }

        public static string DeserializeString(MemoryStream stream)
        {
            var len = BigEndianConverter.ReadInt16(stream);
            // per contract, null string is represented with -1 len.
            if (len == -1)
                return null;

            var buffer = new byte[len];
            stream.Read(buffer, 0, len);
            return Encoding.UTF8.GetString(buffer);
        }

        public static void SerializeString(MemoryStream stream, string s)
        {
            if (s == null)
            {
                stream.Write(MinusOne16, 0, 2);
                return;
            }

            BigEndianConverter.Write(stream, (short)s.Length);
            var b = Encoding.UTF8.GetBytes(s);
            stream.Write(b, 0, b.Length);
        }

        public static byte[] WriteMessageLength(MemoryStream stream)
        {
            var buff = stream.ToArray();
            var len = buff.Length - 4; // -4 because do not count size flag itself
            // write message length to the head
            // TODO: use seek?
            BigEndianConverter.Write(buff, len);
            return buff;
        }

        static long ReserveHeader(MemoryStream stream)
        {
            stream.Write(Zero32, 0, 4);
            return stream.Position;
        }

        static void WriteHeader(MemoryStream stream, long initPos)
        {
            var pos = stream.Position;
            var size = pos - initPos;
            stream.Position = initPos - 4;
            BigEndianConverter.Write(stream, (int)size);
            stream.Position = pos;
        }

        public static void WriteSizeInBytes(MemoryStream stream, Action<MemoryStream> write)
        {
            var initPos = ReserveHeader(stream);
            write(stream);
            WriteHeader(stream, initPos);
        }

        public static void WriteSizeInBytes<T>(MemoryStream stream, T t, Action<MemoryStream, T> write)
        {
            var initPos = ReserveHeader(stream);
            write(stream, t);
            WriteHeader(stream, initPos);
        }

        public static void WriteSizeInBytes<T, U>(MemoryStream stream, T t, U u, Action<MemoryStream, T, U> write)
        {
            var initPos = ReserveHeader(stream);
            write(stream, t, u);
            WriteHeader(stream, initPos);
        }

        public static void WriteArray<T>(MemoryStream stream, IEnumerable<T> items, Action<MemoryStream, T> write)
        {
            var sizePosition = stream.Position;
            stream.Write(MinusOne32, 0, 4); // placeholder for count field
            var count = 0;
            foreach (var item in items)
            {
                write(stream, item);
                count++;
            }
            var pos = stream.Position; // update count field
            stream.Position = sizePosition;
            BigEndianConverter.Write(stream, count);
            stream.Position = pos;
        }

        public static void WriteRequestHeader(MemoryStream stream, int correlationId, ApiKey requestType, byte[] clientId)
        {
            stream.Write(MinusOne32, 0, 4); // reserve space for message size
            BigEndianConverter.Write(stream, (short)requestType);
            stream.Write(ApiVersion, 0, 2);
            BigEndianConverter.Write(stream, correlationId);
            BigEndianConverter.Write(stream, (short)clientId.Length);
            stream.Write(clientId, 0, clientId.Length);
        }

        public static void Update(MemoryStream stream, long pos, byte[] buff)
        {
            var currPos = stream.Position;
            stream.Position = pos;
            stream.Write(buff, 0, buff.Length);
            stream.Position = currPos;
        }
    }
}
