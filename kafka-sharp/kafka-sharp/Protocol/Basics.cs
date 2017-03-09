// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

using System;
using System.Collections.Generic;
using Kafka.Common;
using Kafka.Public;

namespace Kafka.Protocol
{
    // Utility interface to facilitate code factorization.
    // This is not intended to be beautiful.
    internal interface IMemoryStreamSerializable
    {
        void Serialize(ReusableMemoryStream stream, object extra);
        void Deserialize(ReusableMemoryStream stream, object extra);
    }

    static class Basics
    {
        public static readonly byte[] MinusOne32 = { 0xff, 0xff, 0xff, 0xff };
        static readonly byte[] MinusOne16 = { 0xff, 0xff };
        static readonly byte[] ApiVersion0 = { 0x00, 0x00 };
        static readonly byte[] ApiVersion1 = { 0x00, 0x01 };
        static readonly byte[] ApiVersion2 = { 0x00, 0x02 };
        public static readonly byte[] Zero64 = { 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 };
        public static readonly byte[] Zero32 = { 0x00, 0x00, 0x00, 0x00 };


        public struct ApiVersion
        {
            public readonly byte[] Version;

            public ApiVersion(byte[] versionArray)
            {
                Version = versionArray;
            }

            public static ApiVersion V0 = new ApiVersion(ApiVersion0);
            public static ApiVersion V1 = new ApiVersion(ApiVersion1);
            public static ApiVersion V2 = new ApiVersion(ApiVersion2);
        }

        public enum ApiKey : short
        {
            ProduceRequest = 0,
            FetchRequest = 1,
            OffsetRequest = 2,
            MetadataRequest = 3,
            // Non-user facing control APIs 4-7
            OffsetCommitRequest = 8,
            OffsetFetchRequest = 9,
            GroupCoordinatorRequest = 10,
            JoinGroupRequest = 11,
            HeartbeatRequest = 12,
            LeaveGroupRequest = 13,
            SyncGroupRequest = 14,
            DescribeGroupsRequest = 15,
            ListGroupsRequest = 16,
        }

        public static byte[] DeserializeBytes(ReusableMemoryStream stream)
        {
            var len = BigEndianConverter.ReadInt32(stream);
            // per contract, null string is represented with -1 len.
            if (len == -1)
                return null;

            var buffer = new byte[len];
            stream.Read(buffer, 0, len);
            return buffer;
        }

        public static void SerializeBytes(ReusableMemoryStream stream, byte[] b)
        {
            if (b == null)
            {
                stream.Write(MinusOne32, 0, 4);
                return;
            }

            BigEndianConverter.Write(stream, b.Length);
            stream.Write(b, 0, b.Length);
        }

        private static readonly StringDeserializer _stringDeser = new StringDeserializer();

        public static string DeserializeString(ReusableMemoryStream stream)
        {
            var len = BigEndianConverter.ReadInt16(stream);
            // per contract, null string is represented with -1 len.
            if (len == -1)
                return null;

            return _stringDeser.Deserialize(stream, len) as string;
        }

        private static readonly StringSerializer _stringSer = new StringSerializer();

        public static void SerializeString(ReusableMemoryStream stream, string s)
        {
            if (s == null)
            {
                stream.Write(MinusOne16, 0, 2);
                return;
            }

            var start = stream.Position;
            BigEndianConverter.Write(stream, (short)0);
            var length = _stringSer.Serialize(s, stream);
            var current = stream.Position;
            stream.Position = start;
            BigEndianConverter.Write(stream, (short)length);
            stream.Position = current;
        }

        public static ReusableMemoryStream WriteMessageLength(ReusableMemoryStream stream)
        {
            stream.Position = 0;
            var len = (int) stream.Length - 4; // -4 because do not count size flag itself
            // write message length to the head
            // TODO: use seek?
            BigEndianConverter.Write(stream, len);
            return stream;
        }

        static long ReserveHeader(ReusableMemoryStream stream)
        {
            stream.Write(Zero32, 0, 4);
            return stream.Position;
        }

        static void WriteHeader(ReusableMemoryStream stream, long initPos)
        {
            var pos = stream.Position;
            var size = pos - initPos;
            stream.Position = initPos - 4;
            BigEndianConverter.Write(stream, (int)size);
            stream.Position = pos;
        }

        public static void WriteSizeInBytes(ReusableMemoryStream stream, Action<ReusableMemoryStream> write)
        {
            var initPos = ReserveHeader(stream);
            write(stream);
            WriteHeader(stream, initPos);
        }

        public static void WriteSizeInBytes<T>(ReusableMemoryStream stream, T t, Action<ReusableMemoryStream, T> write)
        {
            var initPos = ReserveHeader(stream);
            write(stream, t);
            WriteHeader(stream, initPos);
        }

        public static void WriteSizeInBytes<T, U>(ReusableMemoryStream stream, T t, U u, Action<ReusableMemoryStream, T, U> write)
        {
            var initPos = ReserveHeader(stream);
            write(stream, t, u);
            WriteHeader(stream, initPos);
        }

        // To avoid dynamically allocated closure
        private static void WriteMemoryStreamSerializable<T>(ReusableMemoryStream stream, T item, object extra)
            where T : IMemoryStreamSerializable
        {
            item.Serialize(stream, extra);
        }

        public static void WriteArray<T>(ReusableMemoryStream stream, IEnumerable<T> items) where T : IMemoryStreamSerializable
        {
            WriteArray(stream, items, null, WriteMemoryStreamSerializable);
        }

        public static void WriteArray<T>(ReusableMemoryStream stream, IEnumerable<T> items, object extra) where T : IMemoryStreamSerializable
        {
            WriteArray(stream, items, extra, WriteMemoryStreamSerializable);
        }

        private static long WriteArrayHeader(ReusableMemoryStream stream)
        {
            var sizePosition = stream.Position;
            stream.Write(MinusOne32, 0, 4); // placeholder for count field
            return sizePosition;
        }

        private static void WriteArraySize(ReusableMemoryStream stream, long sizePosition, int count)
        {
            var pos = stream.Position; // update count field
            stream.Position = sizePosition;
            BigEndianConverter.Write(stream, count);
            stream.Position = pos;
        }

        public static void WriteArray<T>(ReusableMemoryStream stream, IEnumerable<T> items, Action<ReusableMemoryStream, T> write)
        {
            var sizePosition = WriteArrayHeader(stream);
            var count = 0;
            foreach (var item in items)
            {
                write(stream, item);
                ++count;
            }
            WriteArraySize(stream, sizePosition, count);
        }

        public static void WriteArray<T>(ReusableMemoryStream stream, IEnumerable<T> items, object extra, Action<ReusableMemoryStream, T, object> write)
        {
            var sizePosition = WriteArrayHeader(stream);
            var count = 0;
            foreach (var item in items)
            {
                write(stream, item, extra);
                ++count;
            }
            WriteArraySize(stream, sizePosition, count);
        }

        public static void WriteRequestHeader(ReusableMemoryStream stream, int correlationId, ApiKey requestType, ApiVersion version, byte[] clientId)
        {
            stream.Write(MinusOne32, 0, 4); // reserve space for message size
            BigEndianConverter.Write(stream, (short)requestType);
            stream.Write(version.Version, 0, 2);
            BigEndianConverter.Write(stream, correlationId);
            BigEndianConverter.Write(stream, (short)clientId.Length);
            stream.Write(clientId, 0, clientId.Length);
        }

        public static void Update(ReusableMemoryStream stream, long pos, byte[] buff)
        {
            var currPos = stream.Position;
            stream.Position = pos;
            stream.Write(buff, 0, buff.Length);
            stream.Position = currPos;
        }

        public static TData[] DeserializeArray<TData>(ReusableMemoryStream stream)
            where TData : IMemoryStreamSerializable, new()
        {
            return DeserializeArrayExtra<TData>(stream, null);
        }

        public static TData[] DeserializeArrayExtra<TData>(ReusableMemoryStream stream, object extra) where TData : IMemoryStreamSerializable, new()
        {
            var count = BigEndianConverter.ReadInt32(stream);
            var array = new TData[count];
            for (int i = 0; i < count; ++i)
            {
                array[i] = new TData();
                array[i].Deserialize(stream, extra);
            }
            return array;
        }

        public static TData[] DeserializeArray<TData>(ReusableMemoryStream stream, Func<ReusableMemoryStream, TData> dataDeserializer)
        {
            var count = BigEndianConverter.ReadInt32(stream);
            var array = new TData[count];
            for (int i = 0; i < count; ++i)
            {
                array[i] = dataDeserializer(stream);
            }
            return array;
        }

        public static object DeserializeByteArray(ReusableMemoryStream stream, IDeserializer deserializer)
        {
            var len = BigEndianConverter.ReadInt32(stream);
            if (len == -1)
                return null;
            return deserializer.Deserialize(stream, len);
        }
    }
}
