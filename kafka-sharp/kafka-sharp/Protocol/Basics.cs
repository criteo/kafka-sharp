// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Runtime.Serialization;
using Kafka.Common;
using Kafka.Public;
#if !NETSTANDARD1_3
using Snappy;
#endif

namespace Kafka.Protocol
{
    // Utility interface to facilitate code factorization.
    // This is not intended to be beautiful.
    internal interface IMemoryStreamSerializable
    {
        void Serialize(ReusableMemoryStream stream, object extra, Basics.ApiVersion version);
        void Deserialize(ReusableMemoryStream stream, object extra, Basics.ApiVersion version);
    }

    static class Basics
    {
        // Value representing the case when the size of an object is unknown (when calling a SizeOf*() method).
        // This is kind of ugly but the alternative is to throw an exception, which is very expensive.
        public static readonly long UnknownSize = long.MinValue;

        public static readonly byte[] MinusOne32 = { 0xff, 0xff, 0xff, 0xff };
        public static readonly byte[] MinusOne16 = { 0xff, 0xff };
        public static readonly byte[] MinusOneVarInt = { 0x01 };
        public static readonly byte[] Zero64 = { 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 };
        public static readonly byte[] Zero32 = { 0x00, 0x00, 0x00, 0x00 };
        public static readonly byte[] ZeroVarInt = { 0x00 };

        public static readonly byte[][] VersionBytes =
        {
            new byte[]{ 0x00, 0x00 },
            new byte[]{ 0x00, 0x01 },
            new byte[]{ 0x00, 0x02 },
            new byte[]{ 0x00, 0x03 },
            new byte[]{ 0x00, 0x04 },
            new byte[]{ 0x00, 0x05 },
            new byte[]{ 0x00, 0x06 },
        };

        public enum ApiVersion
        {
            Ignored = -1,
            V0 = 0,
            V1 = 1,
            V2 = 2,
            V3 = 3,
            V4 = 4,
            V5 = 5,
            V6 = 6,
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
            SaslHandshake = 17,
            ApiVersions = 18,
        }

        public enum IsolationLevel : byte
        {
            ReadUncommited = 0,
            ReadCommited = 1,
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

        public static byte[] DeserializeBytesWithVarIntSize(ReusableMemoryStream stream)
        {
            var len = VarIntConverter.ReadInt32(stream);
            if (len == -1)
                return null;

            var buffer = new byte[len];
            stream.Read(buffer, 0, len);
            return buffer;
        }
        public static void SerializeBytesWithVarIntSize(ReusableMemoryStream stream, byte[] b)
        {
            if (b == null)
            {
                stream.Write(MinusOneVarInt, 0, MinusOneVarInt.Length);
                return;
            }

            VarIntConverter.Write(stream, b.Length);
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

        public static string DeserializeStringWithVarIntSize(ReusableMemoryStream stream)
        {
            var len = VarIntConverter.ReadInt32(stream);
            // per contract, null string is represented with -1 len.
            if (len == -1)
                return null;

            return _stringDeser.Deserialize(stream, len) as string;
        }

        public static void SerializeStringWithVarIntSize(ReusableMemoryStream stream, string s)
        {
            if (s == null)
            {
                stream.Write(MinusOneVarInt, 0, MinusOneVarInt.Length);
                return;
            }

            VarIntConverter.Write(stream, _stringSer.SerializedSize(s));
            _stringSer.Serialize(s, stream);
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
        private static void WriteMemoryStreamSerializable<T>(ReusableMemoryStream stream, T item, object extra, ApiVersion version)
            where T : IMemoryStreamSerializable
        {
            item.Serialize(stream, extra, version);
        }

        public static void WriteArray<T>(ReusableMemoryStream stream, IEnumerable<T> items) where T : IMemoryStreamSerializable
        {
            WriteArray(stream, items, null, ApiVersion.Ignored, WriteMemoryStreamSerializable);
        }

        public static void WriteArray<T>(ReusableMemoryStream stream, IEnumerable<T> items, ApiVersion version) where T : IMemoryStreamSerializable
        {
            WriteArray(stream, items, null, version, WriteMemoryStreamSerializable);
        }

        public static void WriteArray<T>(ReusableMemoryStream stream, IEnumerable<T> items, object extra, ApiVersion version) where T : IMemoryStreamSerializable
        {
            WriteArray(stream, items, extra, version, WriteMemoryStreamSerializable);
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

        public static void WriteArray<T>(ReusableMemoryStream stream, IEnumerable<T> items, object extra, ApiVersion version, Action<ReusableMemoryStream, T, object, ApiVersion> write)
        {
            var sizePosition = WriteArrayHeader(stream);
            var count = 0;
            foreach (var item in items)
            {
                write(stream, item, extra, version);
                ++count;
            }
            WriteArraySize(stream, sizePosition, count);
        }

        public static void WriteRequestHeader(ReusableMemoryStream stream, int correlationId, ApiKey requestType, ApiVersion version, byte[] clientId)
        {
            stream.Write(MinusOne32, 0, 4); // reserve space for message size
            BigEndianConverter.Write(stream, (short)requestType);
            stream.Write(VersionBytes[(int)version], 0, 2);
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
            return DeserializeArrayExtra<TData>(stream, null, ApiVersion.Ignored);
        }

        public static TData[] DeserializeArray<TData>(ReusableMemoryStream stream, ApiVersion version)
            where TData : IMemoryStreamSerializable, new()
        {
            return DeserializeArrayExtra<TData>(stream, null, version);
        }

        public static TData[] DeserializeArrayExtra<TData>(ReusableMemoryStream stream, object extra, ApiVersion version) where TData : IMemoryStreamSerializable, new()
        {
            var count = BigEndianConverter.ReadInt32(stream);
            if (count == -1) return new TData[0];
            var array = new TData[count];
            for (int i = 0; i < count; ++i)
            {
                array[i] = new TData();
                array[i].Deserialize(stream, extra, version);
            }
            return array;
        }

        public static TData[] DeserializeArray<TData>(ReusableMemoryStream stream, Func<ReusableMemoryStream, TData> dataDeserializer)
        {
            var count = BigEndianConverter.ReadInt32(stream);
            if (count == -1) return new TData[0];
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

        /// <summary>
        /// Write the size of the serialized object as a VarInt then the serialized object on the stream.
        ///
        /// </summary>
        /// <param name="stream">target stream</param>
        /// <param name="o">object to serialize and write to target</param>
        /// <param name="serializer">serializer to use if object is not serializable</param>
        public static void WriteObject(ReusableMemoryStream stream, object o, ISerializer serializer)
        {
            if (o is byte[] asBytes)
            {
                SerializeBytesWithVarIntSize(stream, asBytes);
            }
            else if (o is string asString)
            {
                SerializeStringWithVarIntSize(stream, asString);
            }
            else
            {
                if (o == null)
                {
                    stream.Write(MinusOneVarInt, 0, MinusOneVarInt.Length);
                    return;
                }

                var expectedPosition = -1L;
                if (o is ISizedMemorySerializable asSizedSerializable)
                {
                    var expectedSize = asSizedSerializable.SerializedSize();
                    VarIntConverter.Write(stream, expectedSize);
                    expectedPosition = stream.Position + expectedSize;
                    asSizedSerializable.Serialize(stream);
                }
                else if (serializer is ISizableSerializer sizableSerializer)
                {
                    var expectedSize = sizableSerializer.SerializedSize(o);
                    VarIntConverter.Write(stream, expectedSize);
                    expectedPosition = stream.Position + expectedSize;
                    sizableSerializer.Serialize(o, stream);
                }
                else
                {
                    // If we can not know the size of the serialized object in advance, we need to use an intermediate buffer.
                    using (var buffer = stream.Pool.Reserve())
                    {
                        if (o is IMemorySerializable asSerializable)
                        {
                            asSerializable.Serialize(buffer);
                        }
                        else
                        {
                            serializer.Serialize(o, buffer);
                        }
                        WriteObject(stream, buffer, null);
                    }

                    return;
                }

                if (expectedPosition != stream.Position)
                {
                    throw new SerializationException(
                        "SerializedSize() returned a different value than the size of the serialized object written");
                }
            }
        }

        /// <summary>
        /// Return the size of the string in bytes when serialized with <see cref="SerializeString" />.
        ///
        /// While the method returns a long, the size of a string is actually bounded by an integer size.
        /// </summary>
        /// <param name="s">string to be serialized</param>
        /// <returns>size in bytes</returns>
        public static long SizeOfSerializedString(string s)
        {
            if (s == null)
            {
                return -1;
            }
            return _stringSer.SerializedSize(s);
        }

        /// <summary>
        /// Returns the size in bytes of the object when serialized with <see cref="WriteObject" />.
        /// </summary>
        /// <param name="o">object to be serialized</param>
        /// <param name="serializer">optional serializer used if th object is not serializable</param>
        /// <returns></returns>
        public static long SizeOfSerializedObject(object o, ISizableSerializer serializer)
        {
            if (o == null)
            {
                return -1;
            }

            if (o is byte[] asBytes)
            {
                return asBytes.Length;
            }

            if (o is string asString)
            {
                return SizeOfSerializedString(asString);
            }

            if (o is ISizedMemorySerializable asSizedSerializable)
            {
                return asSizedSerializable.SerializedSize();
            }

            return serializer?.SerializedSize(o) ?? UnknownSize;
        }

        #region Compression utility

        /// <summary>
        /// Compress a given stream using a given compression codec
        /// </summary>
        /// <param name="uncompressedStream"> The initial stream we want to compress</param>
        /// <param name="compressedStream"> The stream that want to put the compressed data in (should be empty before calling the method).</param>
        /// <param name="compression"> The compression we want to use.</param>
        /// <returns></returns>
        internal static void CompressStream(ReusableMemoryStream uncompressedStream,
            ReusableMemoryStream compressedStream, CompressionCodec compression)
        {
            if (compression == CompressionCodec.None)
            {
                throw new ArgumentException("Compress a stream only when you want compression.");
            }

            switch (compression)
            {
                case CompressionCodec.Gzip:
                    using (var gzip = new GZipStream(compressedStream, CompressionMode.Compress, true))
                    {
                        uncompressedStream.WriteTo(gzip);
                    }

                    break;

                case CompressionCodec.Lz4:
                    KafkaLz4.Compress(compressedStream, uncompressedStream.GetBuffer(),
                        (int) uncompressedStream.Length);
                    break;

                case CompressionCodec.Snappy:
#if NETSTANDARD1_3
                                throw new NotImplementedException();
#else
                    compressedStream.SetLength(SnappyCodec.GetMaxCompressedLength((int) uncompressedStream.Length));
                {
                    int size = SnappyCodec.Compress(uncompressedStream.GetBuffer(), 0, (int) uncompressedStream.Length,
                        compressedStream.GetBuffer(), 0);
                    compressedStream.SetLength(size);
                }
#endif
                    break;
            }
        }

        /// <summary>
        /// Uncompress a byte array into a stream.
        /// </summary>
        /// <param name="uncompressed">The stream that will contain the uncompressed data (must be given empty).</param>
        /// <param name="body">The byte array that we want to uncompress.</param>
        /// <param name="offset">The index in the body where the data we want to uncompress starts.</param>
        /// <param name="length">The length of the data in the body that we want to uncompress.</param>
        /// <param name="codec">The compression codec that was used to compress the data in the body.</param>
        internal static void Uncompress(ReusableMemoryStream uncompressed, byte[] body, int offset, int length, CompressionCodec codec)
        {
            try
            {
                if (codec == CompressionCodec.Snappy)
                {
#if NETSTANDARD1_3
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
    }
}
