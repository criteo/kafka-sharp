// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Kafka.Common;
using Kafka.Protocol;

namespace Kafka.Public
{
    using Serializers = Tuple<ISerializer, ISerializer>;
    using Deserializers = Tuple<IDeserializer, IDeserializer>;

    /// <summary>
    /// Interface to custom serialization.
    /// Implementation of this class must be threadsafe.
    /// </summary>
    public interface ISerializer
    {
        /// <summary>
        /// Serialize an object to a stream of bytes.
        /// </summary>
        /// <param name="input">The object to serialize.</param>
        /// <param name="toStream">The stream to write to. We limit output to memory stream
        /// so that various optimizations are possible when implementing the interface.</param>
        /// <returns>The number of bytes written to the stream.</returns>
        int Serialize(object input, MemoryStream toStream);
    }

    public interface ISizableSerializer : ISerializer
    {
        long SerializedSize(object input);
    }

    /// <summary>
    /// Interface to custom deserialization.
    /// Implementation of this class must be threadsafe.
    /// </summary>
    public interface IDeserializer
    {
        /// <summary>
        /// Deserialize an object from a stream of bytes.
        /// </summary>
        /// <param name="fromStream">The stream to read from. Knowing it is a memory stream
        /// allows for various optimizations when implementing the interface.</param>
        /// <param name="length">The length of the serialized data in the stream.</param>
        /// <returns>A deserialized object.</returns>
        object Deserialize(MemoryStream fromStream, int length);
    }

    /// <summary>
    /// This defines an interface for object serializable to memory.
    /// If your data Key or Value class implements this interface,
    /// then it will always be used for serialization by the Producer part of
    /// the driver.
    /// This is an alternative to providing an ISerializer for a topic.
    /// </summary>
    public interface IMemorySerializable
    {
        void Serialize(MemoryStream toStream);
    }

    /// <summary>
    /// This defines an interface for object serializable to memory, where the
    /// size of the serialized objects can be known in advance.
    ///
    /// Knowing the size of the serialized object ahead of the serialization
    /// avoids the allocation of a temporary buffer in some cases.
    /// </summary>
    public interface ISizedMemorySerializable : IMemorySerializable
    {
        long SerializedSize();
    }

    /// <summary>
    /// A class holding information on serializer/deserializer per topic.
    /// This class is not threadsafe: don't set from multiple threads at  the same time.
    /// </summary>
    public sealed class SerializationConfig
    {
        private readonly Dictionary<string, Serializers> _serializers;
        private readonly Dictionary<string, Deserializers> _deserializers;
        private Serializers _defaultSerializers = ByteArraySerializers;
        private Deserializers _defaultDeserializers = ByteArrayDeserializers;

        public SerializationConfig()
        {
            _serializers = new Dictionary<string, Serializers>();
            _deserializers = new Dictionary<string, Deserializers>();
        }

        internal SerializationConfig(SerializationConfig config)
        {
            _serializers = new Dictionary<string, Serializers>(config._serializers);
            _deserializers = new Dictionary<string, Deserializers>(config._deserializers);
            SerializeOnProduce = config.SerializeOnProduce;
        }

        /// <summary>
        /// If true, the producer will serialize messages keys/values before routing a
        /// message. Original data will not be kept in memory while buffering messages
        /// for a batch. This helps decreasing the pressure on the garbage collector
        /// by avoiding sending original data to Gen 1 or 2.
        /// As a consequence, in case of unrecoverable error the original object will not
        /// be reported back to the client.
        ///
        /// If you require high performances (you plan for very high throughput),
        /// or plan to set big windows and wants to minimize GC for batching you sould
        /// probably set this to true.
        /// </summary>
        public bool SerializeOnProduce = false;

        /// <summary>
        /// Maximum size of kept memory chunks in the message pool. This is only effective
        /// while using SerializeOnProduce = true. It means memory used for serializing messages
        /// bigger than this value won't be retained in the underlying memory pool. It helps protect
        /// against topic that most of the time have small messages and sometimes have very big messages.
        /// </summary>
        public int MaxMessagePoolChunkSize = 16384;

        /// <summary>
        /// Maximum number of messages kept in the pool. This is only effective
        /// while using SerializeOnProduce = true.
        /// </summary>
        public int MaxPooledMessages = 10000;

        public void SetDefaultSerializers(ISerializer keySerializer, ISerializer valueSerializer)
        {
            _defaultSerializers = Tuple.Create(keySerializer, valueSerializer);
        }

        public void SetDefaultDeserializers(IDeserializer keyDeserializer, IDeserializer valueDeserializer)
        {
            _defaultDeserializers = Tuple.Create(keyDeserializer, valueDeserializer);
        }

        public void SetSerializersForTopic(string topic, ISerializer keySerializer, ISerializer valueSerializer)
        {
            _serializers[topic] = Tuple.Create(keySerializer, valueSerializer);
        }

        public void SetDeserializersForTopic(string topic, IDeserializer keyDeserializer, IDeserializer valueDeserializer)
        {
            _deserializers[topic] = Tuple.Create(keyDeserializer, valueDeserializer);
        }

        internal Serializers GetSerializersForTopic(string topic)
        {
            Serializers output;
            if (!_serializers.TryGetValue(topic, out output))
            {
                output = _defaultSerializers;
            }
            else
            {
                if (output.Item1 == null)
                    output = Tuple.Create(ByteArraySerialization.DefaultSerializer, output.Item2);
                if (output.Item2 == null)
                    output = Tuple.Create(output.Item1, ByteArraySerialization.DefaultSerializer);
            }
            return output;
        }

        internal Deserializers GetDeserializersForTopic(string topic)
        {
            Deserializers output;
            if (!_deserializers.TryGetValue(topic, out output))
            {
                output = _defaultDeserializers;
            }
            else
            {
                if (output.Item1 == null)
                    output = Tuple.Create(ByteArraySerialization.DefaultDeserializer, output.Item2);
                if (output.Item2 == null)
                    output = Tuple.Create(output.Item1, ByteArraySerialization.DefaultDeserializer);
            }
            return output;
        }

        internal static readonly Serializers ByteArraySerializers = Tuple.Create(ByteArraySerialization.DefaultSerializer, ByteArraySerialization.DefaultSerializer);
        internal static readonly Deserializers ByteArrayDeserializers = Tuple.Create(ByteArraySerialization.DefaultDeserializer, ByteArraySerialization.DefaultDeserializer);
    }

    /// <summary>
    /// A serializer for strings.
    /// </summary>
    public sealed class StringSerializer : ISizableSerializer
    {
        private readonly Encoding _encoding;

        /// <summary>
        /// Create a string serializer using the provided encoding.
        /// </summary>
        /// <param name="encoding">The target encoding</param>
        public StringSerializer(Encoding encoding)
        {
            _encoding = encoding;
        }

        /// <summary>
        /// Create a string serializer using UTF8 encoding.
        /// </summary>
        public StringSerializer()
            : this(Encoding.UTF8)
        {
        }

        public int Serialize(object input, MemoryStream toStream)
        {
            var data = input as string;
            if (data == null)
                throw new ArgumentException("Non string argument", "input");

            var position = toStream.Position;
            var length = _encoding.GetByteCount(data);

            // If toStream.Position > int.MaxValue - length we're screwed but it means you're probably
            // doing something very wrong (like sending huge amount of data per request to Kafka).
            toStream.SetLength(toStream.Length + length);
            _encoding.GetBytes(data, 0, data.Length, toStream.GetBuffer(), (int) position);
            toStream.Position = position + length;
            return length;
        }

        public long SerializedSize(object input)
        {
            var data = input as string;
            if (data == null)
                throw new ArgumentException("Non string argument", nameof(input));

            return _encoding.GetByteCount(data);
        }
    }

    /// <summary>
    /// A deserializer for strings.
    /// </summary>
    public sealed class StringDeserializer : IDeserializer
    {
        private readonly Encoding _encoding;

        /// <summary>
        /// Create a string deserializer using the provided encoding.
        /// </summary>
        /// <param name="encoding">The target encoding</param>
        public StringDeserializer(Encoding encoding)
        {
            _encoding = encoding;
        }

        /// <summary>
        /// Create a string deserializer using UTF8 encoding.
        /// </summary>
        public StringDeserializer()
            : this(Encoding.UTF8)
        {
        }

        public object Deserialize(MemoryStream fromStream, int length)
        {
            // If fromStream.Position > int.MaxValue - length we're screwed but it means you're probably
            // doing something very wrong (like fetching huge amount of data per request from Kafka).
            var output = _encoding.GetString(fromStream.GetBuffer(), (int)fromStream.Position, length);
            fromStream.Position += length;
            return output;
        }
    }
}