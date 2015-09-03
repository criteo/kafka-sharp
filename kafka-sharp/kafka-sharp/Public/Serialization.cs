// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
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
    /// A class holding information on serializer/deserializer per topic.
    /// This class is not threadsafe: don't set from multiple threads at  the same time.
    /// </summary>
    public sealed class SerializationConfig
    {
        private readonly Dictionary<string, Serializers> _serializers;
        private readonly Dictionary<string, Deserializers> _deserializers;

        public SerializationConfig()
        {
            _serializers = new Dictionary<string, Serializers>();
            _deserializers = new Dictionary<string, Deserializers>();
        }

        internal SerializationConfig(SerializationConfig config)
        {
            _serializers = new Dictionary<string, Serializers>(config._serializers);
            _deserializers = new Dictionary<string, Deserializers>(config._deserializers);
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
                output = DefaultSerializers;
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
                output = DefaultDeserializers;
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

        internal static readonly Serializers DefaultSerializers = Tuple.Create(ByteArraySerialization.DefaultSerializer, ByteArraySerialization.DefaultSerializer);
        internal static readonly Deserializers DefaultDeserializers = Tuple.Create(ByteArraySerialization.DefaultDeserializer,
            ByteArraySerialization.DefaultDeserializer);
    }

    /// <summary>
    /// A serializer for strings.
    /// </summary>
    public sealed class StringSerializer : ISerializer
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
    }

    /// <summary>
    /// A desrializer for strings.
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
            var output = _encoding.GetString(fromStream.GetBuffer(), (int) fromStream.Position, length);
            fromStream.Position += length;
            return output;
        }
    }
}