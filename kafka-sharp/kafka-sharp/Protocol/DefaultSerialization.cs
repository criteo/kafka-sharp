// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

using System;
using System.IO;
using Kafka.Public;

namespace Kafka.Protocol
{
    /// <summary>
    /// A default message serializer/deserializer that assumes the
    /// serialized objects are byte arrays.
    /// </summary>
    sealed class ByteArraySerialization : ISerializer, IDeserializer
    {
        public int Serialize(object input, MemoryStream toStream)
        {
            var toSerialize = input as byte[];
            if (toSerialize == null)
            {
                throw new ArgumentException("Input cannot be converted to byte[]", "input");
            }

            toStream.Write(toSerialize, 0, toSerialize.Length);
            return toSerialize.Length;
        }

        public object Deserialize(MemoryStream fromStream, int length)
        {
            var output = new byte[length];
            fromStream.Read(output, 0, length);
            return output;
        }

        // Not constructible
        private ByteArraySerialization()
        {
        }

        /// <summary>
        /// The only one instance of this type.
        /// </summary>
        public static readonly ISerializer DefaultSerializer = new ByteArraySerialization();
        public static readonly IDeserializer DefaultDeserializer = DefaultSerializer as IDeserializer;
    }
}