using System;
using Kafka.Public;

namespace Kafka.Protocol
{
    class ProtocolException : Exception
    {
        // Fields to identify the faulty message (useful in case the exception stops a batch)
        internal string Topic;
        internal int Partition;

        public ProtocolException(string message) : base(message)
        {
        }

        public ProtocolException(string message, Exception innerException) : base(message, innerException)
        {
        }
    }

    // Crc mismatch
    class CrcException : ProtocolException
    {
        public CrcException(string message)
            : base(message)
        {
        }
    }

    // Wrong compressed data
    class UncompressException : ProtocolException
    {
        public CompressionCodec Codec { get; internal set; }

        public UncompressException(string message, CompressionCodec codec, Exception ex)
            : base(message, ex)
        {
            Codec = codec;
        }
    }

    // Wrong message version
    class UnsupportedMagicByteVersion : ProtocolException
    {
        public UnsupportedMagicByteVersion(byte badMagic, string supported)
            : base($"Unsupported magic byte version: {badMagic}, only {supported} is supported")
        {
        }
    }
}