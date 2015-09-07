using System;
using Kafka.Public;

namespace Kafka.Protocol
{
    class ProtocolException : Exception
    {
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
        public UnsupportedMagicByteVersion(byte badMagic)
            : base(string.Format("Unsupported magic byte version: {0}, only 0 is supported", badMagic))
        {
        }
    }
}