#if NET_CORE
using System;
using System.IO;

namespace Kafka.Common
{
    internal static class MemoryStreamExtensions
    {
        public static byte[] GetBuffer(this MemoryStream memoryStream)
        {
            ArraySegment<byte> arraySegment;
            if (!memoryStream.TryGetBuffer(out arraySegment))
            {
                // Shouldn't happen since we are never constructing a memory stream with "publiclyVisible" set to false
                throw new NotSupportedException();
            }

            // Don't care about the offset since we are never providing an array and specifying an origin index != 0
            return arraySegment.Array;
        }
    }
}
#endif