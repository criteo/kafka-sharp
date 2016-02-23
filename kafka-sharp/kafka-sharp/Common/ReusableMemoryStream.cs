// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

using System;
using System.IO;
using System.Threading;
using Kafka.Public;

namespace Kafka.Common
{
    /// <summary>
    /// Implement a pool of MemoryStream allowing for recycling
    /// of underlying buffers. This kills two birds with one stone:
    /// we can minimize MemoryStream/buffers creation when (de)serialing requests/responses
    /// and we can minimize the number of buffers passed to the network layers.
    /// </summary>
    class ReusableMemoryStream : MemoryStream, IMemorySerializable, IDisposable
    {
        private static int _nextId;
        private readonly int _id; // Useful to track leaks while debugging
        private readonly Pool<ReusableMemoryStream> _myPool;

        public ReusableMemoryStream(Pool<ReusableMemoryStream> myPool)
        {
            _id = Interlocked.Increment(ref _nextId);
            _myPool = myPool;
        }

        public Pool<ReusableMemoryStream> Pool
        {
            get
            {
                return _myPool;
            }
        }

        internal byte this[int index]
        {
            get { return GetBuffer()[index]; }
            set { GetBuffer()[index] = value; }
        }

        public new void Dispose()
        {
            (this as IDisposable).Dispose();
        }

        void IDisposable.Dispose()
        {
            if (_myPool != null)
            {
                _myPool.Release(this);
            }
        }

        public void Serialize(MemoryStream toStream)
        {
            byte[] array = GetBuffer();
            int length = (int) Length;
            toStream.Write(array, 0, length);
        }
    }

    static class ReusableExtentions
    {
        // CopyTo allocates a temporary buffer. As we're already pooling buffers,
        // we might as well provide a CopyTo that makes use of that instead of
        // using Stream.CopyTo (which allocates a buffer of its own, even if it
        // does so efficiently).
        public static void ReusableCopyTo(this Stream input, Stream destination, ReusableMemoryStream tmpBuffer)
        {
            tmpBuffer.SetLength(81920);
            var buffer = tmpBuffer.GetBuffer();
            int read;
            while ((read = input.Read(buffer, 0, buffer.Length)) != 0)
            {
                destination.Write(buffer, 0, read);
            }
        }
    }
}