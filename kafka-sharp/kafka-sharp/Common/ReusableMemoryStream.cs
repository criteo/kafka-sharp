// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

using System;
using System.IO;
using System.Threading;

namespace Kafka.Common
{
    /// <summary>
    /// Implement a pool of MemoryStream allowing for recycling
    /// of underlying buffers. This kills two birds with one stone:
    /// we can minimize MemoryStream/buffers creation when (de)serialing requests/responses
    /// and we can minimize the number of buffers passed to the network layers.
    ///
    /// TODO: (for all pools) check that we do not keep too many objects in the pool
    /// </summary>
    class ReusableMemoryStream : MemoryStream, IDisposable
    {
        //private static readonly ConcurrentQueue<ReusableMemoryStream> _pool =
        //    new ConcurrentQueue<ReusableMemoryStream>();
        private static readonly Pool<ReusableMemoryStream> _pool = new Pool<ReusableMemoryStream>(() =>  new ReusableMemoryStream(), s => s.SetLength(0));

        private static int _nextId;
        private readonly int _id; // Useful to track leaks while debugging

        private ReusableMemoryStream()
        {
            _id = Interlocked.Increment(ref _nextId);
        }

        public static ReusableMemoryStream Reserve()
        {
            return _pool.Reserve();
        }

        public static ReusableMemoryStream Reserve(int capacity)
        {
            return Reserve().EnsureCapacity(capacity);
        }

        private static void Release(ReusableMemoryStream stream)
        {
            _pool.Release(stream);
        }

        public byte this[int index]
        {
            get { return GetBuffer()[index]; }
        }

        public new void Dispose()
        {
            (this as IDisposable).Dispose();
        }

        void IDisposable.Dispose()
        {
            Release(this);
        }

        private ReusableMemoryStream EnsureCapacity(int capacity)
        {
            int position = (int) Position;
            SetLength(capacity);
            Position = position;
            return this;
        }
    }

    static class ReusableExtentions
    {
        // CopyTo allocates a temporary buffer. As we're already pooling buffers,
        // we might as well provide a CopyTo that makes use of that instead of
        // using Stream.CopyTo (which allocates a buffer of its own, even if it
        // does so efficiently).
        public static void ReusableCopyTo(this Stream input, Stream destination)
        {
            using (var m = ReusableMemoryStream.Reserve(128*1024))
            {
                var buffer = m.GetBuffer();
                int read;
                while ((read = input.Read(buffer, 0, buffer.Length)) != 0)
                {
                    destination.Write(buffer, 0, read);
                }
            }
        }
    }
}