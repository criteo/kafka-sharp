// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

using Kafka.Common;
using Kafka.Public;

namespace Kafka.Cluster
{
    /// <summary>
    /// A utility class used to configure and manage internally used object pools.
    /// </summary>
    class Pools
    {
        private readonly IStatistics _stats;

        public Pools(IStatistics stats)
        {
            _stats = stats;
        }

        public Pool<byte[]> SocketBuffersPool { get; private set; }

        public void InitSocketBuffersPool(int buffersLength)
        {
            SocketBuffersPool = new Pool<byte[]>(
                () =>
                {
                    _stats.UpdateSocketBuffers(1);
                    return new byte[buffersLength];
                }, (s, b) => { });
        }

        public Pool<ReusableMemoryStream> MessageBuffersPool { get; private set; }

        public void InitMessageBuffersPool(int limit)
        {
            MessageBuffersPool = new Pool<ReusableMemoryStream>(
                limit,
                () =>
                {
                    _stats.UpdateMessageBuffers(1);
                    return new ReusableMemoryStream(MessageBuffersPool);
                },
                (b, reused) =>
                {
                    if (!reused)
                    {
                        _stats.UpdateMessageBuffers(-1);
                    }
                    else
                    {
                        b.SetLength(0);
                    }
                });
        }

        public Pool<ReusableMemoryStream> RequestsBuffersPool { get; private set; }

        public void InitRequestsBuffersPool()
        {
            RequestsBuffersPool = new Pool<ReusableMemoryStream>(
                () =>
                {
                    _stats.UpdateRequestsBuffers(1);
                    return new ReusableMemoryStream(RequestsBuffersPool);
                },
                (b, reused) =>
                {
                    if (!reused)
                    {
                        _stats.UpdateRequestsBuffers(-1);
                    }
                    else
                    {
                        b.SetLength(0);
                    }
                });
        }
    }
}