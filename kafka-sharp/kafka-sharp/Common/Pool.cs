// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

using System;
using System.Collections.Concurrent;
using System.Threading;

namespace Kafka.Common
{
    /// <summary>
    /// Manage a pool of T.
    /// </summary>
    internal class Pool<T> where T : class
    {
        private readonly ConcurrentQueue<T> _pool = new ConcurrentQueue<T>();
        private readonly Func<T> _constructor;
        private readonly Action<T, bool> _clearAction;
        private readonly int _limit;
        private int _watermark;

        public int Watermark
        {
            get { return _watermark; }
        }

        public Pool(Func<T> constructor, Action<T, bool> clearAction) : this(0, constructor, clearAction)
        {
        }

        public Pool(int limit, Func<T> constructor, Action<T, bool> clearAction)
        {
            if (constructor == null)
            {
                throw new ArgumentNullException("constructor");
            }
            if (clearAction == null)
            {
                throw new ArgumentNullException("clearAction");
            }
            _limit = limit;
            _constructor = constructor;
            _clearAction = clearAction;
        }

        public T Reserve()
        {
            T item;
            if (!_pool.TryDequeue(out item))
            {
                return _constructor();
            }

            Interlocked.Decrement(ref _watermark);
            return item;
        }

        public void Release(T item)
        {
            if (item == null) return;
            if (_limit > 0 && Interlocked.Increment(ref _watermark) > _limit)
            {
                _clearAction(item, false);
                Interlocked.Decrement(ref _watermark);
            }
            else
            {
                _clearAction(item, true);
                _pool.Enqueue(item);
            }
        }
    }
}