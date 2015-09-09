// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Kafka.Common;

namespace Kafka.Batching
{
    class Grouping<TKey, TData> : IGrouping<TKey, TData>, IDisposable
    {
        private static readonly Pool<Grouping<TKey, TData>> _pool = new Pool<Grouping<TKey, TData>>(() => new Grouping<TKey, TData>(), g => g.Clear());

        private readonly List<TData> _data = new List<TData>();

        public void Clear()
        {
            Key = default(TKey);
            _data.Clear();
        }

        public static Grouping<TKey, TData> New(TKey key)
        {
            var g = _pool.Reserve();
            g.Key = key;
            return g;
        }

        public void Add(TData data)
        {
            _data.Add(data);
        }

        private Grouping()
        {
        }

        #region IGrouping<TKey,TData> Members

        public TKey Key { get; private set; }

        #endregion

        #region IEnumerable<TData> Members

        public System.Collections.Generic.IEnumerator<TData> GetEnumerator()
        {
            return _data.GetEnumerator();
        }

        #endregion

        #region IEnumerable Members

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            return _data.GetEnumerator();
        }

        #endregion

        #region IDisposable Members

        public void Dispose()
        {
            _pool.Release(this);
        }

        #endregion
    }

    class Grouping<TKey1, TKey2, TData> : IGrouping<TKey1, IGrouping<TKey2, TData>>, IDisposable
    {
        private static readonly Pool<Grouping<TKey1, TKey2, TData>> _pool = new Pool<Grouping<TKey1, TKey2, TData>>(
            () => new Grouping<TKey1, TKey2, TData>(),
            g => g.Clear());

        private readonly Dictionary<TKey2, Grouping<TKey2, TData>> _key2Groupings = new Dictionary<TKey2, Grouping<TKey2, TData>>();

        public static Grouping<TKey1, TKey2, TData> New(TKey1 key1)
        {
            var g = _pool.Reserve();
            g.Key = key1;
            return g;
        }

        private void Clear()
        {
            Key = default(TKey1);
            foreach (var g in _key2Groupings.Values)
            {
                g.Dispose();
            }
            _key2Groupings.Clear();
        }

        public void Add(TKey2 key2, TData data)
        {
            Grouping<TKey2, TData> key2Grouping;
            if (!_key2Groupings.TryGetValue(key2, out key2Grouping))
            {
                key2Grouping = Grouping<TKey2, TData>.New(key2);
                _key2Groupings[key2] = key2Grouping;
            }
            key2Grouping.Add(data);
        }

        #region IGrouping<TKey1, IGrouping<TKey2, TData>> Members

        public TKey1 Key { get; private set; }

        #endregion

        #region IEnumerable<IGrouping<TKey2, TData>> Members

        public IEnumerator<IGrouping<TKey2, TData>> GetEnumerator()
        {
            return _key2Groupings.Values.GetEnumerator();
        }

        #endregion

        #region IEnumerable Members

        IEnumerator IEnumerable.GetEnumerator()
        {
            return _key2Groupings.Values.GetEnumerator();
        }

        #endregion

        #region IDisposable Members

        public void Dispose()
        {
            _pool.Release(this);
        }

        #endregion
    }
}