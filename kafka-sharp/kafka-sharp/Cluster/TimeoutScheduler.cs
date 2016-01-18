// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

using System;
using System.Collections.Generic;
using System.Threading;

namespace Kafka.Cluster
{
    /// <summary>
    /// Handle timeout checks for nodes.
    /// </summary>
    class TimeoutScheduler : IDisposable
    {
        private readonly Dictionary<INode, Action> _checkers = new Dictionary<INode, Action>();
        private readonly Timer _timer;

        // Scheduler that does nothing, useful for tests
        public TimeoutScheduler()
        {
        }

        /// <summary>
        /// Scheduler that checks timeout every periodMs milliseconds.
        /// </summary>
        /// <param name="periodMs"></param>
        public TimeoutScheduler(int periodMs)
        {
            _timer = new Timer(_ => Check(), null, periodMs, periodMs);
        }

        public void Register(INode node, Action checker)
        {
            lock (_checkers)
            {
                _checkers.Add(node, checker);
            }
        }

        public void Unregister(INode node)
        {
            lock (_checkers)
            {
                _checkers.Remove(node);
            }
        }

        public void Check()
        {
            lock (_checkers)
            {
                foreach (var checker in _checkers.Values)
                {
                    checker();
                }
            }
        }

        public void Dispose()
        {
            if (_timer != null)
            {
                _timer.Dispose();
            }
        }
    }
}