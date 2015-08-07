using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace Kafka.Common
{
    class ActionBlockTaskScheduler : TaskScheduler
    {
        private readonly int _dop;
        private readonly ActionBlock<Action> _pool;
#if DEBUG
        private readonly HashSet<Task> _tasks = new HashSet<Task>();
#endif

        public ActionBlockTaskScheduler(int dop)
        {
            _pool = new ActionBlock<Action>(a => a(), new ExecutionDataflowBlockOptions {MaxDegreeOfParallelism = dop});
            _dop = dop;
        }

        protected override IEnumerable<Task> GetScheduledTasks()
        {
#if DEBUG
            bool lockTaken = false;
            try
            {
                Monitor.TryEnter(_tasks, ref lockTaken);
                return _tasks.ToArray();
            }
            finally
            {
                if (lockTaken)
                    Monitor.Exit(_tasks);
            }
#else
            yield break;
#endif
        }

        protected override void QueueTask(Task task)
        {
#if DEBUG
            lock (_tasks)
            {
                _tasks.Add(task);
            }
            _pool.Post(() =>
                {
                    lock (_tasks)
                    {
                        _tasks.Remove(task);
                    }
                    TryExecuteTask(task);
                });
#else
            _pool.Post(() => TryExecuteTask(task));
#endif
        }

        protected override bool TryExecuteTaskInline(Task task, bool taskWasPreviouslyQueued)
        {
            return !taskWasPreviouslyQueued && TryExecuteTask(task);
        }

        public override int MaximumConcurrencyLevel
        {
            get { return _dop; }
        }
    }
}
