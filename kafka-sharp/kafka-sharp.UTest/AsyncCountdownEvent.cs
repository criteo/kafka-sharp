using System.Threading;
using System.Threading.Tasks;


namespace tests_kafka_sharp
{
    public sealed class AsyncCountdownEvent
    {
        private readonly TaskCompletionSource<bool> _tcs;

        private int _count;

        public AsyncCountdownEvent(int count)
        {
            _tcs = new TaskCompletionSource<bool>();
            _count = count;
        }

        public Task WaitAsync()
        {
            return _tcs.Task;
        }

        public void Signal()
        {
            var count = Interlocked.Decrement(ref _count);
            if (count == 0)
                _tcs.SetResult(true);
        }
    }
}
