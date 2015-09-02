using System.Threading;

namespace Kafka.Public
{
    public interface IStatistics
    {
        long SuccessfulSent { get; }

        long RequestSent { get; }

        long ResponseReceived { get; }

        long Errors { get; }

        long NodeDead { get; }

        long Expired { get; }

        long Discarded { get; }

        long Exited { get; }

        long Received { get; }

        void AddToSuccessfulSent(int nb);

        void IncrementRequestSent();

        void IncrementResponseReceived();

        void IncrementErrors();

        void IncrementNodeDead();

        void IncrementExpired();

        void IncrementDiscarded();

        void AddToExited(int nb);

        void IncrementExited();

        void IncrementReceived();
    }


    /// <summary>
    /// Some statistics about the cluster.
    /// </summary>
    public class Statistics : IStatistics
    {
        private long _successfulSent;
        private long _requestSent;
        private long _responseReceived;
        private long _errors;
        private long _nodeDead;
        private long _expired;
        private long _discarded;
        private long _exited;
        private long _received;

        /// <summary>
        /// Number of messages successfully sent (for which we have received an ack)
        /// </summary>
        public long SuccessfulSent { get { return _successfulSent; } }

        /// <summary>
        /// Number of requests sent to the Kafka cluster
        /// </summary>
        public long RequestSent { get { return _requestSent; } }

        /// <summary>
        /// Number of responses received from the Kafka cluster
        /// </summary>
        public long ResponseReceived { get { return _responseReceived; } }

        /// <summary>
        /// Number of hard errors encountered (network errors or decode errors)
        /// </summary>
        public long Errors { get { return _errors; } }

        /// <summary>
        ///  Number of times nodes have been marked dead (not the current number of dead nodes)
        /// </summary>
        public long NodeDead { get { return _nodeDead; } }

        /// <summary>
        /// Number of expired messages.
        /// </summary>
        public long Expired { get { return _expired; } }

        /// <summary>
        /// Number of discarded messages.
        /// </summary>
        public long Discarded { get { return _discarded; } }

        /// <summary>
        /// Number of produce request that have exited the system either successful, discard or expired.
        /// </summary>
        public long Exited { get { return _exited; } }

        /// <summary>
        /// Number of received messages.
        /// </summary>
        public long Received { get { return _received; } }

        public override string ToString()
        {
            return string.Format(
                "{{Messages successfully sent: {0} - Messages received: {8} - Requests sent: {1} - Responses received: {2} - Errors: {3} - Dead nodes: {4} - Expired: {5} - Discarded: {6} - Exit: {7}}}",
                SuccessfulSent, RequestSent, ResponseReceived, Errors, NodeDead, Expired, Discarded, Exited, Received);
        }

        public void AddToSuccessfulSent(int nb)
        {
            Interlocked.Add(ref _successfulSent, nb);
        }

        public void IncrementRequestSent()
        {
            Interlocked.Increment(ref _requestSent);
        }

        public void IncrementResponseReceived()
        {
            Interlocked.Increment(ref _responseReceived);
        }

        public void IncrementErrors()
        {
            Interlocked.Increment(ref _errors);
        }

        public void IncrementNodeDead()
        {
            Interlocked.Increment(ref _nodeDead);
        }

        public void IncrementExpired()
        {
            Interlocked.Increment(ref _expired);
        }

        public void IncrementDiscarded()
        {
            Interlocked.Increment(ref _discarded);
        }

        public void AddToExited(int nb)
        {
            Interlocked.Add(ref _exited, nb);
        }

        public void IncrementExited()
        {
            Interlocked.Increment(ref _exited);
        }

        public void IncrementReceived()
        {
            Interlocked.Increment(ref _received);
        }
    }
}
