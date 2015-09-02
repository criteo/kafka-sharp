using System.Threading;

namespace Kafka.Public
{
    /// <summary>
    /// Some statistics about the cluster.
    /// </summary>
    public interface IStatistics
    {
        /// <summary>
        /// Number of messages successfully sent (for which we have received an ack)
        /// </summary>
        long SuccessfulSent { get; }

        /// <summary>
        /// Number of requests sent to the Kafka cluster
        /// </summary>
        long RequestSent { get; }

        /// <summary>
        /// Number of responses received from the Kafka cluster
        /// </summary>
        long ResponseReceived { get; }

        /// <summary>
        /// Number of hard errors encountered (network errors or decode errors)
        /// </summary>
        long Errors { get; }

        /// <summary>
        ///  Number of times nodes have been marked dead (not the current number of dead nodes)
        /// </summary>
        long NodeDead { get; }

        /// <summary>
        /// Number of expired messages.
        /// </summary>
        long Expired { get; }

        /// <summary>
        /// Number of discarded messages.
        /// </summary>
        long Discarded { get; }

        /// <summary>
        /// Number of produce request that have exited the system either successful, discard or expired.
        /// </summary>
        long Exited { get; }

        /// <summary>
        /// Number of received messages.
        /// </summary>
        long Received { get; }

        /// <summary>
        /// Total number of received messages, including those that are filtered due to
        /// offset out of range.
        /// </summary>
        long RawReceived { get; }

        /// <summary>
        /// Total number of bytes received through Fetch responses.
        /// </summary>
        long RawReceivedBytes { get; }

        /// <summary>
        /// Total number of messages sent over the wire in Produce request,
        /// including messages sent multiple times through retries.
        /// </summary>
        long RawProduced { get; }

        /// <summary>
        /// Total number of bytes sent through Produce requests.
        /// </summary>
        long RawProducedBytes { get; }

        void AddToSuccessfulSent(long nb);

        void IncrementRequestSent();

        void IncrementResponseReceived();

        void IncrementErrors();

        void IncrementNodeDead();

        void IncrementExpired();

        void IncrementDiscarded();

        void AddToExited(long nb);

        void IncrementExited();

        void IncrementReceived();

        void AddToRawReceived(long nb);

        void AddToRawReceivedBytes(long nb);

        void AddToRawProduced(long nb);

        void AddToRawProducedBytes(long nb);
    }

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
        private long _rawReceived;
        private long _rawReceivedBytes;
        private long _rawProduced;
        private long _rawProducedBytes;

        public long SuccessfulSent { get { return _successfulSent; } }

        public long RequestSent { get { return _requestSent; } }

        public long ResponseReceived { get { return _responseReceived; } }

        public long Errors { get { return _errors; } }

        public long NodeDead { get { return _nodeDead; } }

        public long Expired { get { return _expired; } }

        public long Discarded { get { return _discarded; } }

        public long Exited { get { return _exited; } }

        public long Received { get { return _received; } }

        public long RawReceived { get { return _rawReceived; } }

        public long RawReceivedBytes { get { return _rawReceivedBytes; } }

        public long RawProduced { get { return _rawProduced; } }

        public long RawProducedBytes { get { return _rawProducedBytes; } }

        public override string ToString()
        {
            return string.Format(
                "{{Messages successfully sent: {0} - Messages received: {8} - Requests sent: {1} - Responses received: {2} - Errors: {3} - Dead nodes: {4} - Expired: {5} - Discarded: {6} - Exited: {7} - Raw produced: {9} - Raw produced bytes: {10} - Raw received: {11} - Raw received bytes: {12}}}",
                SuccessfulSent, RequestSent, ResponseReceived, Errors, NodeDead, Expired, Discarded, Exited, Received, RawProduced, RawProducedBytes, RawReceived, RawReceivedBytes);
        }

        public void AddToSuccessfulSent(long nb)
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

        public void AddToExited(long nb)
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

        public void AddToRawReceived(long nb)
        {
            Interlocked.Add(ref _rawReceived, nb);
        }

        public void AddToRawReceivedBytes(long nb)
        {
            Interlocked.Add(ref _rawReceivedBytes, nb);
        }

        public void AddToRawProduced(long nb)
        {
            Interlocked.Add(ref _rawProduced, nb);
        }

        public void AddToRawProducedBytes(long nb)
        {
            Interlocked.Add(ref _rawProducedBytes, nb);
        }
    }
}
