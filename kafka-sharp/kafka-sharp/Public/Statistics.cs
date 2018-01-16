// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

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
        /// The number of connection time out encountered (Kafka cluster did not reply)
        /// </summary>
        long RequestTimeout { get; }

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
        /// Number of produce requests that have entered the system
        /// </summary>
        long Entered { get; }

        /// <summary>
        /// Number of produce requests that have exited the system either successful, discard or expired.
        /// </summary>
        long Exited { get; }

        /// <summary>
        /// Number of messages received by the consumer.
        /// </summary>
        long Received { get; }

        /// <summary>
        /// Number of messages received in latest request, including those that are
        /// filtered due to offset out of range (i.e. consumer response size).
        /// </summary>
        long RawReceived { get; }

        /// <summary>
        /// Number of bytes received through latest Fetch responses (i.e. consumer response size).
        /// </summary>
        long RawReceivedBytes { get; }

        /// <summary>
        /// Number of messages sent over the wire in latest Produce request, including messages
        /// sent multiple times through retries (i.e. producer batch size).
        /// </summary>
        long RawProduced { get; }

        /// <summary>
        /// Number of bytes sent through latest Produce requests (i.e. producer batch size).
        /// </summary>
        long RawProducedBytes { get; }

        /// <summary>
        /// The number of allocated socket buffers.
        /// </summary>
        long SocketBuffers { get; }

        /// <summary>
        ///  The number of allocated buffers to serialize requests.
        /// </summary>
        long RequestsBuffers { get; }

        /// <summary>
        /// The number of allocated buffers to serialize messages
        /// when SerializeOnProduce is set to true.
        /// </summary>
        long MessageBuffers { get; }

        /// <summary>
        ///  The latency in ms of the latest request
        /// </summary>
        double LatestRequestLatency { get; }

        /// <summary>
        ///  Number of partitions with timeout errors in produce request responses
        /// </summary>
        long BrokerTimeoutError { get; }

        /// <summary>
        /// Number of retries done to send a produce message
        /// </summary>
        long MessageRetry { get; }

        /// <summary>
        /// Number of messages that were postponed
        /// </summary>
        long MessagePostponed { get; }

        /// <summary>
        /// Lag observed by the consumer for the latest request
        /// </summary>
        long LatestConsumerLag { get; }

        void UpdateSuccessfulSent(long nb);

        void UpdateRequestSent();

        void UpdateResponseReceived(int nodeId, double latencyMs);

        void UpdateRequestTimeout(int nodeId);

        void UpdateErrors();

        void UpdateNodeDead(int nodeId);

        void UpdateExpired();

        void UpdateDiscarded();

        void UpdateEntered();

        void UpdateExited(long nb);

        void UpdateExited();

        void UpdateReceived();

        void UpdateRawReceived(long nb);

        void UpdateRawReceivedBytes(long nb);

        void UpdateRawProduced(long nb);

        void UpdateRawProducedBytes(long nb);

        void UpdateSocketBuffers(long nb);

        void UpdateRequestsBuffers(long nb);

        void UpdateMessageBuffers(long nb);

        void UpdateBrokerTimeoutError(string topic);

        void UpdateMessageRetry(string topic);

        void UpdateMessagePostponed(string topic);

        void UpdateConsumerLag(string topic, long nb);
    }

    public class Statistics : IStatistics
    {
        private long _successfulSent;
        private long _requestSent;
        private long _responseReceived;
        private long _requestTimeout;
        private long _errors;
        private long _nodeDead;
        private long _expired;
        private long _discarded;
        private long _entered;
        private long _exited;
        private long _received;
        private long _rawReceived;
        private long _rawReceivedBytes;
        private long _rawProduced;
        private long _rawProducedBytes;
        private long _socketBuffers;
        private long _requestsBuffers;
        private long _messageBuffers;
        private double _latestRequestLatency;
        private long _brokerTimeoutError;
        private long _messageRetry;
        private long _messagePostponed;
        private long _latestConsumerLag;

        public long SuccessfulSent { get { return _successfulSent; } }

        public long RequestSent { get { return _requestSent; } }

        public long ResponseReceived { get { return _responseReceived; } }

        public long RequestTimeout { get { return _requestTimeout; } }

        public long Errors { get { return _errors; } }

        public long NodeDead { get { return _nodeDead; } }

        public long Expired { get { return _expired; } }

        public long Discarded { get { return _discarded; } }

        public long Entered { get { return _entered; } }

        public long Exited { get { return _exited; } }

        public long Received { get { return _received; } }

        public long RawReceived { get { return _rawReceived; } }

        public long RawReceivedBytes { get { return _rawReceivedBytes; } }

        public long RawProduced { get { return _rawProduced; } }

        public long RawProducedBytes { get { return _rawProducedBytes; } }

        public long SocketBuffers { get { return _socketBuffers; } }

        public long RequestsBuffers { get { return _requestsBuffers; } }

        public long MessageBuffers { get { return _messageBuffers; } }

        public double LatestRequestLatency { get { return _latestRequestLatency; } }

        public long BrokerTimeoutError { get { return _brokerTimeoutError; } }

        public long MessageRetry { get { return _messageRetry; } }

        public long MessagePostponed { get { return _messagePostponed; } }

        public long LatestConsumerLag { get { return _latestConsumerLag; } }

        public override string ToString()
        {
            return string.Format(@"Messages successfully sent: {0} - Messages received: {8}
                    Requests sent: {1} - Responses received: {2}
                    Requests time out: {17} - Broker time out error: {18}
                    Errors: {3} - Dead nodes: {4}
                    Expired: {5} - Discarded: {6}
                    Entered: {16} - Exited: {7}
                    Raw produced: {9} - Raw produced bytes: {10}
                    Raw received: {11} - Raw received bytes: {12}
                    Socket buffers: {13} - Requests buffers: {14} - MessageBuffers: {15}
                    Message retry: {19} - Message postponed: {20} - Consumer lag: {21}", SuccessfulSent, RequestSent, ResponseReceived,
                Errors, NodeDead, Expired, Discarded, Exited, Received, RawProduced, RawProducedBytes, RawReceived,
                RawReceivedBytes, SocketBuffers, RequestsBuffers, MessageBuffers, Entered, RequestTimeout,
                BrokerTimeoutError, MessageRetry, MessagePostponed, LatestConsumerLag);
        }

        public void UpdateSuccessfulSent(long nb)
        {
            Interlocked.Add(ref _successfulSent, nb);
        }

        public void UpdateRequestSent()
        {
            Interlocked.Increment(ref _requestSent);
        }

        public void UpdateResponseReceived(int nodeId, double latencyMs)
        {
            Interlocked.Increment(ref _responseReceived);
            Interlocked.Exchange(ref _latestRequestLatency, latencyMs);
        }

        public void UpdateRequestTimeout(int nodeId)
        {
            Interlocked.Increment(ref _requestTimeout);
        }

        public void UpdateErrors()
        {
            Interlocked.Increment(ref _errors);
        }

        public void UpdateNodeDead(int nodeId)
        {
            Interlocked.Increment(ref _nodeDead);
        }

        public void UpdateExpired()
        {
            Interlocked.Increment(ref _expired);
        }

        public void UpdateDiscarded()
        {
            Interlocked.Increment(ref _discarded);
        }

        public void UpdateEntered()
        {
            Interlocked.Increment(ref _entered);
        }

        public void UpdateExited(long nb)
        {
            Interlocked.Add(ref _exited, nb);
        }

        public void UpdateExited()
        {
            Interlocked.Increment(ref _exited);
        }

        public void UpdateReceived()
        {
            Interlocked.Increment(ref _received);
        }

        public void UpdateRawReceived(long nb)
        {
            Interlocked.Add(ref _rawReceived, nb);
        }

        public void UpdateRawReceivedBytes(long nb)
        {
            Interlocked.Add(ref _rawReceivedBytes, nb);
        }

        public void UpdateRawProduced(long nb)
        {
            Interlocked.Add(ref _rawProduced, nb);
        }

        public void UpdateRawProducedBytes(long nb)
        {
            Interlocked.Add(ref _rawProducedBytes, nb);
        }

        public void UpdateSocketBuffers(long nb)
        {
            Interlocked.Add(ref _socketBuffers, nb);
        }

        public void UpdateRequestsBuffers(long nb)
        {
            Interlocked.Add(ref _requestsBuffers, nb);
        }

        public void UpdateMessageBuffers(long nb)
        {
            Interlocked.Add(ref _messageBuffers, nb);
        }

        public void UpdateBrokerTimeoutError(string topic)
        {
            Interlocked.Increment(ref _brokerTimeoutError);
        }

        public void UpdateMessageRetry(string topic)
        {
            Interlocked.Increment(ref _messageRetry);
        }

        public void UpdateMessagePostponed(string topic)
        {
            Interlocked.Increment(ref _messagePostponed);
        }

        public void UpdateConsumerLag(string topic, long nb)
        {
            Interlocked.Exchange(ref _latestConsumerLag, nb);
        }
    }
}
