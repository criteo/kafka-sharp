using System;
using System.Collections.Generic;
using Kafka.Cluster;
using Kafka.Common;
using Kafka.Public;
using Kafka.Public.Loggers;

namespace Kafka.Routing.PartitionSelection
{
    internal class MessageKeyPartitionSelection : IPartitionSelection
    {
        private const int LogSamplingPercentage = 99;

        private readonly ISerializer _keySerializer;
        private readonly IPartitionSelection _roundRobinSelection;
        private readonly ILogger _logger;
        private readonly Pool<ReusableMemoryStream> _messageKeyBuffersPool;
        private readonly Random _rnd;

        public MessageKeyPartitionSelection(ISerializer keySerializer, IPartitionSelection roundRobinSelection,
            ILogger logger)
        {
            _keySerializer = keySerializer;
            _roundRobinSelection = roundRobinSelection;
            _logger = logger;
            _rnd = new Random();
            _messageKeyBuffersPool = new Pool<ReusableMemoryStream>(
                limit: 100,
                constructor: () => new ReusableMemoryStream(_messageKeyBuffersPool),
                clearAction: (stream, reused) =>
                {
                    if (reused)
                        stream.SetLength(0);
                });
        }

        public Partition GetPartition(ProduceMessage produceMessage, Partition[] partitions, IReadOnlyDictionary<int, DateTime> blacklist)
        {
            var partitionId = GetPartitionIdFromKey(produceMessage.Message.Key, partitions.Length);
            if (partitionId != Partition.None.Id && !blacklist.ContainsKey(partitionId))
            {
                return partitions[partitionId];
            }

            return _roundRobinSelection.GetPartition(produceMessage, partitions, blacklist);
        }

        /// <summary>
        /// Compute the partition id given the message key and the number of partitions
        /// Do a simple Crc32 of the key, modulo the number of partitions. This is the exact same
        /// algorithm in librdkafka
        /// </summary>
        private int GetPartitionIdFromKey(object messageKey, int partitionsLength)
        {
            if (messageKey != null)
            {
                var memoryStream = _messageKeyBuffersPool.Reserve();
                _keySerializer.Serialize(messageKey, memoryStream);
                var partitionId = Crc32.Compute(memoryStream, 0, memoryStream.Length) % partitionsLength;
                _messageKeyBuffersPool.Release(memoryStream);
                return (int) partitionId;
            }

            if (_rnd.Next(0, 100) >= LogSamplingPercentage)
            {
                _logger.LogError($"{typeof(MessageKeyPartitionSelection)} cannot determine partition as message's "
                    + "key is null. Falling back to round robin selection");
            }

            return Partition.None.Id;
        }
    }
}
