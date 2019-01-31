using System.Collections.Generic;
using Kafka.Routing.PartitionSelection;

namespace Kafka.Public
{
    public enum PartitionSelectionStrategy
    {
        /// <summary>
        /// Round Robin between all available partitions.
        /// </summary>
        RoundRobin,

        /// <summary>
        /// Choose the partition depending on the message's key. It guarantees that given the same
        /// number of available partitions, messages with the same key will go to the same partition.
        /// If the message's key is null, it fall backs to the round robin strategy.
        /// If the chosen partition is blacklisted, messages to that partition will actually be sent
        /// to another partition using the round robin strategy.
        /// </summary>
        MessageKey,
    }

    public sealed class PartitionSelectionConfig
    {
        private readonly Dictionary<string, PartitionSelectionStrategy> _selectionStrategyByTopic;
        private PartitionSelectionStrategy _defaultStrategy = PartitionSelectionStrategy.RoundRobin;

        public PartitionSelectionConfig()
        {
            _selectionStrategyByTopic = new Dictionary<string, PartitionSelectionStrategy>();
        }

        public void SetDefaultPartitionSelectionStrategy(PartitionSelectionStrategy strategy)
        {
            _defaultStrategy = strategy;
        }

        public void SetPartitionSelectionStrategyForTopic(string topic, PartitionSelectionStrategy strategy)
        {
            _selectionStrategyByTopic.Add(topic, strategy);
        }

        internal IPartitionSelection GetPartitionSelectionForTopic(string topic, int delay, int startSeed, ISerializer keySerializer)
        {
            if (!_selectionStrategyByTopic.TryGetValue(topic, out var selectionStrategy))
            {
                selectionStrategy = _defaultStrategy;
            }

            var roundRobinSelection = new RoundRobinPartitionSelection(delay, startSeed);
            if (selectionStrategy == PartitionSelectionStrategy.RoundRobin)
            {
                return roundRobinSelection;
            }

            return new MessageKeyPartitionSelection(keySerializer, roundRobinSelection);
        }
    }
}
