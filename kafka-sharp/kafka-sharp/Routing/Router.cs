// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. 
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using Kafka.Protocol;
using Kafka.Public;
using ICluster = Kafka.Cluster.ICluster;

namespace Kafka.Routing
{
    using Partitioners = Dictionary<string, IPartitioner>;

    interface IRouter
    {
        void ChangeRoutingTable(RoutingTable table);
        void Route(string topic, Message message, DateTime expirationDate);
        void Route(string topic, IEnumerable<Message> messages, DateTime expirationDate);
        Task Stop();
        event Action<string> MessageRouted;
        event Action<string> MessageExpired;
    }

    /// <summary>
    /// Route messages to Kafka nodes according to topic partitioners (default
    /// is to round robin between nodes associated to a topic).
    /// All mechanics is handled by an actor modelized using ActionBlock class.
    /// Most modification of state is done within the actor loop.
    /// </summary>
    class Router : IRouter
    {
        /// <summary>
        /// Underlying actor supported messages.
        /// </summary>
        enum RouterMessageType
        {
            Partitioner,
            Produce
        }

        class PartitionerMessage
        {
            public string Topic;
            public object PartitionerInfo;
        }

        class ProduceMessage
        {
            public string Topic;
            public IEnumerable<Message> Messages;
            public DateTime ExpirationDate;

            // Those objects are pooled to minimize stress on the GC.
            // Use New/Release for managing lifecycle.

            private ProduceMessage() { }

            public static ProduceMessage New(string topic, IEnumerable<Message> messages, DateTime expirationDate)
            {
                ProduceMessage reserved;
                if (!_produceMessagePool.TryDequeue(out reserved))
                {
                    reserved = new ProduceMessage();
                }
                reserved.Topic = topic;
                reserved.Messages = messages;
                reserved.ExpirationDate = expirationDate;
                return reserved;
            }

            public static void Release(ProduceMessage message)
            {
                _produceMessagePool.Enqueue(message);
            }

            static readonly ConcurrentQueue<ProduceMessage> _produceMessagePool = new ConcurrentQueue<ProduceMessage>();
        }
        
        [StructLayout(LayoutKind.Explicit)]
        struct RouterMessageValue
        {
            [FieldOffset(0)]
            public PartitionerMessage PartitionerMessage;

            [FieldOffset(0)]
            public ProduceMessage ProduceMessage;
        }

        /// <summary>
        /// The actor's messages type.
        /// </summary>
        struct RouterMessage
        {
            public RouterMessageType MessageType;
            public RouterMessageValue MessageValue;
        }

        private readonly ICluster _cluster;

        private RoutingTable _routingTable = new RoutingTable(new Dictionary<string, Partition[]>());
        private Partitioners _partitioners = new Partitioners();

        private readonly ActionBlock<RouterMessage> _messages;

        public event Action<string> MessageEnqueued = _ => { };
        public event Action<string> MessageReEnqueued = _ => { };
        public event Action<string> MessageExpired = _ => { };
        public event Action<string> MessageRouted = _ => { };
        public event Action RoutingTableRequired = () => { };

        public Router(ICluster cluster, Configuration configuration)
        {
            _cluster = cluster;
            _messages = new ActionBlock<RouterMessage>(m => ProcessMessage(m),
                                                       new ExecutionDataflowBlockOptions
                                                           {
                                                               MaxMessagesPerTask = configuration.BatchSize,
                                                               TaskScheduler = configuration.TaskScheduler
                                                           });
        }

        /// <summary>
        /// Flushes messages and stop accepting new ones.
        /// </summary>
        public async Task Stop()
        {
            _messages.Complete();
            await _messages.Completion;
        }

        public void ChangeRoutingTable(RoutingTable table)
        {
            Interlocked.Exchange(ref _routingTable, table);
        }

        public void Route(string topic, Message message,  DateTime expirationDate)
        {
            Route(topic, Enumerable.Repeat(message, 1), expirationDate);
        }

        public void Route(string topic, IEnumerable<Message> messages, DateTime expirationDate)
        {
            var posted = _messages.Post(new RouterMessage
                {
                    MessageType = RouterMessageType.Produce,
                    MessageValue = new RouterMessageValue
                        {
                            ProduceMessage = ProduceMessage.New(topic, messages, expirationDate)
                        }
                });
            if (posted)
            {
                MessageEnqueued(topic);
            }
        }

        private void ReEnqueue(ProduceMessage message)
        {
            if (message.ExpirationDate < DateTime.UtcNow)
            {
                MessageExpired(message.Topic);
                return;
            }

            _messages.Post(new RouterMessage
                {
                    MessageType = RouterMessageType.Produce,
                    MessageValue = new RouterMessageValue
                        {
                            ProduceMessage = message
                        }
                });
            MessageReEnqueued(message.Topic);
        }

        public void SetPartitioners(Partitioners partitioners)
        {
            if (partitioners == null)
                throw new ArgumentNullException("partitioners");

            _messages.Post(new RouterMessage
                {
                    MessageType = RouterMessageType.Partitioner,
                    MessageValue = new RouterMessageValue
                        {
                            PartitionerMessage = new PartitionerMessage
                                {
                                    PartitionerInfo = partitioners
                                }
                        }
                });
        }

        public void SetPartitioner(string topic, IPartitioner partitioner)
        {
            if (topic == null)
                throw new ArgumentNullException("topic");
            if (partitioner == null)
                throw new ArgumentNullException("partitioner");

            _messages.Post(new RouterMessage
            {
                MessageType = RouterMessageType.Partitioner,
                MessageValue = new RouterMessageValue
                {
                    PartitionerMessage = new PartitionerMessage
                    {
                        Topic = topic,
                        PartitionerInfo = partitioner
                    }
                }
            });
        }

        private async Task EnsureHasRoutingTable()
        {
            RoutingTableRequired();
            bool hasError = false;
            try
            {
                _routingTable = await _cluster.RequireNewRoutingTable();
            }
            catch(Exception ex)
            {
                hasError = true;
            }
            if (hasError)
                await Task.Delay(1000);
        }

        /// <summary>
        /// Actor loop.
        /// </summary>
        private async Task ProcessMessage(RouterMessage message)
        {
            switch (message.MessageType)
            {
                case RouterMessageType.Partitioner:
                    HandlePartitionerMessage(message.MessageValue.PartitionerMessage);
                    break;

                case RouterMessageType.Produce:
                    await HandleProduceMessage(message.MessageValue.ProduceMessage);
                    break;
            }
        }

        private void HandlePartitionerMessage(PartitionerMessage message)
        {
            if (message.Topic != null)
            {
                _partitioners[message.Topic] = message.PartitionerInfo as IPartitioner;
            }
            else
            {
                _partitioners = message.PartitionerInfo as Partitioners;
            }
        }

        private async Task HandleProduceMessage(ProduceMessage produceMessage)
        {
            if (produceMessage.ExpirationDate < DateTime.UtcNow)
            {
                MessageExpired(produceMessage.Topic);
                return;
            }

            IPartitioner partitioner;
            if (!_partitioners.TryGetValue(produceMessage.Topic, out partitioner))
            {
                partitioner = new DefaultPartitioner();
                _partitioners[produceMessage.Topic] = partitioner;
            }

            var partitions = _routingTable.GetPartitions(produceMessage.Topic);
            if (partitions.Length == 0)
            {
                await EnsureHasRoutingTable();
                partitions = _routingTable.GetPartitions(produceMessage.Topic);
                if (partitions.Length == 0)
                {
                    ReEnqueue(produceMessage);
                    return;
                }
            }

            foreach (var message in produceMessage.Messages)
            {
                var partition = partitioner.GetPartition(message, partitions);
                partition.Leader.Produce(produceMessage.Topic, partition.Id, message, produceMessage.ExpirationDate);
                MessageRouted(produceMessage.Topic);
            }
            ProduceMessage.Release(produceMessage);
        }
    }
}
