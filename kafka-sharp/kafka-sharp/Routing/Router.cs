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
using Kafka.Cluster;
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
        void Route(ProduceMessage produceMessage);
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
            Produce,
            CheckPostponed
        }

        class PartitionerMessage
        {
            public string Topic;
            public object PartitionerInfo;
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
        private readonly Configuration _configuration;

        private RoutingTable _routingTable = new RoutingTable(new Dictionary<string, Partition[]>());
        private Partitioners _partitioners = new Partitioners();
        
        private readonly ActionBlock<RouterMessage> _messages;
        private readonly Dictionary<string, Queue<ProduceMessage>> _postponedMessages = new Dictionary<string, Queue<ProduceMessage>>();
        private Timer _checkPostponedMessages;

        public event Action<string> MessageEnqueued = _ => { };
        public event Action<string> MessageReEnqueued = _ => { };
        public event Action<string> MessageExpired = _ => { };
        public event Action<string> MessageRouted = _ => { };
        public event Action RoutingTableRequired = () => { };

        public Router(ICluster cluster, Configuration configuration)
        {
            _cluster = cluster;
            _configuration = configuration;
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
            _messages.Post(new RouterMessage {MessageType = RouterMessageType.CheckPostponed});
        }

        public void Route(string topic, Message message,  DateTime expirationDate)
        {
            Route(ProduceMessage.New(topic, message, expirationDate));
        }

        public void Route(ProduceMessage message)
        {
            var posted = _messages.Post(new RouterMessage
            {
                MessageType = RouterMessageType.Produce,
                MessageValue = new RouterMessageValue
                {
                    ProduceMessage = message
                }
            });
            if (posted)
            {
                MessageEnqueued(message.Topic);
            }
            else
            {
                ProduceMessage.Release(message);
            }
        }

        private void ReEnqueue(ProduceMessage message)
        {
            if (message.ExpirationDate < DateTime.UtcNow)
            {
                OnMessageExpired(message);
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

                case RouterMessageType.CheckPostponed:
                    HandlePostponedMessages();
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
                OnMessageExpired(produceMessage);
                return;
            }

            var topic = produceMessage.Topic;

            if (IsPostponedTopic(topic))
            {
                PostponeMessage(produceMessage);
                return;
            }

            IPartitioner partitioner;
            if (!_partitioners.TryGetValue(topic, out partitioner))
            {
                partitioner = new DefaultPartitioner();
                _partitioners[topic] = partitioner;
            }

            var partitions = _routingTable.GetPartitions(topic);
            if (partitions.Length == 0)
            {
                await EnsureHasRoutingTable();
                partitions = _routingTable.GetPartitions(topic);
                if (partitions.Length == 0)
                {
                    // Message for topics with no partition available are postponed.
                    // They will be checked again when the routing table is updated.
                    PostponeMessage(produceMessage);
                    return;
                }
            }

            var partition = partitioner.GetPartition(produceMessage.Message, partitions);
            produceMessage.Partition = partition.Id;
            partition.Leader.Produce(produceMessage);
            MessageRouted(topic);
        }

        private int _numberOfPostponedMessages;

        private void HandlePostponedMessages()
        {
            foreach (var kv in _postponedMessages)
            {
                var postponed = kv.Value;
                if (_routingTable.GetPartitions(kv.Key).Length > 0)
                {
                    while (postponed.Count > 0)
                    {
                        ReEnqueue(postponed.Dequeue());
                        --_numberOfPostponedMessages;
                    }
                }
                else
                {
                    while (postponed.Count > 0)
                    {
                        if (postponed.Peek().ExpirationDate >= DateTime.UtcNow)
                        {
                            // Messages are queued in order or close enough
                            // so just get out of the loop on the first non expired
                            // message encountered
                            break;
                        }
                        OnMessageExpired(postponed.Dequeue());
                        --_numberOfPostponedMessages;
                    }
                }
            }

            if (_numberOfPostponedMessages == 0 && _checkPostponedMessages != null)
            {
                _checkPostponedMessages.Dispose();
                _checkPostponedMessages = null;
            }
        }

        private bool IsPostponedTopic(string topic)
        {
            Queue<ProduceMessage> postponed;
            if (_postponedMessages.TryGetValue(topic, out postponed) && postponed.Count > 0)
            {
                return true;
            }
            return false;
        }

        private void PostponeMessage(ProduceMessage produceMessage)
        {
            Queue<ProduceMessage> postponedQueue;
            if (!_postponedMessages.TryGetValue(produceMessage.Topic, out postponedQueue))
            {
                postponedQueue = new Queue<ProduceMessage>();
                _postponedMessages.Add(produceMessage.Topic, postponedQueue);
            }
            postponedQueue.Enqueue(produceMessage);
            ++_numberOfPostponedMessages;
            if (_checkPostponedMessages == null)
            {
                _checkPostponedMessages =
                    new Timer(_ => _messages.Post(new RouterMessage {MessageType = RouterMessageType.CheckPostponed}),
                              null, _configuration.MessageTtl, _configuration.MessageTtl);
            }
        }

        private void OnMessageExpired(ProduceMessage message)
        {
            MessageExpired(message.Topic);
            ProduceMessage.Release(message);
        }
    }
}
