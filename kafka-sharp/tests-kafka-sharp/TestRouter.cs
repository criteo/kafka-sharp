using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Kafka.Cluster;
using Kafka.Protocol;
using Kafka.Public;
using Kafka.Routing;
using NUnit.Framework;

namespace tests_kafka_sharp
{
    [TestFixture]
    class TestRouter
    {
        private NodeMock[] _nodes;
        private ClusterMock _cluster;
        private Dictionary<string, Partition[]> _routes;
        private ProduceRouter _produceRouter;
        private Dictionary<string, int> _messagesSentByTopic;
        private int MessagesEnqueued;
        private int MessagesReEnqueued;
        private int MessagesRouted;
        private int MessagesExpired;
        private int MessagesPostponed;
        private int RoutingTableRequired;

        private AsyncCountdownEvent _finished;

        private void InitRouter(ProduceRouter produceRouter)
        {
            _produceRouter = produceRouter;
            _produceRouter.MessageEnqueued += _ =>
            {
                ++MessagesEnqueued;
                if (_finished != null) _finished.Signal();
            };
            _produceRouter.MessageReEnqueued += _ =>
            {
                ++MessagesReEnqueued;
                if (_finished != null) _finished.Signal();
            };
            _produceRouter.MessageExpired += _ =>
            {
                ++MessagesExpired;
                if (_finished != null) _finished.Signal();
            };
            _produceRouter.MessageRouted += _ =>
            {
                ++MessagesRouted;
                if (_finished != null) _finished.Signal();
            };
            _produceRouter.MessagePostponed += _ =>
            {
                ++MessagesPostponed;
                if (_finished != null) _finished.Signal();
            };
            _produceRouter.RoutingTableRequired += () =>
            {
                ++RoutingTableRequired;
                if (_finished != null) _finished.Signal();
            };
        }

        [SetUp]
        public void SetUp()
        {
            _messagesSentByTopic = new Dictionary<string, int>
                {
                    {"test", 0},
                    {"test2", 0},
                    {"test1p", 0},
                    {"test2p", 0},
                    {"testallp", 0}
                };
            _nodes = new NodeMock[5];
            for (int i = 0; i < _nodes.Length; ++i)
            {
                _nodes[i] = new NodeMock();
                _nodes[i].MessageReceived += t => _messagesSentByTopic[t] += 1; // No need to use interlocked, NodeMock is synchronous
            }

            _routes = new Dictionary<string, Partition[]>
                {
                    {"test1p", new[] {new Partition {Id = 0, Leader = _nodes[0]}}},
                    {"test2p", new[] {new Partition {Id = 0, Leader = _nodes[0]}, new Partition {Id = 1, Leader = _nodes[1]}}},
                    {"testallp", new[]
                        {
                            new Partition {Id = 0, Leader = _nodes[0]},
                            new Partition {Id = 1, Leader = _nodes[1]},
                            new Partition {Id = 2, Leader = _nodes[2]},
                            new Partition {Id = 3, Leader = _nodes[3]},
                            new Partition {Id = 4, Leader = _nodes[4]},
                        }}
                };

            _cluster = new ClusterMock(_routes);

            MessagesEnqueued = MessagesExpired = MessagesReEnqueued = MessagesRouted = MessagesPostponed = RoutingTableRequired = 0;
            InitRouter(new ProduceRouter(_cluster, new Configuration()));
            _finished = null;
        }

        [TearDown]
        public void TearDown()
        {
            _nodes = null;
            _cluster = null;
        }

        [Test]
        public async Task TestMessagesAreSent()
        {
            _produceRouter.Route("test1p", new Message(), DateTime.UtcNow.AddMinutes(5));
            _produceRouter.Route("test1p", new Message(), DateTime.UtcNow.AddMinutes(5));
            _produceRouter.Route("test2p", new Message(), DateTime.UtcNow.AddMinutes(5));
            _produceRouter.Route("test2p", new Message(), DateTime.UtcNow.AddMinutes(5));
            _produceRouter.Route("testallp", new Message(), DateTime.UtcNow.AddMinutes(5));
            _produceRouter.Route("testallp", new Message(), DateTime.UtcNow.AddMinutes(5));

            await _produceRouter.Stop();

            Assert.AreEqual(2, _messagesSentByTopic["test1p"]);
            Assert.AreEqual(2, _messagesSentByTopic["test2p"]);
            Assert.AreEqual(2, _messagesSentByTopic["testallp"]);

            CheckCounters(expectedMessagesEnqueued: 6, expectedMessagesReEnqueued: 0, expectedMessagesRouted: 6, expectedMessagesExpired: 0, expectedRoutingTableRequired: 1);
        }

        private void CheckCounters(int expectedMessagesEnqueued = 0, int expectedMessagesReEnqueued = 0, int expectedMessagesRouted = 0,
            int expectedMessagesExpired = 0, int expectedMessagesPostponed = 0, int expectedRoutingTableRequired = 0)
        {
            Assert.AreEqual(expectedMessagesEnqueued, MessagesEnqueued);
            Assert.AreEqual(expectedMessagesReEnqueued, MessagesReEnqueued);
            Assert.AreEqual(expectedMessagesRouted, MessagesRouted);
            Assert.AreEqual(expectedMessagesExpired, MessagesExpired);
            Assert.AreEqual(expectedMessagesPostponed, MessagesPostponed);
            Assert.AreEqual(expectedRoutingTableRequired, RoutingTableRequired);
        }

        class TestPartitioner : IPartitioner
        {
            private readonly int _p;
            public TestPartitioner(int p)
            {
                _p = p;
            }

            public Partition GetPartition(Message message, Partition[] partitions)
            {
                return partitions[_p];
            }
        }

        [Test]
        public void TestPartitionerChanges()
        {
            var ev = new AutoResetEvent(false);
            _produceRouter.MessageRouted += _ => ev.Set();
            int node3rec = 0;
            _nodes[3].ProduceAcknowledgement += (_, ack) => node3rec += ack.OriginalBatch.SelectMany(g => g).Count();
            int node2rec = 0;
            _nodes[2].ProduceAcknowledgement += (_, ack) => node2rec += ack.OriginalBatch.SelectMany(g => g).Count();

            _produceRouter.SetPartitioners(new Dictionary<string, IPartitioner> {{"testallp", new TestPartitioner(3)}});
            _produceRouter.Route("testallp", new Message(), DateTime.UtcNow.AddMinutes(5));
            ev.WaitOne();
            _produceRouter.Route("testallp", new Message(), DateTime.UtcNow.AddMinutes(5));
            ev.WaitOne();

            Assert.AreEqual(2, node3rec);
            Assert.AreEqual(0, node2rec);

            _produceRouter.SetPartitioner("testallp", new TestPartitioner(2));
            _produceRouter.Route("testallp", new Message(), DateTime.UtcNow.AddMinutes(5));
            ev.WaitOne();
            _produceRouter.Route("testallp", new Message(), DateTime.UtcNow.AddMinutes(5));
            ev.WaitOne();
            _produceRouter.Route("testallp", new Message(), DateTime.UtcNow.AddMinutes(5));
            ev.WaitOne();

            Assert.AreEqual(2, node3rec);
            Assert.AreEqual(3, node2rec);

            CheckCounters(expectedMessagesEnqueued: 5, expectedMessagesRouted: 5, expectedRoutingTableRequired: 1);
        }

        [Test]
        public void TestAcknowledgementNoError()
        {
            var acknowledgement = new ProduceAcknowledgement
            {
                ProduceResponse = new ProduceResponse
                {
                    TopicsResponse = new[]
                                {
                                    new TopicResponse
                                        {
                                            TopicName = "test",
                                            Partitions = new[]
                                                {
                                                    new PartitionResponse
                                                        {
                                                            ErrorCode = ErrorCode.NoError,
                                                            Offset = 0,
                                                            Partition = 0
                                                        }
                                                }
                                        }
                                }
                },
                OriginalBatch =
                    new[]
                            {
                                new BatchMock
                                    {
                                        Key = "test",
                                        Messages =
                                            new[]
                                                {
                                                    ProduceMessage.New("test", 0, new Message(),
                                                                       DateTime.UtcNow.AddDays(1))
                                                }
                                    }
                            }

            };

            _TestAcknowledgementNoError(acknowledgement, 1);
        }

        [Test]
        public void TestProduceReplicaNotAvailableIsNotAnError()
        {
            var acknowledgement = new ProduceAcknowledgement
            {
                ProduceResponse = new ProduceResponse
                {
                    TopicsResponse = new[]
                                {
                                    new TopicResponse
                                        {
                                            TopicName = "test",
                                            Partitions = new[]
                                                {
                                                    new PartitionResponse
                                                        {
                                                            ErrorCode = ErrorCode.ReplicaNotAvailable,
                                                            Offset = 0,
                                                            Partition = 0
                                                        }
                                                }
                                        }
                                }
                },
                OriginalBatch =
                    new[]
                            {
                                new BatchMock
                                    {
                                        Key = "test",
                                        Messages =
                                            new[]
                                                {
                                                    ProduceMessage.New("test", 0, new Message(),
                                                                       DateTime.UtcNow.AddDays(1))
                                                }
                                    }
                            }

            };

            _TestAcknowledgementNoError(acknowledgement, 1);
        }

        [Test]
        public void TestAcknowledgementMultipleNoError()
        {
            var acknowledgement = new ProduceAcknowledgement
                {
                    ProduceResponse = new ProduceResponse
                        {
                            TopicsResponse = new[]
                                {
                                    new TopicResponse
                                        {
                                            TopicName = "test",
                                            Partitions = new[]
                                                {
                                                    new PartitionResponse
                                                        {
                                                            ErrorCode = ErrorCode.NoError,
                                                            Offset = 0,
                                                            Partition = 0
                                                        },
                                                    new PartitionResponse
                                                        {
                                                            ErrorCode = ErrorCode.NoError,
                                                            Offset = 0,
                                                            Partition = 1
                                                        },
                                                    new PartitionResponse
                                                        {
                                                            ErrorCode = ErrorCode.NoError,
                                                            Offset = 0,
                                                            Partition = 2
                                                        }
                                                }
                                        }
                                }
                        },
                    OriginalBatch =
                        new[]
                            {
                                new BatchMock
                                    {
                                        Key = "test",
                                        Messages =
                                            new[]
                                                {
                                                    ProduceMessage.New("test", 0, new Message(),
                                                                       DateTime.UtcNow.AddDays(1)),
                                                    ProduceMessage.New("test", 1, new Message(),
                                                                       DateTime.UtcNow.AddDays(1)),
                                                    ProduceMessage.New("test", 2, new Message(),
                                                                       DateTime.UtcNow.AddDays(1))
                                                }
                                    },
                                new BatchMock
                                    {
                                        Key = "test2",
                                        Messages =
                                            new[]
                                                {
                                                    ProduceMessage.New("test", 0, new Message(),
                                                                       DateTime.UtcNow.AddDays(1)),
                                                    ProduceMessage.New("test", 1, new Message(),
                                                                       DateTime.UtcNow.AddDays(1))
                                                }
                                    }
                            }

                };

            _TestAcknowledgementNoError(acknowledgement, 5);
        }

        public void _TestAcknowledgementNoError(ProduceAcknowledgement acknowledgement, int expected)
        {
            var ev = new ManualResetEvent(false);
            int rec = 0;
            int success = 0;
            int discarded = 0;
            _produceRouter.MessagesAcknowledged += (t, i) =>
            {
                Interlocked.Add(ref success, i);
                if (Interlocked.Add(ref rec, i) == expected)
                {
                    ev.Set();
                }
            };
            _produceRouter.MessagesDiscarded += (t, i) =>
            {
                Interlocked.Add(ref discarded, i);
                if (Interlocked.Add(ref rec, i) == expected)
                {
                    ev.Set();
                }
            };

            _produceRouter.Acknowledge(acknowledgement);

            ev.WaitOne();
            Assert.AreEqual(expected, rec);
            Assert.AreEqual(expected, success);
            Assert.AreEqual(0, discarded);
        }

        [Test]
        public async Task TestAcknowledgementResponseNoneProduceWasNotSent()
        {
            _finished = new AsyncCountdownEvent(3);
            var acknowledgement = new ProduceAcknowledgement
                {
                    OriginalBatch =
                        new[]
                            {
                                new BatchMock
                                    {
                                        Key = "test1p",
                                        Messages =
                                            new[] {ProduceMessage.New("test1p", 0, new Message(), DateTime.UtcNow.AddDays(1))}
                                    }
                            }
                };
            _produceRouter.Acknowledge(acknowledgement);
            await _finished.WaitAsync();
            CheckCounters(expectedMessagesReEnqueued: 1, expectedMessagesRouted: 1, expectedRoutingTableRequired: 1);
        }

        [Test]
        public async Task TestAcknowledgementResponseNoneProduceWasSentDiscard()
        {
            _finished = new AsyncCountdownEvent(1);
            int discarded = 0;
            _produceRouter.MessagesDiscarded += (t, n) =>
                {
                    discarded += n;
                    for (int i = 0; i < n; ++i)
                        _finished.Signal();
                };
            var acknowledgement = new ProduceAcknowledgement
                {
                    OriginalBatch =
                        new[]
                            {
                                new BatchMock
                                    {
                                        Key = "test1p",
                                        Messages =
                                            new[]
                                                {
                                                    ProduceMessage.New("test1p", 0, new Message(),
                                                                       DateTime.UtcNow.AddDays(1))
                                                }
                                    }
                            },
                    ReceiveDate = DateTime.UtcNow
                };
            _produceRouter.Acknowledge(acknowledgement);
            await _finished.WaitAsync();
            Assert.AreEqual(1, discarded);
        }

        [Test]
        public async Task TestAcknowledgementResponseNoneProduceWasSentRetry()
        {
            InitRouter(new ProduceRouter(_cluster, new Configuration{ErrorStrategy = ErrorStrategy.Retry}));
            _finished = new AsyncCountdownEvent(3);
            var acknowledgement = new ProduceAcknowledgement
            {
                OriginalBatch =
                    new[]
                            {
                                new BatchMock
                                    {
                                        Key = "test1p",
                                        Messages =
                                            new[]
                                                {
                                                    ProduceMessage.New("test1p", 0, new Message(),
                                                                       DateTime.UtcNow.AddDays(1))
                                                }
                                    }
                            },
                ReceiveDate = DateTime.UtcNow
            };
            _produceRouter.Acknowledge(acknowledgement);
            await _finished.WaitAsync();
            CheckCounters(expectedMessagesReEnqueued: 1, expectedMessagesRouted: 1, expectedRoutingTableRequired: 1);
        }

        [Test]
        public void TestNonRecoverableErrorsAreDiscarded()
        {
            var acknowledgement = new ProduceAcknowledgement
                {
                    ProduceResponse = new ProduceResponse
                        {
                            TopicsResponse = new[]
                                {
                                    new TopicResponse
                                        {
                                            TopicName = "test",
                                            Partitions = new[]
                                                {
                                                    new PartitionResponse
                                                        {
                                                            ErrorCode = ErrorCode.NoError,
                                                            Offset = 0,
                                                            Partition = 0
                                                        },
                                                    new PartitionResponse
                                                        {
                                                            ErrorCode = ErrorCode.MessageSizeTooLarge,
                                                            Offset = 0,
                                                            Partition = 1
                                                        }
                                                }
                                        }
                                }
                        },
                    OriginalBatch =
                        new[]
                            {
                                new BatchMock
                                    {
                                        Key = "test",
                                        Messages =
                                            new[]

                                                {
                                                    ProduceMessage.New("test", 0, new Message(),
                                                                       DateTime.UtcNow.AddDays(1)),
                                                    ProduceMessage.New("test", 1, new Message(),
                                                                       DateTime.UtcNow.AddDays(1))
                                                }
                                    }
                            }
                };

            var ev = new ManualResetEvent(false);
            int rec = 0;
            int success = 0;
            int discarded = 0;
            _produceRouter.MessagesAcknowledged += (t, i) =>
            {
                Interlocked.Add(ref success, i);
                if (Interlocked.Add(ref rec, i) == 2)
                {
                    ev.Set();
                }
            };
            _produceRouter.MessagesDiscarded += (t, i) =>
            {
                Interlocked.Add(ref discarded, i);
                if (Interlocked.Add(ref rec, i) == 2)
                {
                    ev.Set();
                }
            };

            _produceRouter.Acknowledge(acknowledgement);

            ev.WaitOne();
            Assert.AreEqual(2, rec);
            Assert.AreEqual(1, success);
            Assert.AreEqual(1, discarded);
        }

        [Test]
        public void TestProduceRecoverableErrorsAreRerouted()
        {
            var acknowledgement = new ProduceAcknowledgement
                {
                    ProduceResponse = new ProduceResponse
                        {
                            TopicsResponse = new[]
                                {
                                    new TopicResponse
                                        {
                                            TopicName = "test",
                                            Partitions = new[]
                                                {
                                                    new PartitionResponse
                                                        {
                                                            ErrorCode = ErrorCode.NoError,
                                                            Offset = 0,
                                                            Partition = 0
                                                        },
                                                    new PartitionResponse
                                                        {
                                                            ErrorCode = ErrorCode.NotLeaderForPartition,
                                                            Offset = 0,
                                                            Partition = 1
                                                        },
                                                    new PartitionResponse
                                                        {
                                                            ErrorCode = ErrorCode.LeaderNotAvailable,
                                                            Offset = 0,
                                                            Partition = 2
                                                        },
                                                    new PartitionResponse
                                                        {
                                                            ErrorCode = ErrorCode.RequestTimedOut,
                                                            Offset = 0,
                                                            Partition = 3
                                                        },
                                                    new PartitionResponse
                                                        {
                                                            ErrorCode = ErrorCode.UnknownTopicOrPartition,
                                                            Offset = 0,
                                                            Partition = 4
                                                        }
                                                }
                                        }
                                }
                        },
                    OriginalBatch =
                        new[]
                            {
                                new BatchMock
                                    {
                                        Key = "test",
                                        Messages =
                                            new[]
                                                {
                                                    ProduceMessage.New("test", 0, new Message(),
                                                                       DateTime.UtcNow.AddDays(1)),
                                                    ProduceMessage.New("test", 1, new Message(),
                                                                       DateTime.UtcNow.AddDays(1)),
                                                    ProduceMessage.New("test", 2, new Message(),
                                                                       DateTime.UtcNow.AddDays(1)),
                                                    ProduceMessage.New("test", 3, new Message(),
                                                                       DateTime.UtcNow.AddDays(1)),
                                                    ProduceMessage.New("test", 4, new Message(),
                                                                       DateTime.UtcNow.AddDays(1))
                                                }
                                    }
                            }
                };

            const int exp = 5;
            var ev = new ManualResetEvent(false);
            int rec = 0;
            int success = 0;
            int discarded = 0;
            int rerouted = 0;
            _produceRouter.MessageReEnqueued += t =>
            {
                Interlocked.Increment(ref rerouted);
                if (Interlocked.Increment(ref rec) == exp)
                {
                    ev.Set();
                }
            };
            _produceRouter.MessagesAcknowledged += (t, i) =>
            {
                Interlocked.Add(ref success, i);
                if (Interlocked.Add(ref rec, i) == exp)
                {
                    ev.Set();
                }
            };
            _produceRouter.MessagesDiscarded += (t, i) =>
            {
                Interlocked.Add(ref discarded, i);
                if (Interlocked.Add(ref rec, i) == exp)
                {
                    ev.Set();
                }
            };

            _produceRouter.Acknowledge(acknowledgement);

            ev.WaitOne();
            Assert.AreEqual(5, rec);
            Assert.AreEqual(1, success);
            Assert.AreEqual(0, discarded);
            Assert.AreEqual(4, rerouted);
        }

        [Test]
        public async Task TestExpiredMessagesAreNotRouted()
        {
            _produceRouter.Route("test1p", new Message(), DateTime.UtcNow.AddMilliseconds(-1));
            _produceRouter.Route("test2p", new Message(), DateTime.UtcNow.AddMilliseconds(-1));

            await _produceRouter.Stop();

            Assert.AreEqual(0, _messagesSentByTopic["test1p"]);
            Assert.AreEqual(0, _messagesSentByTopic["test2p"]);
            CheckCounters(expectedMessagesEnqueued: 2, expectedMessagesExpired: 2);
        }

        [Test]
        public async Task TestNoMessageIsSentAfterStop()
        {
            _produceRouter.Route("test1p", new Message(), DateTime.UtcNow.AddMinutes(5));
            await _produceRouter.Stop();
            Assert.AreEqual(1, _messagesSentByTopic["test1p"]);

            _produceRouter.Route("test1p", new Message(), DateTime.UtcNow.AddMinutes(5));
            await Task.Delay(TimeSpan.FromMilliseconds(100));

            Assert.AreEqual(1, _messagesSentByTopic["test1p"]);
            CheckCounters(expectedMessagesEnqueued: 1, expectedMessagesRouted: 1, expectedRoutingTableRequired: 1);
        }

        [Test]
        public async Task TestMessagesArePostponedIfThereAreNoPartitions_AndSentWhenPartitionsBecomeAvailable()
        {
            _cluster.Partitions = new Dictionary<string, Partition[]>();

            _finished = new AsyncCountdownEvent(8);
            _produceRouter.Route("test1p", new Message(), DateTime.UtcNow.AddMinutes(5)); // => 1x MessageEnqueued, 1x MessagePostponed, 1x RoutingTableRequired
            _produceRouter.Route("test1p", new Message(), DateTime.UtcNow.AddMinutes(5)); // => 1x MessageEnqueued, 1x MessagePostponed, no routing table required because postponed
            _produceRouter.Route("test2p", new Message(), DateTime.UtcNow.AddMinutes(5)); // => 1x MessageEnqueued, 1x MessagePostponed, 1x RoutingTableRequired
            await _finished.WaitAsync();

            Assert.AreEqual(0, _messagesSentByTopic["test1p"]);
            Assert.AreEqual(0, _messagesSentByTopic["test2p"]);
            CheckCounters(expectedMessagesEnqueued: 3, expectedMessagesPostponed: 3, expectedRoutingTableRequired: 2);

            _finished = new AsyncCountdownEvent(6);
            _cluster.Partitions = _routes;
            // Advance a little the date of the new routing table, to be sure it's greater than the current one
            _produceRouter.ChangeRoutingTable(new RoutingTable(_routes){LastRefreshed = DateTime.UtcNow.Add(TimeSpan.FromMilliseconds(1))}); // => 3x Renqueued, 3x Routed
            await _finished.WaitAsync();

            Assert.AreEqual(2, _messagesSentByTopic["test1p"]);
            Assert.AreEqual(1, _messagesSentByTopic["test2p"]);
            CheckCounters(expectedMessagesEnqueued: 3, expectedMessagesReEnqueued: 3, expectedMessagesRouted: 3, expectedMessagesPostponed: 3, expectedRoutingTableRequired: 2);
        }

        [Test]
        public async Task TestPostponedMessagesAreNotReEnqueuedIfExpired()
        {
            _cluster.Partitions = new Dictionary<string, Partition[]>();

            _finished = new AsyncCountdownEvent(3);
            _produceRouter.Route("test1p", new Message(), DateTime.UtcNow.AddMilliseconds(50));
            await _finished.WaitAsync();

            Assert.AreEqual(0, _messagesSentByTopic["test1p"]);
            CheckCounters(expectedMessagesEnqueued: 1, expectedMessagesPostponed: 1, expectedRoutingTableRequired: 1);

            await Task.Delay(TimeSpan.FromMilliseconds(100));

            //trigger check postponed
            _cluster.Partitions = _routes;
            _produceRouter.ChangeRoutingTable(new RoutingTable(_routes));

            await _produceRouter.Stop();

            Assert.AreEqual(0, _messagesSentByTopic["test1p"]);
            CheckCounters(expectedMessagesEnqueued: 1, expectedMessagesExpired: 1, expectedMessagesPostponed: 1, expectedRoutingTableRequired: 1);
        }

        [Test]
        public async Task TestPostponedExpiredMessagesAreRemovedWhenNoPartition()
        {
            _cluster.Partitions = new Dictionary<string, Partition[]>();

            _finished = new AsyncCountdownEvent(5);
            _produceRouter.Route("test1p", new Message(), DateTime.UtcNow.AddMilliseconds(50));
            _produceRouter.Route("test1p", new Message(), DateTime.UtcNow.AddMinutes(5));
            await _finished.WaitAsync();

            Assert.AreEqual(0, _messagesSentByTopic["test1p"]);
            CheckCounters(expectedMessagesEnqueued: 2, expectedMessagesPostponed: 2, expectedRoutingTableRequired: 1);
            await Task.Delay(TimeSpan.FromMilliseconds(100));

            //trigger check postponed
            _finished = new AsyncCountdownEvent(3);
            _cluster.Partitions = _routes;
            _produceRouter.ChangeRoutingTable(new RoutingTable(_routes)); // => 1x renqueue, 1x routed, 1x expired
            await _finished.WaitAsync();

            Assert.AreEqual(1, _messagesSentByTopic["test1p"]);
            CheckCounters(expectedMessagesEnqueued: 2, expectedMessagesReEnqueued: 1, expectedMessagesRouted: 1, expectedMessagesExpired: 1, expectedMessagesPostponed: 2, expectedRoutingTableRequired: 1);
        }
    }
}
