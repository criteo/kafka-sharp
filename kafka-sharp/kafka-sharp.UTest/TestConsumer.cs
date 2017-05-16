using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Kafka.Batching;
using Kafka.Cluster;
using Kafka.Protocol;
using Kafka.Public;
using Kafka.Public.Loggers;
using Kafka.Routing;
using Moq;
using NUnit.Framework;

namespace tests_kafka_sharp
{
    [TestFixture]
    internal class TestConsumer
    {
        private const string TOPIC = "poulpe";
        private const string TOPIC2 = "tako";
        private const long OFFSET = 42L;
        private const int PARTITION = 0;

        [Test]
        [TestCase(Offsets.Latest)]
        [TestCase(Offsets.Earliest)]
        public void TestStart_AllPartition_OffsetUnknown(long offset)
        {
            var node = new Mock<INode>();
            node.Setup(n => n.Fetch(It.IsAny<FetchMessage>())).Returns(true);
            node.Setup(n => n.Offset(It.IsAny<OffsetMessage>())).Returns(true);
            var cluster = new Mock<ICluster>();
            cluster.Setup(c => c.RequireAllPartitionsForTopic(TOPIC))
                .Returns(Task.FromResult(new[] {0, 1, 2}));
            cluster.Setup(c => c.RequireNewRoutingTable())
                .Returns(() =>
                    Task.FromResult(
                        new RoutingTable(new Dictionary<string, Partition[]>
                        {
                            {
                                TOPIC,
                                new[]
                                {
                                    new Partition {Id = 0, Leader = node.Object},
                                    new Partition {Id = 1, Leader = node.Object},
                                    new Partition {Id = 2, Leader = node.Object}
                                }
                            }
                        })));
            var configuration = new Configuration {TaskScheduler = new CurrentThreadTaskScheduler()};
            var consumer = new ConsumeRouter(cluster.Object, configuration, 1);
            consumer.StartConsume(TOPIC, Partitions.All, offset);
            cluster.Verify(c => c.RequireAllPartitionsForTopic(It.Is<string>(t => t == TOPIC)), Times.AtLeastOnce());
            cluster.Verify(c => c.RequireAllPartitionsForTopic(It.Is<string>(s => s != TOPIC)), Times.Never());
            cluster.Verify(c => c.RequireNewRoutingTable(), Times.AtLeastOnce());
            node.Verify(n => n.Offset(It.IsAny<OffsetMessage>()), Times.Exactly(3));
            node.Verify(n => n.Offset(It.Is<OffsetMessage>(om => om.Partition == 0)), Times.Once());
            node.Verify(n => n.Offset(It.Is<OffsetMessage>(om => om.Partition == 1)), Times.Once());
            node.Verify(n => n.Offset(It.Is<OffsetMessage>(om => om.Partition == 2)), Times.Once());
            node.Verify(n => n.Offset(It.Is<OffsetMessage>(om => om.Topic != TOPIC)), Times.Never());
            node.Verify(n => n.Offset(It.Is<OffsetMessage>(om => om.Time != offset)), Times.Never());
        }

        [Test]
        public void TestStart_AllPartitions_KnownOffset()
        {
            var node = new Mock<INode>();
            node.Setup(n => n.Fetch(It.IsAny<FetchMessage>())).Returns(true);
            node.Setup(n => n.Offset(It.IsAny<OffsetMessage>())).Returns(true);
            var cluster = new Mock<ICluster>();
            cluster.Setup(c => c.RequireAllPartitionsForTopic(TOPIC))
                .Returns(Task.FromResult(new[] {0, 1, 2}));
            cluster.Setup(c => c.RequireNewRoutingTable())
                .Returns(() =>
                    Task.FromResult(
                        new RoutingTable(new Dictionary<string, Partition[]>
                        {
                            {
                                TOPIC,
                                new[]
                                {
                                    new Partition {Id = 0, Leader = node.Object},
                                    new Partition {Id = 1, Leader = node.Object},
                                    new Partition {Id = 2, Leader = node.Object}
                                }
                            }
                        })));
            var configuration = new Configuration {TaskScheduler = new CurrentThreadTaskScheduler()};
            var consumer = new ConsumeRouter(cluster.Object, configuration, 1);
            consumer.StartConsume(TOPIC, Partitions.All, OFFSET);
            cluster.Verify(c => c.RequireAllPartitionsForTopic(It.Is<string>(t => t == TOPIC)), Times.AtLeastOnce());
            cluster.Verify(c => c.RequireAllPartitionsForTopic(It.Is<string>(s => s != TOPIC)), Times.Never());
            cluster.Verify(c => c.RequireNewRoutingTable(), Times.AtLeastOnce());
            node.Verify(n => n.Fetch(It.IsAny<FetchMessage>()), Times.Exactly(3));
            node.Verify(n => n.Fetch(It.Is<FetchMessage>(fm => fm.Partition == 0)), Times.Once());
            node.Verify(n => n.Fetch(It.Is<FetchMessage>(fm => fm.Partition == 1)), Times.Once());
            node.Verify(n => n.Fetch(It.Is<FetchMessage>(fm => fm.Partition == 2)), Times.Once());
            node.Verify(n => n.Fetch(It.Is<FetchMessage>(fm => fm.Topic != TOPIC)), Times.Never());
            node.Verify(n => n.Fetch(It.Is<FetchMessage>(fm => fm.Offset != OFFSET)), Times.Never());
            node.Verify(n => n.Fetch(It.Is<FetchMessage>(fm => fm.MaxBytes != configuration.FetchMessageMaxBytes)),
                Times.Never());
        }

        [Test]
        public void TestStart_AllPartitions_KnownOffset_GlobalBatching()
        {
            var node = new Mock<INode>();
            var cluster = new Mock<ICluster>();
            cluster.Setup(c => c.RequireAllPartitionsForTopic(TOPIC))
                .Returns(Task.FromResult(new[] { 0, 1, 2 }));
            cluster.Setup(c => c.RequireNewRoutingTable())
                .Returns(() =>
                    Task.FromResult(
                        new RoutingTable(new Dictionary<string, Partition[]>
                        {
                            {
                                TOPIC,
                                new[]
                                {
                                    new Partition {Id = 0, Leader = node.Object},
                                    new Partition {Id = 1, Leader = node.Object},
                                    new Partition {Id = 2, Leader = node.Object}
                                }
                            }
                        })));
            var configuration = new Configuration
            {
                TaskScheduler = new CurrentThreadTaskScheduler(),
                BatchStrategy = BatchStrategy.Global,
                ConsumeBatchSize = 3,
                ConsumeBufferingTime = TimeSpan.FromHours(28)
            };
            var consumer = new ConsumeRouter(cluster.Object, configuration, 1);
            consumer.StartConsume(TOPIC, Partitions.All, OFFSET);

            cluster.Verify(c => c.RequireAllPartitionsForTopic(It.Is<string>(t => t == TOPIC)), Times.AtLeastOnce());
            cluster.Verify(c => c.RequireAllPartitionsForTopic(It.Is<string>(s => s != TOPIC)), Times.Never());
            cluster.Verify(c => c.RequireNewRoutingTable(), Times.AtLeastOnce());
            node.Verify(n => n.Fetch(It.IsAny<FetchMessage>()), Times.Never());
            node.Verify(n => n.Post(It.IsAny<IBatchByTopic<FetchMessage>>()));
        }

        [Test]
        [TestCase(Offsets.Latest)]
        [TestCase(Offsets.Earliest)]
        public void TestStart_OnePartition_OffsetUnknown(long offset)
        {
            var node = new Mock<INode>();
            var cluster = new Mock<ICluster>();
            cluster.Setup(c => c.RequireNewRoutingTable())
                .Returns(() =>
                    Task.FromResult(
                        new RoutingTable(new Dictionary<string, Partition[]>
                        {
                            {
                                TOPIC,
                                new[]
                                {
                                    new Partition {Id = 0, Leader = node.Object},
                                    new Partition {Id = 1, Leader = node.Object},
                                    new Partition {Id = 2, Leader = node.Object}
                                }
                            }
                        })));
            var configuration = new Configuration {TaskScheduler = new CurrentThreadTaskScheduler()};
            var consumer = new ConsumeRouter(cluster.Object, configuration, 1);
            consumer.StartConsume(TOPIC, PARTITION, offset);
            cluster.Verify(c => c.RequireAllPartitionsForTopic(It.IsAny<string>()), Times.Never());
            cluster.Verify(c => c.RequireNewRoutingTable(), Times.AtLeastOnce());
            node.Verify(n => n.Offset(It.Is<OffsetMessage>(om => om.Partition == PARTITION)), Times.Once());
            node.Verify(n => n.Offset(It.Is<OffsetMessage>(om => om.Partition != PARTITION)), Times.Never());
            node.Verify(n => n.Offset(It.Is<OffsetMessage>(om => om.Topic != TOPIC)), Times.Never());
            node.Verify(n => n.Offset(It.Is<OffsetMessage>(om => om.Time != offset)), Times.Never());
        }

        [Test]
        public void TestStart_OnePartition_OffsetUnknown_GlobalBatching()
        {
            const long offset = Offsets.Latest;
            var node = new Mock<INode>();
            var cluster = new Mock<ICluster>();
            cluster.Setup(c => c.RequireNewRoutingTable())
                .Returns(() =>
                    Task.FromResult(
                        new RoutingTable(new Dictionary<string, Partition[]>
                        {
                            {
                                TOPIC,
                                new[]
                                {
                                    new Partition {Id = 0, Leader = node.Object},
                                    new Partition {Id = 1, Leader = node.Object},
                                    new Partition {Id = 2, Leader = node.Object}
                                }
                            }
                        })));
            cluster.Setup(c => c.RequireAllPartitionsForTopic(TOPIC)).Returns(Task.FromResult(new[] {0, 1, 2}));
            var configuration = new Configuration
            {
                TaskScheduler = new CurrentThreadTaskScheduler(),
                BatchStrategy = BatchStrategy.Global,
                ConsumeBatchSize = 3,
                ConsumeBufferingTime = TimeSpan.FromHours(28)
            };
            var consumer = new ConsumeRouter(cluster.Object, configuration, 1);
            consumer.StartConsume(TOPIC, Partitions.All, offset);

            cluster.Verify(c => c.RequireAllPartitionsForTopic(It.IsAny<string>()), Times.AtLeastOnce());
            cluster.Verify(c => c.RequireNewRoutingTable(), Times.AtLeastOnce());
            node.Verify(n => n.Offset(It.IsAny<OffsetMessage>()), Times.Never());
            node.Verify(n => n.Post(It.IsAny<IBatchByTopic<OffsetMessage>>()));
        }

        [Test]
        public void TestStart_OnePartition_KnownOffset()
        {
            var node = new Mock<INode>();
            var cluster = new Mock<ICluster>();
            cluster.Setup(c => c.RequireNewRoutingTable())
                .Returns(() =>
                    Task.FromResult(
                        new RoutingTable(new Dictionary<string, Partition[]>
                        {
                            {
                                TOPIC,
                                new[]
                                {
                                    new Partition {Id = 0, Leader = node.Object},
                                    new Partition {Id = 1, Leader = node.Object},
                                    new Partition {Id = 2, Leader = node.Object}
                                }
                            }
                        })));
            var configuration = new Configuration {TaskScheduler = new CurrentThreadTaskScheduler()};
            var consumer = new ConsumeRouter(cluster.Object, configuration, 1);
            consumer.StartConsume(TOPIC, PARTITION, OFFSET);
            cluster.Verify(c => c.RequireAllPartitionsForTopic(It.IsAny<string>()), Times.Never());
            cluster.Verify(c => c.RequireNewRoutingTable(), Times.AtLeastOnce());
            node.Verify(n => n.Fetch(It.Is<FetchMessage>(fm => fm.Partition == PARTITION)), Times.Once());
            node.Verify(n => n.Fetch(It.Is<FetchMessage>(fm => fm.Topic != TOPIC)), Times.Never());
            node.Verify(n => n.Fetch(It.Is<FetchMessage>(fm => fm.Offset != OFFSET)), Times.Never());
            node.Verify(n => n.Fetch(It.Is<FetchMessage>(fm => fm.MaxBytes != configuration.FetchMessageMaxBytes)),
                Times.Never());
        }

        [Test]
        [TestCase(Offset.Earliest)]
        [TestCase(Offset.Latest)]
        public void TestStart_OffsetsOutOfRange(Offset offsetStrategy)
        {
            var node = new Mock<INode>();
            node.Setup(n => n.Fetch(It.IsAny<FetchMessage>())).Returns(true);
            var cluster = new Mock<ICluster>();
            cluster.SetupGet(c => c.Logger).Returns(new DevNullLogger());
            cluster.Setup(c => c.RequireNewRoutingTable())
                .Returns(() =>
                    Task.FromResult(
                        new RoutingTable(new Dictionary<string, Partition[]>
                        {
                            {
                                TOPIC,
                                new[]
                                {
                                    new Partition {Id = 0, Leader = node.Object},
                                    new Partition {Id = 1, Leader = node.Object},
                                    new Partition {Id = 2, Leader = node.Object}
                                }
                            }
                        })));
            var configuration = new Configuration { TaskScheduler = new CurrentThreadTaskScheduler(), OffsetOutOfRangeStrategy = offsetStrategy};
            var consumer = new ConsumeRouter(cluster.Object, configuration, 1);
            consumer.StartConsume(TOPIC, PARTITION, OFFSET);
            cluster.Verify(c => c.RequireAllPartitionsForTopic(It.IsAny<string>()), Times.Never());
            cluster.Verify(c => c.RequireNewRoutingTable(), Times.AtLeastOnce());
            node.Verify(n => n.Fetch(It.Is<FetchMessage>(fm => fm.Partition == PARTITION)), Times.Once());
            node.Verify(n => n.Fetch(It.Is<FetchMessage>(fm => fm.Topic != TOPIC)), Times.Never());
            node.Verify(n => n.Fetch(It.Is<FetchMessage>(fm => fm.Offset != OFFSET)), Times.Never());
            node.Verify(n => n.Fetch(It.Is<FetchMessage>(fm => fm.MaxBytes != configuration.FetchMessageMaxBytes)),
                Times.Never());

            consumer.Acknowledge(new CommonAcknowledgement<FetchResponse>
            {
                Response =
                    new FetchResponse
                    {
                        FetchPartitionResponse =
                            new CommonResponse<FetchPartitionResponse>
                            {
                                TopicsResponse =
                                    new[]
                                    {
                                        new TopicData<FetchPartitionResponse>
                                        {
                                            TopicName = TOPIC,
                                            PartitionsData =
                                                new[]
                                                {
                                                    new FetchPartitionResponse
                                                    {
                                                        ErrorCode = ErrorCode.OffsetOutOfRange,
                                                        Partition = PARTITION,
                                                        HighWatermarkOffset = 432515L,
                                                    }
                                                }
                                        }
                                    }
                            }
                    },
                ReceivedDate = DateTime.UtcNow
            });

            node.Verify(n => n.Offset(It.IsAny<OffsetMessage>()), Times.Exactly(1));
            node.Verify(n => n.Offset(It.Is<OffsetMessage>(om => om.Partition == PARTITION && om.Topic == TOPIC && om.Time == (long)configuration.OffsetOutOfRangeStrategy)), Times.Once());
        }

        [Test]
        [TestCase(OFFSET - 1)]
        [TestCase(Offsets.Now)]
        [TestCase(OFFSET + 1)]
        public void TestStopConsumeBeforeFetchLoop(long offset)
        {
            var node = new Mock<INode>();
            var cluster = new Mock<ICluster>();
            cluster.Setup(c => c.RequireNewRoutingTable())
                .Returns(() =>
                    Task.FromResult(
                        new RoutingTable(new Dictionary<string, Partition[]>
                        {
                            {
                                TOPIC,
                                new[]
                                {
                                    new Partition {Id = 0, Leader = node.Object},
                                    new Partition {Id = 1, Leader = node.Object},
                                    new Partition {Id = 2, Leader = node.Object}
                                }
                            }
                        })));
            var configuration = new Configuration { TaskScheduler = new CurrentThreadTaskScheduler() };
            var consumer = new ConsumeRouter(cluster.Object, configuration, 1);
            consumer.StartConsume(TOPIC, PARTITION, Offsets.Latest);
            consumer.StopConsume(TOPIC, PARTITION, offset);

            // Now simulate an offset response, this should not trigger any fetch request.
            consumer.Acknowledge(new CommonAcknowledgement<CommonResponse<OffsetPartitionResponse>>
            {
                Response = new CommonResponse<OffsetPartitionResponse>
                {
                    TopicsResponse = new[]
                    {
                        new TopicData<OffsetPartitionResponse>
                        {
                            TopicName = TOPIC,
                            PartitionsData =
                                new[]
                                {
                                    new OffsetPartitionResponse
                                    {
                                        ErrorCode = ErrorCode.NoError,
                                        Partition = PARTITION,
                                        Offsets = new[] {OFFSET}
                                    }
                                }
                        }
                    }
                },
                ReceivedDate = DateTime.UtcNow
            });

            // Check
            if (offset < OFFSET)
            {
                node.Verify(n => n.Fetch(It.IsAny<FetchMessage>()), Times.Never());
            }
            else
            {
                node.Verify(n => n.Fetch(It.IsAny<FetchMessage>()), Times.Once);
            }
        }

        [Test]
        [TestCase(OFFSET + 1)]
        [TestCase(Offsets.Now)]
        public void TestStopConsumeAfterFetchLoop(long offset)
        {
            var node = new Mock<INode>();
            var cluster = new Mock<ICluster>();
            cluster.Setup(c => c.RequireNewRoutingTable())
                .Returns(() =>
                    Task.FromResult(
                        new RoutingTable(new Dictionary<string, Partition[]>
                        {
                            {
                                TOPIC,
                                new[]
                                {
                                    new Partition {Id = 0, Leader = node.Object},
                                    new Partition {Id = 1, Leader = node.Object},
                                    new Partition {Id = 2, Leader = node.Object}
                                }
                            }
                        })));
            var configuration = new Configuration { TaskScheduler = new CurrentThreadTaskScheduler() };
            var consumer = new ConsumeRouter(cluster.Object, configuration, 1);
            consumer.StartConsume(TOPIC, PARTITION, OFFSET);
            consumer.StopConsume(TOPIC, PARTITION, offset);

            // Now simulate a fetch response getting out of range, this should not trigger any new fetch request.
            consumer.Acknowledge(new CommonAcknowledgement<FetchResponse>
            {
                Response =
                    new FetchResponse
                    {
                        FetchPartitionResponse =
                            new CommonResponse<FetchPartitionResponse>
                            {
                                TopicsResponse =
                                    new[]
                                    {
                                        new TopicData<FetchPartitionResponse>
                                        {
                                            TopicName = TOPIC,
                                            PartitionsData =
                                                new[]
                                                {
                                                    new FetchPartitionResponse
                                                    {
                                                        ErrorCode = ErrorCode.NoError,
                                                        Partition = PARTITION,
                                                        HighWatermarkOffset = 432515L,
                                                        Messages =
                                                            new List<ResponseMessage>
                                                            {
                                                                new ResponseMessage
                                                                {
                                                                    Offset = OFFSET,
                                                                    Message = new Message()
                                                                },
                                                                new ResponseMessage
                                                                {
                                                                    Offset = OFFSET + 1,
                                                                    Message = new Message()
                                                                },
                                                                new ResponseMessage
                                                                {
                                                                    Offset = OFFSET + 2,
                                                                    Message = new Message()
                                                                },
                                                            }
                                                    }
                                                }
                                        }
                                    }
                            }
                    },
                ReceivedDate = DateTime.UtcNow
            });

            // Check
            node.Verify(n => n.Fetch(It.IsAny<FetchMessage>()), Times.Once());
        }

        [Test]
        public void TestStopConsumeAfterFetchLoopMultiple()
        {
            var node = new Mock<INode>();
            var cluster = new Mock<ICluster>();
            cluster.Setup(c => c.RequireNewRoutingTable())
                .Returns(() =>
                    Task.FromResult(
                        new RoutingTable(new Dictionary<string, Partition[]>
                        {
                            {
                                TOPIC,
                                new[]
                                {
                                    new Partition {Id = 0, Leader = node.Object},
                                    new Partition {Id = 1, Leader = node.Object},
                                    new Partition {Id = 2, Leader = node.Object}
                                }
                            }
                        })));
            var configuration = new Configuration { TaskScheduler = new CurrentThreadTaskScheduler() };
            var consumer = new ConsumeRouter(cluster.Object, configuration, 1);
            consumer.StartConsume(TOPIC, 0, OFFSET);
            consumer.StartConsume(TOPIC, 1, OFFSET);
            consumer.StopConsume(TOPIC, Partitions.All, Offsets.Now);

            // Now simulate a fetch response getting out of range, this should not trigger any new fetch request.
            consumer.Acknowledge(new CommonAcknowledgement<FetchResponse>
            {
                Response =
                    new FetchResponse
                    {
                        FetchPartitionResponse =
                            new CommonResponse<FetchPartitionResponse>
                            {
                                TopicsResponse =
                                    new[]
                                    {
                                        new TopicData<FetchPartitionResponse>
                                        {
                                            TopicName = TOPIC,
                                            PartitionsData =
                                                new[]
                                                {
                                                    new FetchPartitionResponse
                                                    {
                                                        ErrorCode = ErrorCode.NoError,
                                                        Partition = 0,
                                                        HighWatermarkOffset = 432515L,
                                                        Messages =
                                                            new List<ResponseMessage>
                                                            {
                                                                new ResponseMessage
                                                                {
                                                                    Offset = OFFSET,
                                                                    Message = new Message()
                                                                },
                                                                new ResponseMessage
                                                                {
                                                                    Offset = OFFSET + 1,
                                                                    Message = new Message()
                                                                },
                                                                new ResponseMessage
                                                                {
                                                                    Offset = OFFSET + 2,
                                                                    Message = new Message()
                                                                },
                                                            }
                                                    },
                                                    new FetchPartitionResponse
                                                    {
                                                        ErrorCode = ErrorCode.NoError,
                                                        Partition = 1,
                                                        HighWatermarkOffset = 432515L,
                                                        Messages =
                                                            new List<ResponseMessage>
                                                            {
                                                                new ResponseMessage
                                                                {
                                                                    Offset = OFFSET,
                                                                    Message = new Message()
                                                                },
                                                                new ResponseMessage
                                                                {
                                                                    Offset = OFFSET + 1,
                                                                    Message = new Message()
                                                                },
                                                                new ResponseMessage
                                                                {
                                                                    Offset = OFFSET + 2,
                                                                    Message = new Message()
                                                                },
                                                            }
                                                    }
                                                }
                                        }
                                    }
                            }
                    },
                ReceivedDate = DateTime.UtcNow
            });

            // Check
            node.Verify(n => n.Fetch(It.IsAny<FetchMessage>()), Times.Exactly(2));
        }

        [Test]
        public void TestStopAndStartConsume_SpecificOffsets()
        {
            var node = new Mock<INode>();
            var cluster = new Mock<ICluster>();
            cluster.Setup(c => c.RequireNewRoutingTable())
                .Returns(() =>
                    Task.FromResult(
                        new RoutingTable(new Dictionary<string, Partition[]>
                        {
                            {
                                TOPIC,
                                new[]
                                {
                                    new Partition {Id = 0, Leader = node.Object},
                                    new Partition {Id = 1, Leader = node.Object},
                                    new Partition {Id = 2, Leader = node.Object}
                                }
                            }
                        })));
            var configuration = new Configuration { TaskScheduler = new CurrentThreadTaskScheduler() };
            var consumer = new ConsumeRouter(cluster.Object, configuration, 1);

            var offsets = new List<long>();
            consumer.MessageReceived += r => offsets.Add(r.Offset);

            consumer.StartConsume(TOPIC, PARTITION, OFFSET);

            consumer.Acknowledge(new CommonAcknowledgement<FetchResponse>
            {
                Response =
                    new FetchResponse
                    {
                        FetchPartitionResponse =
                            new CommonResponse<FetchPartitionResponse>
                            {
                                TopicsResponse =
                                    new[]
                                    {
                                        new TopicData<FetchPartitionResponse>
                                        {
                                            TopicName = TOPIC,
                                            PartitionsData =
                                                new[]
                                                {
                                                    new FetchPartitionResponse
                                                    {
                                                        ErrorCode = ErrorCode.NoError,
                                                        Partition = PARTITION,
                                                        HighWatermarkOffset = 432515L,
                                                        Messages =
                                                            new List<ResponseMessage>
                                                            {
                                                                new ResponseMessage
                                                                {
                                                                    Offset = OFFSET,
                                                                    Message = new Message()
                                                                },
                                                                new ResponseMessage
                                                                {
                                                                    Offset = OFFSET + 1,
                                                                    Message = new Message()
                                                                },
                                                                new ResponseMessage
                                                                {
                                                                    Offset = OFFSET + 2,
                                                                    Message = new Message()
                                                                },
                                                            }
                                                    }
                                                }
                                        }
                                    }
                            }
                    },
                ReceivedDate = DateTime.UtcNow
            });

            consumer.StopConsume(TOPIC, PARTITION, OFFSET + 1); // Should correspond to OFFSET + 2

            node.Verify(n => n.Fetch(It.IsAny<FetchMessage>()), Times.Exactly(2));
            node.Verify(n => n.Fetch(It.Is<FetchMessage>(f => f.Offset == OFFSET)), Times.Once);
            node.Verify(n => n.Fetch(It.Is<FetchMessage>(f => f.Offset == OFFSET + 3)), Times.Once);

            // Should be ignored
            consumer.Acknowledge(new CommonAcknowledgement<FetchResponse>
            {
                Response =
                    new FetchResponse
                    {
                        FetchPartitionResponse =
                            new CommonResponse<FetchPartitionResponse>
                            {
                                TopicsResponse =
                                    new[]
                                    {
                                        new TopicData<FetchPartitionResponse>
                                        {
                                            TopicName = TOPIC,
                                            PartitionsData =
                                                new[]
                                                {
                                                    new FetchPartitionResponse
                                                    {
                                                        ErrorCode = ErrorCode.NoError,
                                                        Partition = PARTITION,
                                                        HighWatermarkOffset = 432515L,
                                                        Messages =
                                                            new List<ResponseMessage>
                                                            {
                                                                new ResponseMessage
                                                                {
                                                                    Offset = OFFSET + 3,
                                                                    Message = new Message()
                                                                },
                                                            }
                                                    }
                                                }
                                        }
                                    }
                            }
                    },
                ReceivedDate = DateTime.UtcNow
            });

            // No operation is pending, everything has been acknowledged, let's resume
            consumer.StartConsume(TOPIC, PARTITION, OFFSET + 1); // Should trigger fetch on OFFSET + 1 again

            // Check
            node.Verify(n => n.Fetch(It.IsAny<FetchMessage>()), Times.Exactly(3));
            node.Verify(n => n.Fetch(It.Is<FetchMessage>(f => f.Offset == OFFSET)), Times.Once);
            node.Verify(n => n.Fetch(It.Is<FetchMessage>(f => f.Offset == OFFSET + 3)), Times.Once);
            node.Verify(n => n.Fetch(It.Is<FetchMessage>(f => f.Offset == OFFSET + 1)), Times.Once);
            Assert.That(offsets, Is.EquivalentTo(new[] { OFFSET, OFFSET + 1, OFFSET + 2 }));
        }

        [Test]
        // Test restart on a stopped partition when there are no pending operation
        // i.e. Restart is sent after Stop has been fully completed
        public void TestResume_NoOperationPending()
        {
            var node = new Mock<INode>();
            var cluster = new Mock<ICluster>();
            cluster.Setup(c => c.RequireNewRoutingTable())
                .Returns(() =>
                    Task.FromResult(
                        new RoutingTable(new Dictionary<string, Partition[]>
                        {
                            {
                                TOPIC,
                                new[]
                                {
                                    new Partition {Id = 0, Leader = node.Object},
                                    new Partition {Id = 1, Leader = node.Object},
                                    new Partition {Id = 2, Leader = node.Object}
                                }
                            }
                        })));
            var configuration = new Configuration { TaskScheduler = new CurrentThreadTaskScheduler() };
            var consumer = new ConsumeRouter(cluster.Object, configuration, 1);

            var offsets = new List<long>();
            consumer.MessageReceived += r => offsets.Add(r.Offset);

            consumer.StartConsume(TOPIC, PARTITION, OFFSET);

            consumer.Acknowledge(new CommonAcknowledgement<FetchResponse>
            {
                Response = new FetchResponse { FetchPartitionResponse = new CommonResponse<FetchPartitionResponse>
                {
                    TopicsResponse = new[]
                    {
                        new TopicData<FetchPartitionResponse>
                        {
                            TopicName = TOPIC,
                            PartitionsData = new[]
                            {
                                new FetchPartitionResponse
                                {
                                    ErrorCode = ErrorCode.NoError,
                                    Partition = PARTITION,
                                    HighWatermarkOffset = 432515L,
                                    Messages = new List<ResponseMessage>
                                    {
                                        new ResponseMessage {Offset = OFFSET, Message = new Message()},
                                        new ResponseMessage {Offset = OFFSET + 1, Message = new Message()},
                                        new ResponseMessage {Offset = OFFSET + 2, Message = new Message()},
                                    }
                                }
                            }
                        }
                    }
                }},
                ReceivedDate = DateTime.UtcNow
            });

            consumer.StopConsume(TOPIC, PARTITION, Offsets.Now); // Should correspond to OFFSET + 2

            node.Verify(n => n.Fetch(It.IsAny<FetchMessage>()), Times.Exactly(2));
            node.Verify(n => n.Fetch(It.Is<FetchMessage>(f => f.Offset == OFFSET)), Times.Once);
            node.Verify(n => n.Fetch(It.Is<FetchMessage>(f => f.Offset == OFFSET + 3)), Times.Once);

            // Should be ignored
            consumer.Acknowledge(new CommonAcknowledgement<FetchResponse>
            {
                Response = new FetchResponse { FetchPartitionResponse = new CommonResponse<FetchPartitionResponse>
                {
                    TopicsResponse = new[]
                    {
                        new TopicData<FetchPartitionResponse>
                        {
                            TopicName = TOPIC,
                            PartitionsData = new[]
                            {
                                new FetchPartitionResponse
                                {
                                    ErrorCode = ErrorCode.NoError,
                                    Partition = PARTITION,
                                    HighWatermarkOffset = 432515L,
                                    Messages = new List<ResponseMessage>
                                    {
                                        new ResponseMessage {Offset = OFFSET + 3, Message = new Message()},
                                    }
                                }
                            }
                        }
                    }
                }},
                ReceivedDate = DateTime.UtcNow
            });

            // No operation is pending, everything has been acknowledged, let's resume
            consumer.StartConsume(TOPIC, PARTITION, Offsets.Now); // Should trigger fetch on OFFSET + 3 again

            // Check
            node.Verify(n => n.Fetch(It.IsAny<FetchMessage>()), Times.Exactly(3));
            node.Verify(n => n.Fetch(It.Is<FetchMessage>(f => f.Offset == OFFSET + 3)), Times.Exactly(2));
            Assert.That(offsets, Is.EquivalentTo(new[] { OFFSET, OFFSET + 1, OFFSET + 2 }));
        }

        [Test]
        // Test restart on a stopped partition when there are still pending operations
        // i.e. Restart is sent while previous stop operation is not fully completed
        public void TestResume_FetchPending()
        {
            var node = new Mock<INode>();
            var cluster = new Mock<ICluster>();
            cluster.Setup(c => c.RequireNewRoutingTable())
                .Returns(() =>
                    Task.FromResult(
                        new RoutingTable(new Dictionary<string, Partition[]>
                        {
                            {
                                TOPIC,
                                new[]
                                {
                                    new Partition {Id = 0, Leader = node.Object},
                                    new Partition {Id = 1, Leader = node.Object},
                                    new Partition {Id = 2, Leader = node.Object}
                                }
                            }
                        })));
            var configuration = new Configuration { TaskScheduler = new CurrentThreadTaskScheduler() };
            var consumer = new ConsumeRouter(cluster.Object, configuration, 1);

            var offsets = new List<long>();
            consumer.MessageReceived += r => offsets.Add(r.Offset);

            consumer.StartConsume(TOPIC, PARTITION, OFFSET);

            consumer.Acknowledge(new CommonAcknowledgement<FetchResponse>
            {
                Response = new FetchResponse { FetchPartitionResponse = new CommonResponse <FetchPartitionResponse>
                {
                    TopicsResponse = new[]
                    {
                        new TopicData<FetchPartitionResponse>
                        {
                            TopicName = TOPIC,
                            PartitionsData = new[]
                            {
                                new FetchPartitionResponse
                                {
                                    ErrorCode = ErrorCode.NoError,
                                    Partition = PARTITION,
                                    HighWatermarkOffset = 432515L,
                                    Messages = new List<ResponseMessage>
                                    {
                                        new ResponseMessage {Offset = OFFSET, Message = new Message()},
                                        new ResponseMessage {Offset = OFFSET + 1, Message = new Message()},
                                        new ResponseMessage {Offset = OFFSET + 2, Message = new Message()},
                                    }
                                }
                            }
                        }
                    }
                }},
                ReceivedDate = DateTime.UtcNow
            });

            consumer.StopConsume(TOPIC, PARTITION, Offsets.Now); // Should correspond to OFFSET + 2

            node.Verify(n => n.Fetch(It.IsAny<FetchMessage>()), Times.Exactly(2));
            node.Verify(n => n.Fetch(It.Is<FetchMessage>(f => f.Offset == OFFSET)), Times.Once);
            node.Verify(n => n.Fetch(It.Is<FetchMessage>(f => f.Offset == OFFSET + 3)), Times.Once);

            // Fetch operation is pending, let's resume
            consumer.StartConsume(TOPIC, PARTITION, Offsets.Now); // Should not trigger anything

            // No Fetch, we're still waiting fro previous Fetch to be acknowledged
            node.Verify(n => n.Fetch(It.IsAny<FetchMessage>()), Times.Exactly(2));

            // Should trigger new Fetch
            consumer.Acknowledge(new CommonAcknowledgement<FetchResponse>
            {
                Response = new FetchResponse { FetchPartitionResponse = new CommonResponse<FetchPartitionResponse>
                {
                    TopicsResponse = new[]
                    {
                        new TopicData<FetchPartitionResponse>
                        {
                            TopicName = TOPIC,
                            PartitionsData = new[]
                            {
                                new FetchPartitionResponse
                                {
                                    ErrorCode = ErrorCode.NoError,
                                    Partition = PARTITION,
                                    HighWatermarkOffset = 432515L,
                                    Messages = new List<ResponseMessage>
                                    {
                                        new ResponseMessage {Offset = OFFSET + 3, Message = new Message()},
                                    }
                                }
                            }
                        }
                    }
                }},
                ReceivedDate = DateTime.UtcNow
            });

            // Check
            node.Verify(n => n.Fetch(It.IsAny<FetchMessage>()), Times.Exactly(3));
            node.Verify(n => n.Fetch(It.Is<FetchMessage>(f => f.Offset == OFFSET + 4)), Times.Once);
            Assert.That(offsets, Is.EquivalentTo(new[] { OFFSET, OFFSET + 1, OFFSET + 2, OFFSET + 3 }));
        }

        [Test]
        // Test restart on a stopped partition when there are still list offset pending operations
        // i.e. Restart is sent while previous stop operation is not fully completed and stop was sent
        // while we were waiting for a list operation to complete
        public void TestResume_OffsetPending()
        {
            var node = new Mock<INode>();
            var cluster = new Mock<ICluster>();
            cluster.Setup(c => c.RequireNewRoutingTable())
                .Returns(() =>
                    Task.FromResult(
                        new RoutingTable(new Dictionary<string, Partition[]>
                        {
                            {
                                TOPIC,
                                new[]
                                {
                                    new Partition {Id = 0, Leader = node.Object},
                                    new Partition {Id = 1, Leader = node.Object},
                                    new Partition {Id = 2, Leader = node.Object}
                                }
                            }
                        })));
            var configuration = new Configuration { TaskScheduler = new CurrentThreadTaskScheduler() };
            var consumer = new ConsumeRouter(cluster.Object, configuration, 1);

            consumer.StartConsume(TOPIC, PARTITION, Offsets.Latest);

            consumer.StopConsume(TOPIC, PARTITION, Offsets.Now);

            // Check no fetch has been sent
            node.Verify(n => n.Offset(It.IsAny<OffsetMessage>()), Times.Once);
            node.Verify(n => n.Fetch(It.IsAny<FetchMessage>()), Times.Never());

            // Offset operation is pending, let's resume
            consumer.StartConsume(TOPIC, PARTITION, Offsets.Now);

            node.Verify(n => n.Fetch(It.IsAny<FetchMessage>()), Times.Never);

            // Should trigger new Fetch
            consumer.Acknowledge(new CommonAcknowledgement<CommonResponse<OffsetPartitionResponse>>
            {
                Response = new CommonResponse<OffsetPartitionResponse>
                {
                    TopicsResponse = new[]
                    {
                        new TopicData<OffsetPartitionResponse>
                        {
                            TopicName = TOPIC,
                            PartitionsData =
                                new[]
                                {
                                    new OffsetPartitionResponse
                                    {
                                        ErrorCode = ErrorCode.NoError,
                                        Partition = PARTITION,
                                        Offsets = new[] {OFFSET}
                                    }
                                }
                        }
                    }
                },
                ReceivedDate = DateTime.UtcNow
            });

            // Check
            node.Verify(n => n.Fetch(It.IsAny<FetchMessage>()), Times.Once);
            node.Verify(n => n.Fetch(It.Is<FetchMessage>(f => f.Offset == OFFSET)), Times.Once);
        }

        [Test]
        public void TestOffsetResponseIsFollowedByFetchRequest_NoError()
        {
            var node = new Mock<INode>();
            node.Setup(n => n.Fetch(It.IsAny<FetchMessage>())).Returns(true);
            node.Setup(n => n.Offset(It.IsAny<OffsetMessage>())).Returns(true);
            var cluster = new Mock<ICluster>();
            cluster.Setup(c => c.RequireNewRoutingTable())
                .Returns(() =>
                    Task.FromResult(
                        new RoutingTable(new Dictionary<string, Partition[]>
                        {
                            {
                                TOPIC,
                                new[]
                                {
                                    new Partition {Id = PARTITION, Leader = node.Object},
                                    new Partition {Id = PARTITION + 1, Leader = node.Object},
                                }
                            },
                            {
                                TOPIC2,
                                new[]
                                {
                                    new Partition {Id = PARTITION, Leader = node.Object},
                                }
                            }
                        })));
            var configuration = new Configuration {TaskScheduler = new CurrentThreadTaskScheduler()};
            var consumer = new ConsumeRouter(cluster.Object, configuration, 1);
            consumer.StartConsume(TOPIC, PARTITION, Offsets.Earliest);
            consumer.StartConsume(TOPIC, PARTITION + 1, Offsets.Earliest);
            consumer.StartConsume(TOPIC2, PARTITION, Offsets.Earliest);
            consumer.Acknowledge(new CommonAcknowledgement<CommonResponse<OffsetPartitionResponse>>
            {
                Response = new CommonResponse<OffsetPartitionResponse>
                {
                    TopicsResponse = new[]
                    {
                        new TopicData<OffsetPartitionResponse>
                        {
                            TopicName = TOPIC,
                            PartitionsData = new[]
                            {
                                new OffsetPartitionResponse
                                {
                                    ErrorCode = ErrorCode.NoError,
                                    Partition = PARTITION,
                                    Offsets = new[] {OFFSET}
                                },
                                new OffsetPartitionResponse
                                {
                                    ErrorCode = ErrorCode.NoError,
                                    Partition = PARTITION + 1,
                                    Offsets = new[] {OFFSET}
                                }
                            }
                        },
                        new TopicData<OffsetPartitionResponse>
                        {
                            TopicName = TOPIC2,
                            PartitionsData = new[]
                            {
                                new OffsetPartitionResponse
                                {
                                    ErrorCode = ErrorCode.NoError,
                                    Partition = PARTITION,
                                    Offsets = new[] {OFFSET}
                                },
                            }
                        }
                    }
                },
                ReceivedDate = DateTime.UtcNow
            });
            node.Verify(n => n.Fetch(It.IsAny<FetchMessage>()), Times.Exactly(3));
            node.Verify(n => n.Fetch(It.Is<FetchMessage>(fm => fm.Offset == OFFSET)), Times.Exactly(3));
            node.Verify(n => n.Fetch(It.Is<FetchMessage>(fm => fm.Topic == TOPIC)), Times.Exactly(2));
            node.Verify(n => n.Fetch(It.Is<FetchMessage>(fm => fm.Topic == TOPIC2)), Times.Exactly(1));
        }

        [Test]
        public void TestFetchResponseIsFollowedByFetchRequest_NoError()
        {
            var node = new Mock<INode>();
            node.Setup(n => n.Fetch(It.IsAny<FetchMessage>())).Returns(true);
            node.Setup(n => n.Offset(It.IsAny<OffsetMessage>())).Returns(true);
            var cluster = new Mock<ICluster>();
            cluster.Setup(c => c.RequireNewRoutingTable())
                .Returns(() =>
                    Task.FromResult(
                        new RoutingTable(new Dictionary<string, Partition[]>
                        {
                            {
                                TOPIC,
                                new[]
                                {
                                    new Partition {Id = PARTITION, Leader = node.Object},
                                    new Partition {Id = PARTITION + 1, Leader = node.Object},
                                }
                            },
                            {
                                TOPIC2,
                                new[]
                                {
                                    new Partition {Id = PARTITION, Leader = node.Object},
                                }
                            }
                        })));
            var configuration = new Configuration {TaskScheduler = new CurrentThreadTaskScheduler()};
            var consumer = new ConsumeRouter(cluster.Object, configuration, 1);
            consumer.StartConsume(TOPIC, PARTITION, Offsets.Earliest);
            consumer.StartConsume(TOPIC, PARTITION + 1, Offsets.Earliest);
            consumer.StartConsume(TOPIC2, PARTITION, Offsets.Earliest);
            consumer.Acknowledge(new CommonAcknowledgement<FetchResponse>
            {
                Response = new FetchResponse { FetchPartitionResponse = new CommonResponse<FetchPartitionResponse>
                {
                    TopicsResponse = new[]
                    {
                        new TopicData<FetchPartitionResponse>
                        {
                            TopicName = TOPIC,
                            PartitionsData = new[]
                            {
                                new FetchPartitionResponse
                                {
                                    ErrorCode = ErrorCode.NoError,
                                    Partition = PARTITION,
                                    HighWatermarkOffset = 432515L,
                                    Messages = new List<ResponseMessage>
                                    {
                                        new ResponseMessage {Offset = OFFSET, Message = new Message()},
                                        new ResponseMessage {Offset = OFFSET + 1, Message = new Message()},
                                    }
                                },
                                new FetchPartitionResponse
                                {
                                    ErrorCode = ErrorCode.NoError,
                                    Partition = PARTITION + 1,
                                    HighWatermarkOffset = 432515L,
                                    Messages = new List<ResponseMessage>
                                    {
                                        new ResponseMessage {Offset = OFFSET, Message = new Message()},
                                    }
                                },
                            }
                        },
                        new TopicData<FetchPartitionResponse>
                        {
                            TopicName = TOPIC2,
                            PartitionsData = new[]
                            {
                                new FetchPartitionResponse
                                {
                                    ErrorCode = ErrorCode.NoError,
                                    Partition = PARTITION,
                                    HighWatermarkOffset = 432515L,
                                    Messages = new List<ResponseMessage>
                                    {
                                        new ResponseMessage {Offset = OFFSET, Message = new Message()},
                                        new ResponseMessage {Offset = OFFSET + 1, Message = new Message()},
                                        new ResponseMessage {Offset = OFFSET + 2, Message = new Message()},
                                    }
                                },
                            }
                        }
                    }
                }},
                ReceivedDate = DateTime.UtcNow
            });
            node.Verify(n => n.Fetch(It.IsAny<FetchMessage>()), Times.Exactly(3));
            node.Verify(n => n.Fetch(It.Is<FetchMessage>(fm => fm.Topic == TOPIC)), Times.Exactly(2));
            node.Verify(
                n =>
                    n.Fetch(
                        It.Is<FetchMessage>(
                            fm => fm.Topic == TOPIC && fm.Partition == PARTITION && fm.Offset == OFFSET + 2)),
                Times.Once());
            node.Verify(
                n =>
                    n.Fetch(
                        It.Is<FetchMessage>(
                            fm => fm.Topic == TOPIC && fm.Partition == PARTITION + 1 && fm.Offset == OFFSET + 1)),
                Times.Once());
            node.Verify(
                n =>
                    n.Fetch(
                        It.Is<FetchMessage>(
                            fm => fm.Topic == TOPIC2 && fm.Offset == OFFSET + 3 && fm.Partition == PARTITION)),
                Times.Exactly(1));
        }

        [Test]
        public void TestMessagesAreFilteredAndPropagated()
        {
            var node = new Mock<INode>();
            var cluster = new Mock<ICluster>();
            cluster.Setup(c => c.RequireNewRoutingTable())
                .Returns(() =>
                    Task.FromResult(
                        new RoutingTable(new Dictionary<string, Partition[]>
                        {
                            {
                                TOPIC,
                                new[]
                                {
                                    new Partition {Id = PARTITION, Leader = node.Object},
                                }
                            }
                        })));
            var configuration = new Configuration {TaskScheduler = new CurrentThreadTaskScheduler()};
            var consumer = new ConsumeRouter(cluster.Object, configuration, 1);
            consumer.StartConsume(TOPIC, PARTITION, OFFSET);
            consumer.StopConsume(TOPIC, PARTITION, OFFSET + 1);
            var offsets = new List<long>();
            var partitions = new List<int>();
            var topics = new List<string>();
            consumer.MessageReceived += kr =>
            {
                offsets.Add(kr.Offset);
                partitions.Add(kr.Partition);
                topics.Add(kr.Topic);
            };
            consumer.Acknowledge(new CommonAcknowledgement<FetchResponse>
            {
                Response = new FetchResponse { FetchPartitionResponse = new CommonResponse<FetchPartitionResponse>
                {
                    TopicsResponse = new[]
                    {
                        new TopicData<FetchPartitionResponse>
                        {
                            TopicName = TOPIC,
                            PartitionsData = new[]
                            {
                                new FetchPartitionResponse
                                {
                                    ErrorCode = ErrorCode.NoError,
                                    Partition = PARTITION,
                                    HighWatermarkOffset = 432515L,
                                    Messages = new List<ResponseMessage>
                                    {
                                        new ResponseMessage {Offset = OFFSET - 1, Message = new Message()}, // Will be filtered
                                        new ResponseMessage {Offset = OFFSET, Message = new Message()},
                                        new ResponseMessage {Offset = OFFSET + 1, Message = new Message()},
                                        new ResponseMessage {Offset = OFFSET + 2, Message = new Message()},
                                    }
                                }
                            }
                        }
                    }
                }},
                ReceivedDate = DateTime.UtcNow
            });

            Assert.That(offsets, Is.EquivalentTo(new[] { OFFSET, OFFSET + 1 }));
            Assert.That(partitions, Is.EquivalentTo(new[] { PARTITION, PARTITION }));
            Assert.That(topics, Is.EquivalentTo(new[] { TOPIC, TOPIC }));
        }

        [Test]
        public void TestPostponeIfNoRoute()
        {
            var cluster = new Mock<ICluster>();
            cluster.SetupGet(c => c.Logger).Returns(new DevNullLogger());
            cluster.Setup(c => c.RequireNewRoutingTable())
                .Returns(() =>
                    Task.FromResult(
                        new RoutingTable(new Dictionary<string, Partition[]>
                        {
                            {
                                TOPIC,
                                new[]
                                {
                                    new Partition {Id = PARTITION + 1},
                                }
                            }
                        })));
            var configuration = new Configuration { TaskScheduler = new CurrentThreadTaskScheduler() };
            var consumer = new ConsumeRouter(cluster.Object, configuration, 1);
            int postponedRaised = 0;
            consumer.FetchPostponed += (t, p) =>
            {
                Assert.AreEqual(TOPIC, t);
                Assert.AreEqual(PARTITION, p);
                ++postponedRaised;
            };
            consumer.StartConsume(TOPIC, PARTITION, Offsets.Earliest);

            Assert.AreEqual(1, postponedRaised);
        }

        [Test]
        public void TestPostponeIfNodeDead()
        {
            var node = new Mock<INode>();
            var cluster = new Mock<ICluster>();
            cluster.SetupGet(c => c.Logger).Returns(new DevNullLogger());
            cluster.Setup(c => c.RequireNewRoutingTable())
                .Returns(
                    () =>
                        Task.FromResult(
                            new RoutingTable(new Dictionary<string, Partition[]>
                            {
                                { TOPIC, new[] { new Partition { Id = PARTITION, Leader = node.Object }, } }
                            })));

            var configuration = new Configuration
            {
                TaskScheduler = new CurrentThreadTaskScheduler(),
                BatchStrategy = BatchStrategy.Global,
                ConsumeBatchSize = 1,
                ConsumeBufferingTime = TimeSpan.FromMilliseconds(1000000)
            };

            var consumer = new ConsumeRouter(cluster.Object, configuration, 1);
            string topic = "";
            int partition = -1;
            consumer.FetchPostponed += (t, p) =>
            {
                topic = t;
                partition = p;
            };

            consumer.StartConsume(TOPIC, PARTITION, OFFSET);
            
            Assert.AreEqual(TOPIC, topic);
            Assert.AreEqual(PARTITION, partition);
        }

        [Test]
        public void TestThrottled()
        {
            var node = new Mock<INode>();
            var cluster = new Mock<ICluster>();
            cluster.Setup(c => c.RequireNewRoutingTable())
                .Returns(
                    () =>
                        Task.FromResult(
                            new RoutingTable(new Dictionary<string, Partition[]>
                            {
                                { TOPIC, new[] { new Partition { Id = 0, Leader = node.Object }, } }
                            })));
            var configuration = new Configuration { TaskScheduler = new CurrentThreadTaskScheduler() };
            var consumer = new ConsumeRouter(cluster.Object, configuration, 1);

            int throttled = -1;
            consumer.Throttled += t => throttled = t;

            consumer.StartConsume(TOPIC, PARTITION, OFFSET);
            consumer.Acknowledge(new CommonAcknowledgement<FetchResponse>
            {
                Response =
                    new FetchResponse
                    {
                        ThrottleTime = 42,
                        FetchPartitionResponse =
                            new CommonResponse<FetchPartitionResponse>
                            {
                                TopicsResponse =
                                    new[]
                                    {
                                        new TopicData<FetchPartitionResponse>
                                        {
                                            TopicName = TOPIC,
                                            PartitionsData =
                                                new[]
                                                {
                                                    new FetchPartitionResponse
                                                    {
                                                        ErrorCode = ErrorCode.NoError,
                                                        Partition = PARTITION,
                                                        HighWatermarkOffset = 432515L,
                                                        Messages =
                                                            new List<ResponseMessage>
                                                            {
                                                                new ResponseMessage
                                                                {
                                                                    Offset = OFFSET,
                                                                    Message = new Message()
                                                                },
                                                            }
                                                    }
                                                }
                                        }
                                    }
                            }
                    },
                ReceivedDate = DateTime.UtcNow
            });
            
            Assert.AreEqual(42, throttled);
        }
    }
}
