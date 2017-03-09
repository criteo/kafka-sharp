using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel.Design;
using System.Linq;
using System.Runtime.InteropServices.WindowsRuntime;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Kafka.Batching;
using Kafka.Cluster;
using Kafka.Protocol;
using Kafka.Public;
using Kafka.Public.Loggers;
using Kafka.Routing;
using NUnit.Framework;
using Moq;

namespace tests_kafka_sharp
{
    [TestFixture]
    class TestConsumerGroup
    {
        [Test]
        public void TestConsumerGroup_New()
        {
            var group = new ConsumerGroup("tutugroup", new ConsumerGroupConfiguration(), new Mock<ICluster>().Object);
            Assert.AreEqual("tutugroup", group.GroupId);
            Assert.IsEmpty(group.MemberId);
            Assert.AreEqual(-1, group.Generation);
        }

        struct Mocks
        {
            public Mock<ICluster> Cluster;
            public Mock<INode> Node;
            public Mock<IConsumerGroup> Group;
        }

        Mocks InitCluster()
        {
            var cluster = new Mock<ICluster>();
            var node = new Mock<INode>();
            var group = new Mock<IConsumerGroup>();

            cluster.Setup(c => c.GetGroupCoordinator(It.IsAny<string>())).ReturnsAsync(node.Object);
            cluster.SetupGet(c => c.Logger).Returns(new DevNullLogger());
            cluster.Setup(c => c.RequireAllPartitionsForTopics(It.IsAny<IEnumerable<string>>()))
                .Returns(
                    (IEnumerable<string> topics) =>
                        Task.FromResult<IDictionary<string, int[]>>(topics.ToDictionary(topic => topic,
                            topic => new[] { 1, 2, 3 })));
            cluster.Setup(c => c.RequireNewRoutingTable())
                .ReturnsAsync(
                    new RoutingTable(new Dictionary<string, Partition[]>
                    {
                        {
                            "the topic",
                            new[]
                            {
                                new Partition { Id = 1, Leader = node.Object },
                                new Partition { Id = 2, Leader = node.Object },
                                new Partition { Id = 3, Leader = node.Object },
                            }
                        }
                    }));

            node.Setup(
                n =>
                    n.JoinConsumerGroup(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<int>(), It.IsAny<int>(),
                        It.IsAny<IEnumerable<string>>()))
                .ReturnsAsync(new JoinConsumerGroupResponse
                {
                    ErrorCode = ErrorCode.NoError,
                    GenerationId = 42,
                    MemberId = "member1",
                    LeaderId = "member2",
                    GroupProtocol = "kafka-sharp-consumer",
                    GroupMembers = new GroupMember[0]
                });
            node.Setup(
                n =>
                    n.SyncConsumerGroup(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<int>(),
                        It.IsAny<IEnumerable<ConsumerGroupAssignment>>()))
                .ReturnsAsync(new SyncConsumerGroupResponse
                {
                    ErrorCode = ErrorCode.NoError,
                    MemberAssignment =
                        new ConsumerGroupMemberAssignment
                        {
                            Version = 0,
                            UserData = null,
                            PartitionAssignments =
                                new[]
                                {
                                    new TopicData<PartitionAssignment>
                                    {
                                        TopicName = "the topic",
                                        PartitionsData = new[] { new PartitionAssignment { Partition = 1 }, }
                                    }
                                }
                        }
                });
            node.Setup(n => n.FetchOffsets(It.IsAny<string>(), It.IsAny<IEnumerable<TopicData<PartitionAssignment>>>()))
                .ReturnsAsync(new CommonResponse<PartitionOffsetData>
                {
                    TopicsResponse =
                        new[]
                        {
                            new TopicData<PartitionOffsetData>
                            {
                                TopicName = "the topic",
                                PartitionsData =
                                    new[]
                                    {
                                        new PartitionOffsetData
                                        {
                                            ErrorCode = ErrorCode.NoError,
                                            Metadata = "",
                                            Offset = 28,
                                            Partition = 1
                                        },
                                        new PartitionOffsetData
                                        {
                                            ErrorCode = ErrorCode.NoError,
                                            Metadata = "",
                                            Offset = -1,
                                            Partition = 1
                                        }
                                    }
                            }
                        }
                });
            node.Setup(
                n =>
                    n.Commit(It.IsAny<string>(), It.IsAny<int>(), It.IsAny<string>(), It.IsAny<long>(),
                        It.IsAny<IEnumerable<TopicData<OffsetCommitPartitionData>>>()))
                .Returns(
                    (string gid, int g, string mid, long t, IEnumerable<TopicData<OffsetCommitPartitionData>> tds) =>
                        Task.FromResult(new CommonResponse<PartitionCommitData>
                        {
                            TopicsResponse =
                                tds.Select(
                                    td =>
                                        new TopicData<PartitionCommitData>
                                        {
                                            TopicName = td.TopicName,
                                            PartitionsData =
                                                td.PartitionsData.Select(
                                                    d =>
                                                        new PartitionCommitData
                                                        {
                                                            ErrorCode = ErrorCode.NoError,
                                                            Partition = d.Partition
                                                        })
                                        }).ToArray()
                        }));
            node.Setup(n => n.Post(It.IsAny<IBatchByTopic<FetchMessage>>())).Returns(true);
            node.Setup(n => n.Post(It.IsAny<IBatchByTopic<OffsetMessage>>())).Returns(true);
            node.Setup(n => n.Fetch(It.IsAny<FetchMessage>())).Returns(true);
            node.Setup(n => n.Offset(It.IsAny<OffsetMessage>())).Returns(true);

            group.Setup(g => g.Join(It.IsAny<IEnumerable<string>>()))
                .Returns(
                    (IEnumerable<string> topics) =>
                        Task.FromResult(new PartitionAssignments
                        {
                            Assignments =
                                topics.ToDictionary(t => t,
                                    t =>
                                        new HashSet<PartitionOffset>(new[]
                                        {
                                            new PartitionOffset { Partition = 1, Offset = 28 },
                                            new PartitionOffset { Partition = 2, Offset = -1 }
                                        }) as
                                            ISet<PartitionOffset>)
                        }));
            group.SetupGet(g => g.Configuration)
                .Returns(new ConsumerGroupConfiguration { AutoCommitEveryMs = 10, SessionTimeoutMs = 10 });

            return new Mocks { Cluster = cluster, Node = node, Group = group };
        }

        [Test]
        public async Task TestConsumerGroup_JoinLeader()
        {
            IEnumerable<ConsumerGroupAssignment> totalAssignments = Enumerable.Empty<ConsumerGroupAssignment>();
            var subscription = new[] { "the topic" };
            var mocks = InitCluster();
            var group = new ConsumerGroup("the group", new ConsumerGroupConfiguration(), mocks.Cluster.Object);
            mocks.Node.Setup(
                n =>
                    n.JoinConsumerGroup(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<int>(), It.IsAny<int>(),
                        It.IsAny<IEnumerable<string>>()))
                .Returns(
                    (string groupid, string member, int s, int r, IEnumerable<string> sub) =>
                        Task.FromResult(new JoinConsumerGroupResponse
                        {
                            ErrorCode = ErrorCode.NoError,
                            GenerationId = 42,
                            MemberId = "member1",
                            LeaderId = "member1",
                            GroupProtocol = "kafka-sharp-consumer",
                            GroupMembers =
                                new[]
                                {
                                    new GroupMember
                                    {
                                        MemberId = "member1",
                                        Metadata = new ConsumerGroupProtocolMetadata { Subscription = subscription }
                                    },
                                    new GroupMember
                                    {
                                        MemberId = "member2",
                                        Metadata = new ConsumerGroupProtocolMetadata { Subscription = subscription }
                                    },
                                    new GroupMember
                                    {
                                        MemberId = "member3",
                                        Metadata =
                                            new ConsumerGroupProtocolMetadata { Subscription = new[] { "the tipoc" } }
                                    },
                                    new GroupMember
                                    {
                                        MemberId = "member4",
                                        Metadata =
                                            new ConsumerGroupProtocolMetadata
                                            {
                                                Subscription = new[] { "the topic", "the tipoc" }
                                            }
                                    },
                                }
                        }));
            mocks.Node.Setup(
                n =>
                    n.SyncConsumerGroup(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<int>(),
                        It.IsAny<IEnumerable<ConsumerGroupAssignment>>()))
                .Callback((string s1, string s2, int i, IEnumerable<ConsumerGroupAssignment> a) => totalAssignments = a) // Capture real assignments made
                .ReturnsAsync(new SyncConsumerGroupResponse
                {
                    ErrorCode = ErrorCode.NoError,
                    MemberAssignment =
                        new ConsumerGroupMemberAssignment
                        {
                            Version = 0,
                            UserData = null,
                            PartitionAssignments =
                                new[]
                                {
                                    new TopicData<PartitionAssignment>
                                    {
                                        TopicName = "the topic",
                                        PartitionsData = new[] { new PartitionAssignment { Partition = 1 }, }
                                    }
                                }
                        }
                });

            var assignments = await group.Join(subscription);

            // Check answers
            Assert.AreEqual(ErrorCode.NoError, assignments.ErrorCode);
            Assert.That(
                new Dictionary<string, HashSet<PartitionOffset>>()
                {
                    {
                        "the topic",
                        new HashSet<PartitionOffset>(new[] { new PartitionOffset { Partition = 1, Offset = 28 } })
                    }
                },
                Is.EquivalentTo(assignments.Assignments));

            // Check assignments sent
            var a1 = totalAssignments.First(ga => ga.MemberId == "member1");
            var a2 = totalAssignments.First(ga => ga.MemberId == "member2");
            var a3 = totalAssignments.First(ga => ga.MemberId == "member3");
            var a4 = totalAssignments.First(ga => ga.MemberId == "member4");

            Assert.AreEqual(1, a1.MemberAssignment.PartitionAssignments.Count());
            Assert.AreEqual(1, a2.MemberAssignment.PartitionAssignments.Count());
            Assert.AreEqual(1, a3.MemberAssignment.PartitionAssignments.Count());
            Assert.AreEqual(2, a4.MemberAssignment.PartitionAssignments.Count());

            Assert.AreEqual("the topic", a1.MemberAssignment.PartitionAssignments.First().TopicName);
            Assert.AreEqual("the topic", a2.MemberAssignment.PartitionAssignments.First().TopicName);
            Assert.AreEqual("the tipoc", a3.MemberAssignment.PartitionAssignments.First().TopicName);
            Assert.That(a4.MemberAssignment.PartitionAssignments.Select(td => td.TopicName), Is.EquivalentTo(new[] { "the topic", "the tipoc" }));

            var all =
                totalAssignments.SelectMany(
                    a =>
                        a.MemberAssignment.PartitionAssignments.SelectMany(
                            td => td.PartitionsData.Select(p => Tuple.Create(td.TopicName, p.Partition))));
            Assert.AreEqual(6, all.Count()); // 2 topic with 3 partitions each
            var tester = new List<Tuple<string, int>>();
            foreach (var t in all)
            {
                Assert.IsFalse(tester.Contains(t));
                tester.Add(t);
            }
        }

        [Test]
        public async Task TestConsumerGroup_Join()
        {
            var mocks = InitCluster();
            var group = new ConsumerGroup("the group", new ConsumerGroupConfiguration(), mocks.Cluster.Object);
            var assignments = await group.Join(new[] { "the topic" });

            Assert.AreEqual("member1", group.MemberId);
            Assert.AreEqual(42, group.Generation);
            Assert.AreEqual(ErrorCode.NoError, assignments.ErrorCode);
            Assert.That(
                new Dictionary<string, HashSet<PartitionOffset>>()
                {
                    {
                        "the topic",
                        new HashSet<PartitionOffset>(new[] { new PartitionOffset { Partition = 1, Offset = 28 } })
                    }
                },
                Is.EquivalentTo(assignments.Assignments));
            mocks.Cluster.Verify(c => c.GetGroupCoordinator("the group"), Times.Once());
            mocks.Node.Verify(n => n.JoinConsumerGroup("the group", "", 15000, 10000, new[] { "the topic" }), Times.Once);
            mocks.Node.Verify(
                n => n.SyncConsumerGroup("the group", "member1", 42, Enumerable.Empty<ConsumerGroupAssignment>()),
                Times.Once);
            mocks.Node.Verify(
                n =>
                    n.FetchOffsets("the group",
                        It.Is<IEnumerable<TopicData<PartitionAssignment>>>(
                            a =>
                                a.Count() == 1 && a.First().TopicName == "the topic"
                                    && a.First().PartitionsData.Count() == 1
                                    && a.First().PartitionsData.First().Partition == 1)), Times.Once);
        }

        [Test]
        public async Task TestConsumerGroup_JoinNoAssignments()
        {
            var mocks = InitCluster();
            var group = new ConsumerGroup("the group", new ConsumerGroupConfiguration(), mocks.Cluster.Object);
            mocks.Node.Setup(
                n =>
                    n.SyncConsumerGroup(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<int>(),
                        It.IsAny<IEnumerable<ConsumerGroupAssignment>>()))
                .ReturnsAsync(new SyncConsumerGroupResponse
                {
                    ErrorCode = ErrorCode.NoError,
                    MemberAssignment =
                        new ConsumerGroupMemberAssignment
                        {
                            Version = 0,
                            UserData = null,
                            PartitionAssignments = Enumerable.Empty<TopicData<PartitionAssignment>>()
                        }
                });
            var assignments = await group.Join(new[] { "the topic" });
            Assert.That(assignments.Assignments, Is.Empty);
        }

        [Test]
        public async Task TestConsumerGroup_Commit()
        {
            var mocks = InitCluster();
            var group = new ConsumerGroup("the group", new ConsumerGroupConfiguration {OffsetRetentionTimeMs = 27000}, mocks.Cluster.Object);
            var assignments = await group.Join(new[] { "the topic" });
            await
                group.Commit(new[]
                {
                    new TopicData<OffsetCommitPartitionData>
                    {
                        TopicName = "the topic",
                        PartitionsData =
                            new[] { new OffsetCommitPartitionData { Metadata = "meta", Offset = 32, Partition = 1 } }
                    }
                });

            mocks.Node.Verify(
                n =>
                    n.Commit("the group", 42, "member1", 27000,
                        It.Is<IEnumerable<TopicData<OffsetCommitPartitionData>>>(
                            l =>
                                l.Count() == 1 && l.First().TopicName == "the topic"
                                    && l.First().PartitionsData.Count() == 1
                                    && l.First().PartitionsData.First().Partition == 1
                                    && l.First().PartitionsData.First().Metadata == "meta"
                                    && l.First().PartitionsData.First().Offset == 32)));
        }

        [Test]
        public async Task TestConsumerGroup_Heartbeat()
        {
            var mocks = InitCluster();
            var group = new ConsumerGroup("the group", new ConsumerGroupConfiguration(), mocks.Cluster.Object);
            var assignments = await group.Join(new[] { "the topic" });

            var res = await group.Heartbeat();

            Assert.AreEqual(ErrorCode.NoError, res);
            mocks.Node.Verify(n => n.Heartbeat("the group", 42, "member1"));

            mocks.Node.Setup(n => n.Heartbeat(It.IsAny<string>(), It.IsAny<int>(), It.IsAny<string>()))
                .ReturnsAsync(ErrorCode.UnknownMemberId);

            res = await group.Heartbeat();

            Assert.AreEqual(ErrorCode.UnknownMemberId, res);
            Assert.AreEqual("", group.MemberId);
        }

        [Test]
        public async Task TestConsumerGroup_Leave()
        {
            var mocks = InitCluster();
            var group = new ConsumerGroup("the group", new ConsumerGroupConfiguration(), mocks.Cluster.Object);
            var assignments = await group.Join(new[] { "the topic" });
            await group.LeaveGroup();

            mocks.Node.Verify(n => n.LeaveGroup("the group", "member1"));
        }

        [Test]
        public async Task TestConsumerGroup_JoinErrorJoin()
        {
            var mocks = InitCluster();
            var group = new ConsumerGroup("the group", new ConsumerGroupConfiguration(), mocks.Cluster.Object);

            mocks.Node.Setup(
                n =>
                    n.JoinConsumerGroup(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<int>(), It.IsAny<int>(),
                        It.IsAny<IEnumerable<string>>()))
                .ReturnsAsync(new JoinConsumerGroupResponse { ErrorCode = ErrorCode.InconsistentGroupProtocol, });

            var assignments = await group.Join(new[] { "the topic" });
            Assert.AreEqual(ErrorCode.InconsistentGroupProtocol, assignments.ErrorCode);
        }

        [Test]
        public async Task TestConsumerGroup_JoinErrorSync()
        {
            var mocks = InitCluster();
            var group = new ConsumerGroup("the group", new ConsumerGroupConfiguration(), mocks.Cluster.Object);

            mocks.Node.Setup(
                n =>
                    n.SyncConsumerGroup(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<int>(),
                        It.IsAny<IEnumerable<ConsumerGroupAssignment>>()))
                .ReturnsAsync(new SyncConsumerGroupResponse
                {
                    ErrorCode = ErrorCode.InconsistentGroupProtocol,
                    MemberAssignment =
                        new ConsumerGroupMemberAssignment
                        {
                            PartitionAssignments = Enumerable.Empty<TopicData<PartitionAssignment>>()
                        }
                });

            var assignments = await group.Join(new[] { "the topic" });
            Assert.AreEqual(ErrorCode.InconsistentGroupProtocol, assignments.ErrorCode);
        }

        [Test]
        public void TestConsumer_ConsumerGroupStartConsume()
        {
            var mocks = InitCluster();
            var consumer = new ConsumeRouter(mocks.Cluster.Object,
                new Configuration { TaskScheduler = new CurrentThreadTaskScheduler(), ConsumeBatchSize = 1 }, 1);
            
            consumer.StartConsumeSubscription(mocks.Group.Object, new[] { "the topic" });

            mocks.Group.Verify(g => g.Join(It.IsAny<IEnumerable<string>>()), Times.Once);
            mocks.Node.Verify(n => n.Fetch(It.IsAny<FetchMessage>()), Times.Once); // 1 partition with specific offset
            mocks.Node.Verify(n => n.Offset(It.IsAny<OffsetMessage>()), Times.Once); // 1 partition with offset -1

            Thread.Sleep(20); // wait for at least one heartbeat to be sent

            mocks.Group.Verify(g => g.Heartbeat());

            consumer.Acknowledge(new CommonAcknowledgement<FetchPartitionResponse>
            {
                ReceivedDate = DateTime.UtcNow,
                Response =
                    new CommonResponse<FetchPartitionResponse>
                    {
                        TopicsResponse =
                            new[]
                            {
                                new TopicData<FetchPartitionResponse>
                                {
                                    TopicName = "the topic",
                                    PartitionsData =
                                        new[]
                                        {
                                            new FetchPartitionResponse
                                            {
                                                Partition = 1,
                                                Messages =
                                                    new List<ResponseMessage>
                                                    {
                                                        new ResponseMessage { Offset = 28, Message = new Message() }
                                                    }
                                            }
                                        }
                                }
                            }
                    }
            });

            mocks.Node.Verify(n => n.Fetch(It.IsAny<FetchMessage>()), Times.Exactly(2)); // response should have triggered one more fetch
            mocks.Group.Verify(g => g.Commit(It.IsAny<IEnumerable<TopicData<OffsetCommitPartitionData>>>())); // should have auto commited

            consumer.Stop().Wait();
        }

        [Test]
        public void TestConsumer_ConsumerGroupRestartConsume()
        {
            var mocks = InitCluster();
            var consumer = new ConsumeRouter(mocks.Cluster.Object,
                new Configuration { TaskScheduler = new CurrentThreadTaskScheduler(), ConsumeBatchSize = 1 }, 1);

            consumer.StartConsumeSubscription(mocks.Group.Object, new[] { "the topic" });

            mocks.Group.Verify(g => g.Join(It.IsAny<IEnumerable<string>>()), Times.Once);
            mocks.Node.Verify(n => n.Fetch(It.IsAny<FetchMessage>()), Times.Once); // 1 partition with specific offset
            mocks.Node.Verify(n => n.Offset(It.IsAny<OffsetMessage>()), Times.Once); // 1 partition with offset -1

            Thread.Sleep(20); // wait for at least one heartbeat to be sent

            mocks.Group.Verify(g => g.Heartbeat());

            consumer.Acknowledge(new CommonAcknowledgement<FetchPartitionResponse>
            {
                ReceivedDate = DateTime.UtcNow,
                Response =
                    new CommonResponse<FetchPartitionResponse>
                    {
                        TopicsResponse =
                            new[]
                            {
                                new TopicData<FetchPartitionResponse>
                                {
                                    TopicName = "the topic",
                                    PartitionsData =
                                        new[]
                                        {
                                            new FetchPartitionResponse
                                            {
                                                Partition = 1,
                                                Messages =
                                                    new List<ResponseMessage>
                                                    {
                                                        new ResponseMessage { Offset = 28, Message = new Message() }
                                                    }
                                            }
                                        }
                                }
                            }
                    }
            });

            mocks.Node.Verify(n => n.Fetch(It.IsAny<FetchMessage>()), Times.Exactly(2)); // response should have triggered one more fetch

            consumer.StopConsume("the topic", Partitions.All, Offsets.Now);

            consumer.Acknowledge(new CommonAcknowledgement<FetchPartitionResponse>
            {
                ReceivedDate = DateTime.UtcNow,
                Response =
                    new CommonResponse<FetchPartitionResponse>
                    {
                        TopicsResponse =
                            new[]
                            {
                                new TopicData<FetchPartitionResponse>
                                {
                                    TopicName = "the topic",
                                    PartitionsData =
                                        new[]
                                        {
                                            new FetchPartitionResponse
                                            {
                                                Partition = 1,
                                                Messages =
                                                    new List<ResponseMessage>
                                                    {
                                                        new ResponseMessage { Offset = 29, Message = new Message() }
                                                    }
                                            }
                                        }
                                }
                            }
                    }
            });

            consumer.StartConsume("the topic", Partitions.All, Offsets.Now);

            mocks.Node.Verify(n => n.Fetch(It.IsAny<FetchMessage>()), Times.Exactly(3));

            consumer.Stop().Wait();
        }

        [Test]
        public void TestConsumer_OnlyOneConsumerGroup()
        {
            var mocks = InitCluster();
            var consumer = new ConsumeRouter(mocks.Cluster.Object,
                new Configuration { TaskScheduler = new CurrentThreadTaskScheduler(), ConsumeBatchSize = 1 }, 1);

            consumer.StartConsumeSubscription(mocks.Group.Object, new[] { "the topic" });
            Assert.That(() => consumer.StartConsumeSubscription(mocks.Group.Object, new[] { "the topic" }), Throws.ArgumentException);
        }

        [Test]
        public void TestConsumer_ConsumerGroupCommit()
        {
            var mocks = InitCluster();
            mocks.Group.SetupGet(g => g.Configuration)
                .Returns(new ConsumerGroupConfiguration { AutoCommitEveryMs = -1, SessionTimeoutMs = 10 });
            var consumer = new ConsumeRouter(mocks.Cluster.Object,
                new Configuration { TaskScheduler = new CurrentThreadTaskScheduler(), ConsumeBatchSize = 1 }, 1);
            
            consumer.StartConsumeSubscription(mocks.Group.Object, new[] { "the topic" });

            Thread.Sleep(10); // wait for at least one heartbeat to be sent

            consumer.Acknowledge(new CommonAcknowledgement<FetchPartitionResponse>
            {
                ReceivedDate = DateTime.UtcNow,
                Response =
                    new CommonResponse<FetchPartitionResponse>
                    {
                        TopicsResponse =
                            new[]
                            {
                                new TopicData<FetchPartitionResponse>
                                {
                                    TopicName = "the topic",
                                    PartitionsData =
                                        new[]
                                        {
                                            new FetchPartitionResponse
                                            {
                                                Partition = 1,
                                                Messages =
                                                    new List<ResponseMessage>
                                                    {
                                                        new ResponseMessage { Offset = 28, Message = new Message() }
                                                    }
                                            }
                                        }
                                }
                            }
                    }
            });

            mocks.Group.Verify(g => g.Commit(It.IsAny<IEnumerable<TopicData<OffsetCommitPartitionData>>>()), Times.Never); // no auto commit

            consumer.RequireCommit();
            mocks.Group.Verify(g => g.Commit(It.Is<IEnumerable<TopicData<OffsetCommitPartitionData>>>(l =>
                                l.Count() == 1 && l.First().TopicName == "the topic"
                                    && l.First().PartitionsData.Count() == 2
                                    && l.First().PartitionsData.First().Partition == 1
                                    && l.First().PartitionsData.First().Metadata == ""
                                    && l.First().PartitionsData.First().Offset == 29)), // Offset saved should be next expected offset
                                    Times.Once);
        }

        [Test]
        public async Task TestConsumer_ConsumerGroupCommitAsync()
        {
            var mocks = InitCluster();
            mocks.Group.SetupGet(g => g.Configuration)
                .Returns(new ConsumerGroupConfiguration { AutoCommitEveryMs = -1, SessionTimeoutMs = 10 });
            var consumer = new ConsumeRouter(mocks.Cluster.Object,
                new Configuration { TaskScheduler = new CurrentThreadTaskScheduler(), ConsumeBatchSize = 1 }, 1);
            
            consumer.StartConsumeSubscription(mocks.Group.Object, new[] { "the topic" });

            await consumer.CommitAsync("the topic", 1, 42);

            mocks.Group.Verify(g => g.Commit(It.Is<IEnumerable<TopicData<OffsetCommitPartitionData>>>(l =>
                                l.Count() == 1 && l.First().TopicName == "the topic"
                                    && l.First().PartitionsData.Count() == 1
                                    && l.First().PartitionsData.First().Partition == 1
                                    && l.First().PartitionsData.First().Metadata == ""
                                    && l.First().PartitionsData.First().Offset == 42)), // Offset saved should be next expected offset
                                    Times.Once);

            mocks.Group.Setup(g => g.Commit(It.IsAny<IEnumerable<TopicData<OffsetCommitPartitionData>>>()))
                .ThrowsAsync(new ApplicationException());

            Assert.Throws<ApplicationException>(async () => await consumer.CommitAsync("the topic", 1, 42));
        }

        [Test]
        public void TestConsumer_ConsumerGroupHeartbeatErrors()
        {
            var mocks = InitCluster();
            mocks.Group.SetupGet(g => g.Configuration)
                .Returns(new ConsumerGroupConfiguration { AutoCommitEveryMs = -1, SessionTimeoutMs = 10 });
            var consumer = new ConsumeRouter(mocks.Cluster.Object,
                new Configuration { TaskScheduler = new CurrentThreadTaskScheduler(), ConsumeBatchSize = 1 }, 1);

            mocks.Group.Setup(g => g.Heartbeat()).ReturnsAsync(ErrorCode.RebalanceInProgress);
            
            consumer.StartConsumeSubscription(mocks.Group.Object, new[] { "the topic" });

            Thread.Sleep(50); // wait for at least one heartbeat to be sent
            
            // At least 2 Join (one on start, one on next heartbeat)
            mocks.Group.Verify(g => g.Join(It.IsAny<IEnumerable<string>>()), Times.AtLeast(2));
            // Commit should have been called due to RebalanceInProgressError
            mocks.Group.Verify(g => g.Commit(It.IsAny<IEnumerable<TopicData<OffsetCommitPartitionData>>>()));
            consumer.Stop().Wait();

            mocks = InitCluster();
            mocks.Group.SetupGet(g => g.Configuration)
                .Returns(new ConsumerGroupConfiguration { AutoCommitEveryMs = -1, SessionTimeoutMs = 10 });
            mocks.Group.Setup(g => g.Heartbeat()).ThrowsAsync(new Exception());
            consumer = new ConsumeRouter(mocks.Cluster.Object,
                new Configuration { TaskScheduler = new CurrentThreadTaskScheduler(), ConsumeBatchSize = 1 }, 1);
            
            consumer.StartConsumeSubscription(mocks.Group.Object, new[] { "the topic" });

            Thread.Sleep(20); // wait for at least one heartbeat to be sent

            mocks.Group.Verify(g => g.Join(It.IsAny<IEnumerable<string>>()), Times.AtLeast(2));
            // No Commit tried in case of ""hard" heartbeat errors
            mocks.Group.Verify(g => g.Commit(It.IsAny<IEnumerable<TopicData<OffsetCommitPartitionData>>>()), Times.Never);
        }

        [Test]
        public async Task TestConsumer_ConsumerGroupLeaveWhenStop()
        {
            var mocks = InitCluster();
            mocks.Group.SetupGet(g => g.Configuration)
                .Returns(new ConsumerGroupConfiguration { AutoCommitEveryMs = -1, SessionTimeoutMs = 10 });
            var consumer = new ConsumeRouter(mocks.Cluster.Object,
                new Configuration { TaskScheduler = new CurrentThreadTaskScheduler(), ConsumeBatchSize = 1 }, 1);

            mocks.Group.Setup(g => g.Heartbeat()).ReturnsAsync(ErrorCode.RebalanceInProgress);
            
            consumer.StartConsumeSubscription(mocks.Group.Object, new[] { "the topic" });

            await consumer.Stop();

            mocks.Group.Verify(g => g.LeaveGroup(), Times.Once);
            mocks.Group.Verify(g => g.Commit(It.IsAny<IEnumerable<TopicData<OffsetCommitPartitionData>>>()));
        }
    }
}