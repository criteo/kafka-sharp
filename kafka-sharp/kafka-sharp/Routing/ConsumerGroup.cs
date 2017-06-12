// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Kafka.Cluster;
using Kafka.Protocol;
using Kafka.Public;

namespace Kafka.Routing
{
    struct PartitionOffset : IEquatable<PartitionOffset>
    {
        public int Partition;
        public long Offset;

        public bool Equals(PartitionOffset other)
        {
            return Partition == other.Partition;
        }

        public override int GetHashCode()
        {
            return Partition;
        }
    }

    struct PartitionAssignments
    {
        public ErrorCode ErrorCode;
        public IDictionary<string, ISet<PartitionOffset>> Assignments;
    }

    interface IConsumerGroup
    {
        ConsumerGroupConfiguration Configuration { get; }
        string GroupId { get; }
        string MemberId { get; }
        int Generation { get; }

        Task<PartitionAssignments> Join(IEnumerable<string> topics);
        Task<ErrorCode> Heartbeat();
        Task<ErrorCode> LeaveGroup();

        Task<IEnumerable<TopicData<PartitionCommitData>>> Commit(IEnumerable<TopicData<OffsetCommitPartitionData>> topicsData);
    }

    /// <summary>
    /// A class managing one consumer group
    /// </summary>
    class ConsumerGroup : IConsumerGroup
    {
        private static readonly IDictionary<string, ISet<PartitionOffset>> EmptyAssignment =
            new Dictionary<string, ISet<PartitionOffset>>();
        private readonly ICluster _cluster;
        private INode _coordinator;
        public int Generation { get; private set; }

        public ConsumerGroupConfiguration Configuration { get; private set; }
        public string GroupId { get; private set; }
        public string MemberId { get; private set; }

        public ConsumerGroup(string groupId, ConsumerGroupConfiguration configuration, ICluster cluster)
        {
            GroupId = groupId;
            MemberId = "";
            Configuration = configuration;
            Generation = -1;
            _cluster = cluster;
        }

        public async Task<PartitionAssignments> Join(IEnumerable<string> topics)
        {
            try
            {
                await RefreshCoordinator();

                // JoinGroup request
                var joinResponse =
                    await
                        _coordinator.JoinConsumerGroup(GroupId, MemberId, Configuration.SessionTimeoutMs,
                            Configuration.RebalanceTimeoutMs, topics);

                Generation = joinResponse.GenerationId;
                MemberId = joinResponse.MemberId ?? "";

                if (joinResponse.ErrorCode != ErrorCode.NoError)
                {
                    HandleError(joinResponse.ErrorCode);
                    return new PartitionAssignments
                    {
                        ErrorCode = joinResponse.ErrorCode,
                        Assignments = EmptyAssignment
                    };
                }

                bool isLeader = joinResponse.GroupMembers.Length > 0;
                _cluster.Logger.LogInformation(
                    string.Format("Consumer group \"{0}\" joined. Member id is \"{1}\", generation is {2}.{3}", GroupId,
                        MemberId, Generation, isLeader ? " I am leader." : ""));

                // SyncGroup request
                var assignments = isLeader
                    ? await LeaderAssign(joinResponse.GroupMembers)
                    : Enumerable.Empty<ConsumerGroupAssignment>();
                var syncResponse = await _coordinator.SyncConsumerGroup(GroupId, MemberId, Generation, assignments);

                _cluster.Logger.LogInformation(string.Format("Consumer group \"{0}\" synced. Assignments: {1}", GroupId,
                    string.Join(", ",
                        syncResponse.MemberAssignment.PartitionAssignments.Select(
                            td =>
                                string.Format("{0}:[{1}]", td.TopicName,
                                    string.Join(",", td.PartitionsData.Select(p => p.Partition.ToString())))))));

                // Empty assignments, no need to fetch offsets
                if (!syncResponse.MemberAssignment.PartitionAssignments.Any()
                    || joinResponse.ErrorCode != ErrorCode.NoError)
                {
                    HandleError(joinResponse.ErrorCode);
                    return new PartitionAssignments
                    {
                        ErrorCode = syncResponse.ErrorCode,
                        Assignments = EmptyAssignment
                    };
                }

                // FetchOffsets request (retrieve saved offsets)
                var offsets =
                    await _coordinator.FetchOffsets(GroupId, syncResponse.MemberAssignment.PartitionAssignments);

                // Returns assignments, each one with the offset we have have to start to read from
                return new PartitionAssignments
                {
                    ErrorCode = syncResponse.ErrorCode,
                    Assignments =
                        offsets.TopicsResponse.ToDictionary(assignment => assignment.TopicName,
                            assignment =>
                                new HashSet<PartitionOffset>(
                                    assignment.PartitionsData.Select(
                                        p =>
                                            new PartitionOffset
                                            {
                                                Partition = p.Partition,
                                                Offset = p.Offset < 0
                                                    ? Offset(Configuration.DefaultOffsetToReadFrom)
                                                    // read from whatever is configured offset (end or start)
                                                    : p.Offset // read from saved offset
                                            })) as ISet<PartitionOffset>)
                };
            }
            catch
            {
                // Something worng occured during the workflow (typically a timeout).
                // Reset Generation in case the problem occured after Join.
                Generation = -1;
                throw;
            }
        }

        private async Task RefreshCoordinator()
        {
            _coordinator = null;
            while (_coordinator == null)
            {
                try
                {
                    _coordinator = await _cluster.GetGroupCoordinator(GroupId);
                }
                catch (Exception ex)
                {
                    _cluster.Logger.LogError(string.Format("Error while retrieving group coordinator. {0}", ex));
                }
                if (_coordinator == null)
                {
                    await Task.Delay(Configuration.CoordinatorDiscoveryRetryTime);
                }
            }
        }

        private async Task<IEnumerable<ConsumerGroupAssignment>> LeaderAssign(IList<GroupMember> members)
        {
            // Leader! We have to assign partitions to members while syncing
            var topicBag = new HashSet<string>(members.SelectMany(m => m.Metadata.Subscription));
            var partitions = await _cluster.RequireAllPartitionsForTopics(topicBag);
            return
                AssignPartitions(members, partitions)
                    .Select(
                        kv =>
                            new ConsumerGroupAssignment
                            {
                                MemberId = kv.Key,
                                MemberAssignment =
                                    new ConsumerGroupMemberAssignment
                                    {
                                        Version = 0,
                                        PartitionAssignments =
                                            kv.Value.Select(
                                                tp =>
                                                    new TopicData<PartitionAssignment>
                                                    {
                                                        TopicName = tp.Key,
                                                        PartitionsData =
                                                            tp.Value.Select(
                                                                p => new PartitionAssignment { Partition = p })
                                                    }),
                                        UserData = null
                                    }
                            });
        }

        private class Member : IComparable<Member>
        {
            public string Id;
            public int Weight;

            public int CompareTo(Member other)
            {
                return Weight - other.Weight;
            }
        }

        private static IEnumerable<KeyValuePair<string, IDictionary<string, IList<int>>>> AssignPartitions(
            IList<GroupMember> members, IEnumerable<KeyValuePair<string, int[]>> topicPartitions)
        {
            // Build assignments container and prepopulate it
            var result = members.ToDictionary<GroupMember, string, IDictionary<string, IList<int>>>(m => m.MemberId,
                _ => new Dictionary<string, IList<int>>());
            foreach (var member in members)
            {
                foreach (var topic in member.Metadata.Subscription)
                {
                    result[member.MemberId].Add(topic, new List<int>());
                }
            }

            // Index who is subscribing to what
            var subscribedByTopic = new Dictionary<string, HashSet<string>>();
            foreach (var member in members)
            {
                foreach (var topic in member.Metadata.Subscription)
                {
                    HashSet<string> subscribed;
                    if (!subscribedByTopic.TryGetValue(topic, out subscribed))
                    {
                        subscribed = new HashSet<string>();
                        subscribedByTopic.Add(topic, subscribed);
                    }
                    subscribed.Add(member.MemberId);
                }
            }

            // Member list ordered by weight
            var mbrs = members.Select(m => new Member { Id = m.MemberId }).ToArray();

            // Iterate over topics and assign their partitions
            foreach (var tp in topicPartitions)
            {
                // sort members from least partitions assigned to max so we assign
                // partitions first to members with least partitions assigned
                Array.Sort(mbrs);

                // keep members that are subscribing to the current topic
                var filtered = mbrs.Where(m => subscribedByTopic[tp.Key].Contains(m.Id)).ToArray();

                // Round robin assign partitions to filtered members
                int midx = 0;
                foreach (var p in tp.Value)
                {
                    result[filtered[midx].Id][tp.Key].Add(p);
                    filtered[midx].Weight += 1;
                    midx = (midx + 1) % filtered.Length;
                }
            }

            return result;
        }

        public async Task<ErrorCode> Heartbeat()
        {
            var result = await _coordinator.Heartbeat(GroupId, Generation, MemberId);
            HandleError(result);
            return result;
        }

        public Task<ErrorCode> LeaveGroup()
        {
            var t = _coordinator.LeaveGroup(GroupId, MemberId);
            MemberId = "";
            return t;
        }

        public async Task<IEnumerable<TopicData<PartitionCommitData>>> Commit(
            IEnumerable<TopicData<OffsetCommitPartitionData>> topicsData)
        {
            return
                (await _coordinator.Commit(GroupId, Generation, MemberId, Configuration.OffsetRetentionTimeMs, topicsData))
                    .TopicsResponse;
        }

        private void HandleError(ErrorCode error)
        {
            switch (error)
            {
                case ErrorCode.InconsistentGroupProtocol:
                case ErrorCode.UnknownMemberId:
                    MemberId = "";
                    break;

                default:
                    break;
            }
        }

        private static long Offset(Offset offset)
        {
            switch (offset)
            {
                case Public.Offset.Earliest:
                    return -2;

                case Public.Offset.Lastest:
                    return -1;

                default:
                    return -1;
            }
        }
    }
}