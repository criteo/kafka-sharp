using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Kafka.Common;
using Kafka.Protocol;
using Kafka.Public;
using NUnit.Framework;

namespace tests_kafka_sharp
{
    [TestFixture]
    class TestSerialization
    {
        private static readonly string TheKey = "The Key Opens the Door";
        private static readonly string TheValue = "The quick brown fox jumps over the zealy god.";
        private static readonly string TheClientId = "ClientId";
        private static readonly byte[] Value = Encoding.UTF8.GetBytes(TheValue);
        private static readonly byte[] Key = Encoding.UTF8.GetBytes(TheKey);
        private static readonly byte[] ClientId = Encoding.UTF8.GetBytes(TheClientId);

        private static readonly int FullMessageSize = 4 + 1 + 1 + 4 + TheKey.Length + 4 + TheValue.Length;
        private static readonly int NullKeyMessageSize = 4 + 1 + 1 + 4 + 4 + TheValue.Length;

        static void CompareArrays<T>(T[] expected, T[] compared, int offset)
        {
            for (int i = 0; i < expected.Length; ++i)
            {
                Assert.AreEqual(expected[i], compared[i + offset]);
            }
        }

        static void CompareBuffers(byte[] expected, ReusableMemoryStream compared)
        {
            CompareArrays(expected, compared.GetBuffer(), (int) compared.Position);
            compared.Position += expected.Length;
        }

        [Test]
        public void TestSerializeOneMessage()
        {
            using (var serialized = ReusableMemoryStream.Reserve())
            {
                var message = new Message {Key = Key, Value = Value};
                message.Serialize(serialized, CompressionCodec.None);
                Assert.AreEqual(FullMessageSize, serialized.Length);
                Assert.AreEqual(0, serialized.GetBuffer()[4]); // magic byte is 0
                Assert.AreEqual(0, serialized.GetBuffer()[5]); // attributes is 0
                serialized.Position = 6;
                Assert.AreEqual(TheKey.Length, BigEndianConverter.ReadInt32(serialized));
                CompareBuffers(Key, serialized);
                Assert.AreEqual(TheValue.Length, BigEndianConverter.ReadInt32(serialized));
                CompareBuffers(Value, serialized);
            }

            using (var serialized = ReusableMemoryStream.Reserve())
            {
                var message = new Message { Value = Value };
                message.Serialize(serialized, CompressionCodec.None);
                Assert.AreEqual(NullKeyMessageSize, serialized.Length);
                Assert.AreEqual(0, serialized.GetBuffer()[4]); // magic byte is 0
                Assert.AreEqual(0, serialized.GetBuffer()[5]); // attributes is 0
                serialized.Position = 6;
                Assert.AreEqual(-1, BigEndianConverter.ReadInt32(serialized));
                Assert.AreEqual(TheValue.Length, BigEndianConverter.ReadInt32(serialized));
                CompareBuffers(Value, serialized);
            }
        }

        [Test]
        public void TestSerializeOneMessageCodec()
        {
            // Just check attributes, we don't put correct data
            using (var serialized = ReusableMemoryStream.Reserve())
            {
                var message = new Message { Value = Value };
                message.Serialize(serialized, CompressionCodec.Snappy);
                Assert.AreEqual(0, serialized.GetBuffer()[4]); // magic byte is 0
                Assert.AreEqual(2, serialized.GetBuffer()[5]); // attributes is 2
            }

            using (var serialized = ReusableMemoryStream.Reserve())
            {
                var message = new Message { Value = Value };
                message.Serialize(serialized, CompressionCodec.Gzip);
                Assert.AreEqual(0, serialized.GetBuffer()[4]); // magic byte is 0
                Assert.AreEqual(1, serialized.GetBuffer()[5]); // attributes is 1
            }
        }

        [Test]
        public void TestSerializeMessageSet()
        {
            using (var serialized = ReusableMemoryStream.Reserve())
            {
                var set = new PartitionData
                {
                    Partition = 42,
                    CompressionCodec = CompressionCodec.None,
                    Messages = new[]
                    {
                        new Message {Key = Key, Value = Value},
                        new Message {Key = Key, Value = Value}
                    }
                };
                set.Serialize(serialized);
                serialized.Position = 0;

                Assert.AreEqual(42, BigEndianConverter.ReadInt32(serialized)); // Partition
                Assert.AreEqual(serialized.Length - 4 - 4, BigEndianConverter.ReadInt32(serialized)); // MessageSet size
                Assert.AreEqual(0, BigEndianConverter.ReadInt64(serialized)); // First message offset
                int firstMsgSize = BigEndianConverter.ReadInt32(serialized);
                serialized.Position += firstMsgSize;
                Assert.AreEqual(0, BigEndianConverter.ReadInt64(serialized)); // First message offset
                int secondMsgSize = BigEndianConverter.ReadInt32(serialized);
                serialized.Position += secondMsgSize;
                Assert.AreEqual(serialized.Length, serialized.Position);
            }
        }

        private static void TestSerializeMessageSetCompressed(CompressionCodec codec, byte attr)
        {
            using (var serialized = ReusableMemoryStream.Reserve())
            {
                var set = new PartitionData
                {
                    Partition = 42,
                    CompressionCodec = codec,
                    Messages = new[]
                    {
                        new Message {Key = Key, Value = Value},
                        new Message {Key = Key, Value = Value}
                    }
                };
                set.Serialize(serialized);
                serialized.Position = 0;

                Assert.AreEqual(42, BigEndianConverter.ReadInt32(serialized)); // Partition
                Assert.AreEqual(serialized.Length - 4 - 4, BigEndianConverter.ReadInt32(serialized)); // MessageSet size
                Assert.AreEqual(0, BigEndianConverter.ReadInt64(serialized)); // First message offset
                int firstMsgSize = BigEndianConverter.ReadInt32(serialized);
                serialized.Position += firstMsgSize;
                Assert.AreEqual(serialized.Length, serialized.Position);
                int msgPos = 4 + 4 + 8 + 4; // partition, msgset size, offset, msg size
                int valuePos = msgPos + 4 + 1 + 1 + 4 + 4; // + crc, magic, attr, key size, value size
                serialized.Position = msgPos;
                serialized.Position += 5;
                Assert.AreEqual(attr, serialized.ReadByte()); // attributes
                Assert.AreEqual(-1, BigEndianConverter.ReadInt32(serialized)); // No key => size = -1
                Assert.AreEqual(serialized.Length - valuePos, BigEndianConverter.ReadInt32(serialized)); // check rest of message length match
            }
        }

        [Test]
        public void TestSerializeMessageSetGzip()
        {
            TestSerializeMessageSetCompressed(CompressionCodec.Gzip, 1);
        }

        [Test]
        public void TestSerializeMessageSetSnappy()
        {
            TestSerializeMessageSetCompressed(CompressionCodec.Snappy, 2);
        }

        private static void TestDeserializeMessageSet(CompressionCodec codec)
        {
            using (var serialized = ReusableMemoryStream.Reserve())
            {
                var set = new PartitionData
                {
                    Partition = 42,
                    CompressionCodec = codec,
                    Messages = new[]
                    {
                        new Message {Key = Key, Value = Value},
                        new Message {Key = Key, Value = Value}
                    }
                };
                set.Serialize(serialized);
                serialized.Position = 4;

                var deserialized = FetchPartitionResponse.DeserializeMessageSet(serialized);
                Assert.AreEqual(2, deserialized.Count);
                foreach (var msg in deserialized)
                {
                    Assert.AreEqual(0, msg.Offset);
                    CollectionAssert.AreEqual(Key, msg.Message.Key);
                    CollectionAssert.AreEqual(Value, msg.Message.Value);
                }
            }
        }

        [Test]
        public void TestDeserializeMessageSetNoCodec()
        {
            TestDeserializeMessageSet(CompressionCodec.None);
        }

        [Test]
        public void TestDeserializeMessageSetGzip()
        {
            TestDeserializeMessageSet(CompressionCodec.Gzip);
        }

        [Test]
        public void TestDeserializeMessageSetSnappy()
        {
            TestDeserializeMessageSet(CompressionCodec.Snappy);
        }

        private static void CheckHeader(
            Basics.ApiKey apiKey,
            short apiVersion,
            int correlationId,
            string clientId,
            ReusableMemoryStream stream)
        {
            Assert.AreEqual(stream.Length - 4, BigEndianConverter.ReadInt32(stream)); // Size
            Assert.AreEqual((short)apiKey, BigEndianConverter.ReadInt16(stream));
            Assert.AreEqual(apiVersion, BigEndianConverter.ReadInt16(stream));
            Assert.AreEqual(correlationId, BigEndianConverter.ReadInt32(stream));
            Assert.AreEqual(clientId, Basics.DeserializeString(stream));
        }

        [Test]
        public void TestSerializeMetadataRequest()
        {
            var meta = new TopicRequest
            {
                Topics = new[] { "poulpe", "banana" }
            };
            using (var serialized = meta.Serialize(61, ClientId))
            {
                CheckHeader(Basics.ApiKey.MetadataRequest, 0, 61, TheClientId, serialized);
                Assert.AreEqual(2, BigEndianConverter.ReadInt32(serialized));
                Assert.AreEqual("poulpe", Basics.DeserializeString(serialized));
                Assert.AreEqual("banana", Basics.DeserializeString(serialized));
            }

            meta = new TopicRequest
            {
                Topics = null
            };
            using (var serialized = meta.Serialize(61, ClientId))
            {
                CheckHeader(Basics.ApiKey.MetadataRequest, 0, 61, TheClientId, serialized);
                Assert.AreEqual(0, BigEndianConverter.ReadInt32(serialized));
                Assert.AreEqual(serialized.Length, serialized.Position);
            }
        }

        [Test]
        public void TestDeserializeMetadataResponse()
        {
            var meta = new MetadataResponse
            {
                BrokersMeta =
                    new[]
                    {
                        new BrokerMeta {Host = "Host", Id = 100, Port = 18909},
                        new BrokerMeta {Host = "tsoH", Id = 28, Port = 1}
                    },
                TopicsMeta =
                    new[]
                    {
                        new TopicMeta
                        {
                            ErrorCode = ErrorCode.NoError,
                            TopicName = "tropique",
                            Partitions =
                                new[]
                                {
                                    new PartitionMeta
                                    {
                                        ErrorCode = ErrorCode.LeaderNotAvailable,
                                        Id = 0,
                                        Leader = -1,
                                        Replicas = new[] {100},
                                        Isr = new int[0]
                                    }
                                }
                        }
                    }
            };
            using (var serialized = ReusableMemoryStream.Reserve())
            {
                meta.Serialize(serialized);
                Assert.AreEqual(74, serialized.Length); // TODO: better check that serialization is correct?

                serialized.Position = 0;
                var metaDeser = MetadataResponse.Deserialize(serialized);
                Assert.AreEqual(meta.BrokersMeta.Length, metaDeser.BrokersMeta.Length);
                Assert.AreEqual(meta.TopicsMeta.Length, metaDeser.TopicsMeta.Length);
                Assert.AreEqual(meta.BrokersMeta[0].Host, metaDeser.BrokersMeta[0].Host);
                Assert.AreEqual(meta.BrokersMeta[1].Host, metaDeser.BrokersMeta[1].Host);
                Assert.AreEqual(meta.BrokersMeta[0].Id, metaDeser.BrokersMeta[0].Id);
                Assert.AreEqual(meta.BrokersMeta[1].Id, metaDeser.BrokersMeta[1].Id);
                Assert.AreEqual(meta.BrokersMeta[0].Port, metaDeser.BrokersMeta[0].Port);
                Assert.AreEqual(meta.BrokersMeta[1].Port, metaDeser.BrokersMeta[1].Port);
                Assert.AreEqual("tropique", metaDeser.TopicsMeta[0].TopicName);
                Assert.AreEqual(ErrorCode.NoError, metaDeser.TopicsMeta[0].ErrorCode);
                Assert.AreEqual(ErrorCode.LeaderNotAvailable, metaDeser.TopicsMeta[0].Partitions[0].ErrorCode);
                Assert.AreEqual(0, metaDeser.TopicsMeta[0].Partitions[0].Id);
                Assert.AreEqual(-1, metaDeser.TopicsMeta[0].Partitions[0].Leader);
            }
        }

        [Test]
        public void TestSerializeFetchRequest()
        {
            var fetch = new FetchRequest
            {
                MaxWaitTime = 11111,
                MinBytes = 222222,
                TopicsData =
                    new[]
                    {
                        new TopicData<FetchPartitionData>
                        {
                            TopicName = "topic1",
                            PartitionsData =
                                new[] {new FetchPartitionData {FetchOffset = 1, MaxBytes = 333, Partition = 42}}
                        },
                        new TopicData<FetchPartitionData>
                        {
                            TopicName = "topic2",
                            PartitionsData =
                                new[]
                                {
                                    new FetchPartitionData {FetchOffset = 1, MaxBytes = 333, Partition = 43},
                                    new FetchPartitionData {FetchOffset = 2, MaxBytes = 89, Partition = 44}
                                }
                        }
                    }
            };
            using (var serialized = fetch.Serialize(1234, ClientId))
            {
                CheckHeader(Basics.ApiKey.FetchRequest, 0, 1234, TheClientId, serialized);
                Assert.AreEqual(-1, BigEndianConverter.ReadInt32(serialized)); // ReplicaId
                Assert.AreEqual(fetch.MaxWaitTime, BigEndianConverter.ReadInt32(serialized));
                Assert.AreEqual(fetch.MinBytes, BigEndianConverter.ReadInt32(serialized));
                Assert.AreEqual(2, BigEndianConverter.ReadInt32(serialized)); // 2 elements
                var td = new TopicData<FetchPartitionData>();
                td.Deserialize(serialized);
                Assert.AreEqual("topic1", td.TopicName);
                Assert.AreEqual(1, td.PartitionsData.Count());
                var f = td.PartitionsData.First();
                Assert.AreEqual(1, f.FetchOffset);
                Assert.AreEqual(333, f.MaxBytes);
                Assert.AreEqual(42, f.Partition);

                td.Deserialize(serialized);
                Assert.AreEqual("topic2", td.TopicName);
                Assert.AreEqual(2, td.PartitionsData.Count());
                f = td.PartitionsData.First();
                Assert.AreEqual(1, f.FetchOffset);
                Assert.AreEqual(333, f.MaxBytes);
                Assert.AreEqual(43, f.Partition);

                f = td.PartitionsData.ElementAt(1);
                Assert.AreEqual(2, f.FetchOffset);
                Assert.AreEqual(89, f.MaxBytes);
                Assert.AreEqual(44, f.Partition);
            }
        }

        [Test]
        public void TestSerializeProduceRequest()
        {
            var produce = new ProduceRequest
            {
                Timeout = 1223,
                RequiredAcks = 1,
                TopicsData = new[]
                {
                    new TopicData<PartitionData>
                    {
                        TopicName = "barbu",
                        PartitionsData = new[]
                        {
                            new PartitionData
                            {
                                Partition = 22,
                                CompressionCodec = CompressionCodec.None,
                                Messages = new[]
                                {
                                    new Message {Value = Value}
                                }
                            }
                        }
                    },
                }
            };

            using (var serialized = produce.Serialize(321, ClientId))
            {
                CheckHeader(Basics.ApiKey.ProduceRequest, 0, 321, TheClientId, serialized);
                Assert.AreEqual(produce.RequiredAcks, BigEndianConverter.ReadInt16(serialized));
                Assert.AreEqual(produce.Timeout, BigEndianConverter.ReadInt32(serialized));
                Assert.AreEqual(1, BigEndianConverter.ReadInt32(serialized)); // 1 topic data
                Assert.AreEqual("barbu", Basics.DeserializeString(serialized));
                Assert.AreEqual(1, BigEndianConverter.ReadInt32(serialized)); // 1 partition data
                Assert.AreEqual(22, BigEndianConverter.ReadInt32(serialized));
                var msgs = FetchPartitionResponse.DeserializeMessageSet(serialized);
                Assert.AreEqual(1, msgs.Count);
                Assert.AreEqual(TheValue, Encoding.UTF8.GetString(msgs[0].Message.Value));
            }
        }

        [Test]
        public void TestSerializeOffsetRequest()
        {
            var offset = new OffsetRequest
            {
                TopicsData = new[]
                {
                    new TopicData<OffsetPartitionData>
                    {
                        TopicName = "boloss",
                        PartitionsData = new[]
                        {
                            new OffsetPartitionData
                            {
                                MaxNumberOfOffsets = 3,
                                Partition = 123,
                                Time = 21341
                            }
                        }
                    }
                }
            };

            using (var serialized = offset.Serialize(1235, ClientId))
            {
                CheckHeader(Basics.ApiKey.OffsetRequest, 0, 1235, TheClientId, serialized);
                Assert.AreEqual(-1, BigEndianConverter.ReadInt32(serialized)); // ReplicaId
                Assert.AreEqual(1, BigEndianConverter.ReadInt32(serialized)); // 1 topic data
                Assert.AreEqual(offset.TopicsData.First().TopicName, Basics.DeserializeString(serialized));
                Assert.AreEqual(1, BigEndianConverter.ReadInt32(serialized)); // 1 partition data
                var od = new OffsetPartitionData();
                od.Deserialize(serialized);
                Assert.AreEqual(123, od.Partition);
                Assert.AreEqual(21341, od.Time);
                Assert.AreEqual(3, od.MaxNumberOfOffsets);
            }
        }

        private static void TestCommonResponse<TPartitionResponse>(ReusableMemoryStream serialized,
            CommonResponse<TPartitionResponse> expected, Func<TPartitionResponse, TPartitionResponse, bool> comparer)
            where TPartitionResponse : IMemoryStreamSerializable, new()
        {
            serialized.Position = 0;
            var response = CommonResponse<TPartitionResponse>.Deserialize(serialized);
            Assert.AreEqual(expected.TopicsResponse.Length, response.TopicsResponse.Length);
            foreach (var ztr in expected.TopicsResponse.Zip(response.TopicsResponse, Tuple.Create))
            {
                Assert.AreEqual(ztr.Item1.TopicName, ztr.Item2.TopicName);
                Assert.AreEqual(ztr.Item1.PartitionsData.Count(), ztr.Item2.PartitionsData.Count());
                foreach (var zpr in ztr.Item1.PartitionsData.Zip(ztr.Item2.PartitionsData, Tuple.Create))
                {
                    Assert.IsTrue(comparer(zpr.Item1, zpr.Item2));
                }
            }
        }

        [Test]
        public void TestDeserializeProduceResponse()
        {
            var response = new CommonResponse<ProducePartitionResponse>
            {
                TopicsResponse = new[]
                {
                    new TopicData<ProducePartitionResponse>
                    {
                        TopicName = "topic",
                        PartitionsData = new[]
                        {
                            new ProducePartitionResponse
                            {
                                ErrorCode = ErrorCode.InvalidMessage,
                                Offset = 2312,
                                Partition = 34
                            }
                        }
                    }
                }
            };

            using (var serialized = ReusableMemoryStream.Reserve())
            {
                Basics.WriteArray(serialized, response.TopicsResponse);

                TestCommonResponse(serialized, response, (p1, p2) => p1.Partition == p2.Partition && p1.Offset == p2.Offset && p1.ErrorCode == p2.ErrorCode);
            }
        }

        [Test]
        public void TestDeserializeOffsetResponse()
        {
            var response = new CommonResponse<OffsetPartitionResponse>
            {
                TopicsResponse = new []
                {
                    new TopicData<OffsetPartitionResponse>
                    {
                        TopicName = "yoleeroy",
                        PartitionsData = new[]{new OffsetPartitionResponse
                        {
                            ErrorCode = ErrorCode.BrokerNotAvailable,
                            Partition = 32,
                            Offsets = new []{1L, 142L}
                        }}
                    }
                }
            };

            using (var serialized = ReusableMemoryStream.Reserve())
            {
                Basics.WriteArray(serialized, response.TopicsResponse);

                TestCommonResponse(serialized, response, (p1, p2) =>
                {
                    if (p1.Partition != p2.Partition)
                        return false;
                    if (p1.ErrorCode != p2.ErrorCode)
                        return false;
                    Assert.AreEqual(p1.Offsets.Length, p2.Offsets.Length);
                    CollectionAssert.AreEqual(p1.Offsets, p2.Offsets);
                    return true;
                });
            }
        }

        [Test]
        public void TestDeserializeFetchResponse()
        {
            var response = new CommonResponse<FetchPartitionResponse>
            {
                TopicsResponse = new []
                {
                    new TopicData<FetchPartitionResponse>
                    {
                        TopicName = "Buffy_contre_les_zombies",
                        PartitionsData = new[]
                        {
                            new FetchPartitionResponse
                            {
                                ErrorCode = ErrorCode.NoError,
                                HighWatermarkOffset = 714,
                                Partition = 999999,
                                Messages = new List<ResponseMessage>
                                {
                                    new ResponseMessage
                                    {
                                        Offset = 44,
                                        Message = new Message
                                        {
                                            Key = Key,
                                            Value = Value
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            };

            using (var serialized = ReusableMemoryStream.Reserve())
            {
                Basics.WriteArray(serialized, response.TopicsResponse);

                TestCommonResponse(serialized, response, (p1, p2) =>
                {
                    Assert.AreEqual(p1.Partition, p2.Partition);
                    Assert.AreEqual(p1.ErrorCode, p2.ErrorCode);
                    Assert.AreEqual(p1.HighWatermarkOffset, p2.HighWatermarkOffset);
                    Assert.AreEqual(p1.Messages.Count, p2.Messages.Count);
                    foreach (var zipped in p1.Messages.Zip(p2.Messages, Tuple.Create))
                    {
                        Assert.AreEqual(zipped.Item1.Offset, zipped.Item2.Offset);
                        CollectionAssert.AreEqual(zipped.Item1.Message.Key, zipped.Item2.Message.Key);
                        CollectionAssert.AreEqual(zipped.Item1.Message.Value, zipped.Item2.Message.Value);
                    }

                    return true;
                });
            }
        }

        /* The following test are ordered to be run first because if we break one
         * it will most likely break all other tests. Thus when serialization tests
         * break we can quick check if it's due to basic stuff being broken or not
         * by simply checking the first tests to fail.
         */

        [Test]
        public void Test003_SerializeString()
        {
            // Non null string
            using (var serialized = ReusableMemoryStream.Reserve())
            {
                Basics.SerializeString(serialized, TheValue);
                Assert.AreEqual(2 + Value.Length, serialized.Length);
                serialized.Position = 0;
                Assert.AreEqual(TheValue.Length, BigEndianConverter.ReadInt16(serialized));
                CompareArrays(Value, serialized.GetBuffer(), 2);
            }

            // Null string
            using (var serialized = ReusableMemoryStream.Reserve())
            {
                Basics.SerializeString(serialized, null);
                Assert.AreEqual(2, serialized.Length);
                serialized.Position = 0;
                Assert.AreEqual(-1, BigEndianConverter.ReadInt16(serialized));
            }
        }

        [Test]
        public void Test004_DeserializeString()
        {
            using (var serialized = ReusableMemoryStream.Reserve())
            {
                // Non null string
                BigEndianConverter.Write(serialized, (short) Value.Length);
                serialized.Write(Value, 0, Value.Length);
                serialized.Position = 0;

                Assert.AreEqual(TheValue, Basics.DeserializeString(serialized));

                // Null string
                serialized.SetLength(0);
                serialized.WriteByte(0xFF);
                serialized.WriteByte(0xFF);
                serialized.Position = 0;

                Assert.IsNull(Basics.DeserializeString(serialized));
            }
        }

        [Test]
        public void Test001_SerializeArray()
        {
            var array = new[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 109};
            using (var serialized = ReusableMemoryStream.Reserve())
            {
                Basics.WriteArray(serialized, array, BigEndianConverter.Write);
                serialized.Position = 0;

                Assert.AreEqual(array.Length * sizeof(int) + 4, serialized.Length);
                Assert.AreEqual(array.Length, BigEndianConverter.ReadInt32(serialized));
                for (int i = 0; i < array.Length; ++i)
                {
                    Assert.AreEqual(array[i], BigEndianConverter.ReadInt32(serialized));
                }
            }
        }

        [Test]
        public void Test002_DeserializeArray()
        {
            var array = new[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 109 };
            using (var serialized = ReusableMemoryStream.Reserve())
            {
                Basics.WriteArray(serialized, array, BigEndianConverter.Write);
                serialized.Position = 0;

                var deserialized = Basics.DeserializeArray(serialized, BigEndianConverter.ReadInt32);
                CollectionAssert.AreEqual(array, deserialized);
            }
        }

        [Test]
        public void Test000_BigEndianConverter()
        {
            const byte b0 = 123;
            const byte b1 = 98;
            const byte b2 = 0;
            const byte b3 = 188;
            const byte b4 = 23;
            const byte b5 = 89;
            const byte b6 = 101;
            const byte b7 = 7;

            using (var s = ReusableMemoryStream.Reserve())
            {
                var n1 = (short) ((b0 << 8) | b1);
                BigEndianConverter.Write(s, n1);
                Assert.AreEqual(b0, s[0]);
                Assert.AreEqual(b1, s[1]);
                s.Position = 0;
                Assert.AreEqual(n1, BigEndianConverter.ReadInt16(s));

                var n2 = (int) ((b0 << 24) | (b1 << 16) | (b2 << 8) | b3);
                s.Position = 0;
                BigEndianConverter.Write(s, n2);
                Assert.AreEqual(b0, s[0]);
                Assert.AreEqual(b1, s[1]);
                Assert.AreEqual(b2, s[2]);
                Assert.AreEqual(b3, s[3]);
                s.Position = 0;
                Assert.AreEqual(n2, BigEndianConverter.ReadInt32(s));


                var n3 = (((long) b0 << 56) |
                          ((long) b1 << 48 | ((long) b2 << 40) | ((long) b3 << 32) | ((long) b4 << 24) |
                           ((long) b5 << 16) | ((long) b6 << 8) | b7));
                s.Position = 0;
                BigEndianConverter.Write(s, n3);
                Assert.AreEqual(b0, s[0]);
                Assert.AreEqual(b1, s[1]);
                Assert.AreEqual(b2, s[2]);
                Assert.AreEqual(b3, s[3]);
                Assert.AreEqual(b4, s[4]);
                Assert.AreEqual(b5, s[5]);
                Assert.AreEqual(b6, s[6]);
                Assert.AreEqual(b7, s[7]);
                s.Position = 0;
                Assert.AreEqual(n3, BigEndianConverter.ReadInt64(s));
            }
        }
    }
}