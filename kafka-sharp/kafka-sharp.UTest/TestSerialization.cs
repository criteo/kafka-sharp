using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using Kafka.Common;
using Kafka.Protocol;
using Kafka.Public;
using Kafka.Routing;
using NUnit.Framework;

namespace tests_kafka_sharp
{
    [TestFixture]
    class TestSerialization
    {
        private static readonly string TheKey = "The Key Opens the Door";
        private static readonly string TheValue = "The quick brown fox jumps over the zealy göd.";
        private static readonly string TheClientId = "ClientId";
        private static readonly byte[] Value = Encoding.UTF8.GetBytes(TheValue);
        private static readonly byte[] Key = Encoding.UTF8.GetBytes(TheKey);
        private static readonly byte[] ClientId = Encoding.UTF8.GetBytes(TheClientId);

        private static readonly Pool<ReusableMemoryStream> Pool =
                new Pool<ReusableMemoryStream>(() => new ReusableMemoryStream(Pool), (m, b) => { m.SetLength(0); });

        private static int GetExpectedMessageSize(int keyLength, int valueLength, MessageVersion messageVersion)
        {
            var timestampLength = messageVersion == MessageVersion.V1 ? 8 : 0; // Timestamp in v1 only (long => 8 bytes)
            return 4    // CRC
                + 1     // Magic byte (version)
                + 1     // Attributes
                + timestampLength
                + 4     // Key size
                + keyLength
                + 4     // Data size
                + valueLength;
        }

        private static readonly int NullKeyMessageSize = 4 + 1 + 1 + 4 + 4 + Value.Length;

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
        public void Test007_KafkaLz4()
        {
            var data = new string('e', 65536 * 2 + 7); // this will produce 3 blocks with uncompressed last block due to very small size
            var value = Encoding.UTF8.GetBytes(data);
            var compressed = new ReusableMemoryStream(null);
            compressed.WriteByte(7);
            KafkaLz4.Compress(compressed, value, value.Length);
            var uncompressed = new ReusableMemoryStream(null);
            KafkaLz4.Uncompress(uncompressed, compressed.GetBuffer(), 1);
            var dec = Encoding.UTF8.GetString(uncompressed.GetBuffer(), 0, (int)uncompressed.Length);
            Assert.AreEqual(data, dec);
        }


        [Test]
        public void TestSerializeOneMessage_V0()
        {
            var message = new Message { Key = Key, Value = Value, TimeStamp = 285};
            TestSerializeOneMessageCommon(message);
        }

        [Test]
        public void TestSerializeOneMessageWithPreserializedKeyValue()
        {
            var message = new Message { Key = Key, Value = Value };
            message.SerializeKeyValue(new ReusableMemoryStream(null), new Tuple<ISerializer, ISerializer>(null, null));
            Assert.IsNull(message.Value);
            Assert.IsNotNull(message.SerializedKeyValue);
            TestSerializeOneMessageCommon(message);
            message.SerializedKeyValue.Dispose();
        }

        class SimpleSerializable : IMemorySerializable
        {
            public byte[] Data;

            public SimpleSerializable(byte[] data)
            {
                Data = data;
            }

            public void Serialize(MemoryStream toStream)
            {
                toStream.Write(Data, 0, Data.Length);
            }
        }

        class SimpleSizedSerializable : SimpleSerializable, ISizedMemorySerializable
        {
            public SimpleSizedSerializable(byte[] data)
                : base(data)
            {
            }

            public long SerializedSize()
            {
                return Data.Length;
            }
        }

        [Test]
        public void TestSerializeOneMessageIMemorySerializable()
        {
            var message = new Message { Key = new SimpleSerializable(Key), Value = new SimpleSerializable(Value) };
            TestSerializeOneMessageCommon(message);
        }

        [Test]
        public void TestSerializeOneMessageIMemorySerializableWithPreserializedKeyValue()
        {
            var message = new Message { Key = new SimpleSerializable(Key), Value = new SimpleSerializable(Value) };
            message.SerializeKeyValue(new ReusableMemoryStream(null), new Tuple<ISerializer, ISerializer>(null, null));
            Assert.IsNull(message.Value);
            Assert.IsNotNull(message.SerializedKeyValue);
            TestSerializeOneMessageCommon(message);
            message.SerializedKeyValue.Dispose();
        }

        [Test]
        public void TestSerializeOneMessage_V1()
        {
            var message = new Message { Key = Key, Value = Value, TimeStamp = 42 };
            using (var serialized = new ReusableMemoryStream(null))
            {
                message.Serialize(serialized, CompressionCodec.None, new Tuple<ISerializer, ISerializer>(null, null), MessageVersion.V1);
                Assert.AreEqual(GetExpectedMessageSize(Key.Length, Value.Length, MessageVersion.V1), serialized.Length);
                Assert.AreEqual(1, serialized.GetBuffer()[4]); // magic byte is 1
                Assert.AreEqual(0, serialized.GetBuffer()[5]); // attributes is 0
                serialized.Position = 6;
                Assert.AreEqual(42, BigEndianConverter.ReadInt64(serialized));
                Assert.AreEqual(Key.Length, BigEndianConverter.ReadInt32(serialized));
                CompareBuffers(Key, serialized);
                Assert.AreEqual(Value.Length, BigEndianConverter.ReadInt32(serialized));
                CompareBuffers(Value, serialized);
            }
        }

        private void TestSerializeOneMessageCommon(Message message)
        {
            using (var serialized = new ReusableMemoryStream(null))
            {
                message.Serialize(serialized, CompressionCodec.None, new Tuple<ISerializer, ISerializer>(null, null), MessageVersion.V0);
                Assert.AreEqual(GetExpectedMessageSize(Key.Length, Value.Length, MessageVersion.V0), serialized.Length);
                Assert.AreEqual(0, serialized.GetBuffer()[4]); // magic byte is 0
                Assert.AreEqual(0, serialized.GetBuffer()[5]); // attributes is 0
                serialized.Position = 6;
                Assert.AreEqual(Key.Length, BigEndianConverter.ReadInt32(serialized));
                CompareBuffers(Key, serialized);
                Assert.AreEqual(Value.Length, BigEndianConverter.ReadInt32(serialized));
                CompareBuffers(Value, serialized);
            }

            using (var serialized = new ReusableMemoryStream(null))
            {
                var msg = new Message { Value = Value };
                msg.Serialize(serialized, CompressionCodec.None, new Tuple<ISerializer, ISerializer>(null, null), MessageVersion.V0);
                Assert.AreEqual(NullKeyMessageSize, serialized.Length);
                Assert.AreEqual(0, serialized.GetBuffer()[4]); // magic byte is 0
                Assert.AreEqual(0, serialized.GetBuffer()[5]); // attributes is 0
                serialized.Position = 6;
                Assert.AreEqual(-1, BigEndianConverter.ReadInt32(serialized));
                Assert.AreEqual(Value.Length, BigEndianConverter.ReadInt32(serialized));
                CompareBuffers(Value, serialized);
            }
        }

        [Test]
        public void TestSerializeOneMessageCodec()
        {
            // Just check attributes, we don't put correct data
            using (var serialized = new ReusableMemoryStream(null))
            {
                var message = new Message { Value = Value };
                message.Serialize(serialized, CompressionCodec.Snappy, new Tuple<ISerializer, ISerializer>(null, null), MessageVersion.V0);
                Assert.AreEqual(0, serialized.GetBuffer()[4]); // magic byte is 0
                Assert.AreEqual(2, serialized.GetBuffer()[5]); // attributes is 2
            }

            using (var serialized = new ReusableMemoryStream(null))
            {
                var message = new Message { Value = Value };
                message.Serialize(serialized, CompressionCodec.Gzip, new Tuple<ISerializer, ISerializer>(null, null), MessageVersion.V0);
                Assert.AreEqual(0, serialized.GetBuffer()[4]); // magic byte is 0
                Assert.AreEqual(1, serialized.GetBuffer()[5]); // attributes is 1
            }
        }

        [TestCase(MessageVersion.V0)]
        [TestCase(MessageVersion.V1)]
        public void TestSerializeOneEmptyByteMessage(MessageVersion messageversion)
        {
            var message = new Message { Key = new byte[0], Value = new byte[0] };
            TestSerializeOneEmptyMessageCommon(message, messageversion);
        }

        [TestCase(MessageVersion.V0)]
        [TestCase(MessageVersion.V1)]
        public void TestSerializeOneEmptySerializableMessage(MessageVersion messageversion)
        {
            var message = new Message { Key = new SimpleSerializable(new byte[0]), Value = new SimpleSerializable(new byte[0]) };
            TestSerializeOneEmptyMessageCommon(message, messageversion);
        }

        [TestCase(MessageVersion.V0)]
        [TestCase(MessageVersion.V1)]
        public void TestSerializeOneEmptyMessageWithPreserializedKeyValue(MessageVersion messageVersion)
        {
            var message = new Message { Key = new byte[0], Value = new byte[0] };
            message.SerializeKeyValue(new ReusableMemoryStream(null), new Tuple<ISerializer, ISerializer>(null, null));
            Assert.IsNull(message.Value);
            Assert.IsNotNull(message.SerializedKeyValue);
            TestSerializeOneEmptyMessageCommon(message, messageVersion);
            message.SerializedKeyValue.Dispose();
        }

        private static void TestSerializeOneEmptyMessageCommon(Message message, MessageVersion messageVersion)
        {
            using (var serialized = new ReusableMemoryStream(null))
            {
                message.Serialize(serialized, CompressionCodec.None, new Tuple<ISerializer, ISerializer>(null, null), messageVersion);
                Assert.AreEqual(GetExpectedMessageSize(0, 0, messageVersion), serialized.Length);
                serialized.Position = 6;
                if (messageVersion == MessageVersion.V1)
                {
                    Assert.AreEqual(message.TimeStamp, BigEndianConverter.ReadInt64(serialized));
                }
                Assert.AreEqual(0, BigEndianConverter.ReadInt32(serialized));
                Assert.AreEqual(0, BigEndianConverter.ReadInt32(serialized));
            }
        }

        [Test]
        public void TestSerializeMessageWithInvalidPreserializedKeyValue()
        {
            var logger = new TestLogger();
            var message = new Message { Key = Key, Value = Value };
            message.SerializeKeyValue(new ReusableMemoryStream(null, logger), new Tuple<ISerializer, ISerializer>(null, null));

            // Simulate a buffer with no data
            message.SerializedKeyValue = new ReusableMemoryStream(null, logger);
            Assert.IsNotNull(message.SerializedKeyValue);
            Assert.AreEqual(0, message.SerializedKeyValue.Length);

            // Verify that serialization is able to process the message anyway
            using (var serialized = new ReusableMemoryStream(null, logger))
            {
                message.Serialize(serialized, CompressionCodec.None, new Tuple<ISerializer, ISerializer>(null, null), MessageVersion.V0);
                Assert.AreEqual(GetExpectedMessageSize(0, 0, MessageVersion.V0), serialized.Length);
                Assert.AreEqual(0, serialized.GetBuffer()[4]); // magic byte is 0
                Assert.AreEqual(0, serialized.GetBuffer()[5]); // attributes is 0
                serialized.Position = 6;
                // SerializedKeyValue invalid => Key & value should be interpreted as null
                Assert.AreEqual(-1, BigEndianConverter.ReadInt32(serialized));
                Assert.AreEqual(-1, BigEndianConverter.ReadInt32(serialized));
            }

            Assert.AreEqual(1, logger.ErrorLog.Count());

            message.SerializedKeyValue.Dispose();
        }

        [Test]
        public void TestSerializeMessageSet()
        {
            using (var serialized = new ReusableMemoryStream(null))
            {
                var set = new PartitionData
                {
                    Partition = 42,
                    CompressionCodec = CompressionCodec.None,
                    Messages =
                        new[] { new Message { Key = Key, Value = Value }, new Message { Key = Key, Value = Value } }
                };
                set.Serialize(serialized, SerializationConfig.ByteArraySerializers, Basics.ApiVersion.Ignored);
                serialized.Position = 0;

                Assert.AreEqual(42, BigEndianConverter.ReadInt32(serialized)); // Partition
                Assert.AreEqual(serialized.Length - 4 - 4, BigEndianConverter.ReadInt32(serialized)); // MessageSet size
                Assert.AreEqual(0, BigEndianConverter.ReadInt64(serialized)); // First message offset
                int firstMsgSize = BigEndianConverter.ReadInt32(serialized);
                serialized.Position += firstMsgSize;
                Assert.AreEqual(1, BigEndianConverter.ReadInt64(serialized)); // Second message offset
                int secondMsgSize = BigEndianConverter.ReadInt32(serialized);
                serialized.Position += secondMsgSize;
                Assert.AreEqual(serialized.Length, serialized.Position);
            }
        }

        [Test]
        [Category("failsOnMono")] //because Snappy uses native code
        [TestCase(CompressionCodec.Gzip, 1)]
#if !NET_CORE
        [TestCase(CompressionCodec.Snappy, 2)]
#endif
        [TestCase(CompressionCodec.Lz4, 3)]
        public void TestSerializeMessageSetCompressed(CompressionCodec codec, byte attr)
        {
            using (var serialized = Pool.Reserve())
            {
                var set = new PartitionData
                {
                    Partition = 42,
                    CompressionCodec = codec,
                    Messages =
                        new[] { new Message { Key = Key, Value = Value }, new Message { Key = Key, Value = Value } }
                };
                set.Serialize(serialized, SerializationConfig.ByteArraySerializers, Basics.ApiVersion.Ignored);
                serialized.Position = 0;

                Assert.AreEqual(42, BigEndianConverter.ReadInt32(serialized)); // Partition
                Assert.AreEqual(serialized.Length - 4 - 4, BigEndianConverter.ReadInt32(serialized)); // MessageSet size
                Assert.AreEqual(0, BigEndianConverter.ReadInt64(serialized)); // Wrapper message offset
                int wrapperMsgSize = BigEndianConverter.ReadInt32(serialized);
                serialized.Position += wrapperMsgSize;
                Assert.AreEqual(serialized.Length, serialized.Position);
                int msgPos = 4 + 4 + 8 + 4; // partition, msgset size, offset, msg size
                int valuePos = msgPos + 4 + 1 + 1 + 4 + 4; // + crc, magic, attr, key size, value size
                serialized.Position = msgPos;
                serialized.Position += 5;
                Assert.AreEqual(attr, serialized.ReadByte()); // attributes of the wrapper msg
                Assert.AreEqual(-1, BigEndianConverter.ReadInt32(serialized)); // No key => size = -1
                Assert.AreEqual(serialized.Length - valuePos, BigEndianConverter.ReadInt32(serialized)); // size of compressed payload
            }
        }

        [Test]
        [Category("failsOnMono")] //because Snappy uses native code
        [TestCase(CompressionCodec.None)]
        [TestCase(CompressionCodec.Gzip)]
#if !NET_CORE
        [TestCase(CompressionCodec.Snappy)]
#endif
        public void TestDeserializeMessageSet_V0(CompressionCodec codec)
        {
            using (var serialized = Pool.Reserve())
            {
                var set = new PartitionData
                {
                    Partition = 42,
                    CompressionCodec = codec,
                    Messages =
                        new[]
                        {
                            new Message { Key = Key, Value = Value },
                            new Message { Key = Key, Value = Value, TimeStamp = 42 }
                        }
                };
                set.Serialize(serialized, SerializationConfig.ByteArraySerializers, Basics.ApiVersion.Ignored);
                serialized.Position = 4;

                var deserialized = FetchPartitionResponse.DeserializeMessageSet(serialized,
                    SerializationConfig.ByteArrayDeserializers);
                Assert.AreEqual(2, deserialized.Count);
                int o = 0;
                foreach (var msg in deserialized)
                {
                    Assert.AreEqual(o, msg.Offset);
                    CollectionAssert.AreEqual(Key, msg.Message.Key as byte[]);
                    CollectionAssert.AreEqual(Value, msg.Message.Value as byte[]);
                    Assert.AreEqual(0, msg.Message.TimeStamp);
                    ++o;
                }
            }
        }

        [Test]
        public void TestDeserializeMessageSet_v1uncompressed()
        {
            using (var serialized = Pool.Reserve())
            {
                var codec = CompressionCodec.None;
                var set = new PartitionData
                {
                    Partition = 42,
                    CompressionCodec = codec,
                    Messages =
                        new[]
                        {
                            new Message { Key = Key, Value = Value },
                            new Message { Key = Key, Value = Value, TimeStamp = 42 },
                            new Message { Key = Key, Value = Value, TimeStamp = 42134 },
                        }
                };
                set.Serialize(serialized, SerializationConfig.ByteArraySerializers, Basics.ApiVersion.V2);
                serialized.Position = 4;

                var deserialized = FetchPartitionResponse.DeserializeMessageSet(serialized,
                    SerializationConfig.ByteArrayDeserializers);
                Assert.AreEqual(3, deserialized.Count);
                int o = 0;
                foreach (var msg in deserialized)
                {
                    Assert.AreEqual(o, msg.Offset);
                    CollectionAssert.AreEqual(Key, msg.Message.Key as byte[]);
                    CollectionAssert.AreEqual(Value, msg.Message.Value as byte[]);
                    Assert.AreEqual(set.Messages.ElementAt(o).TimeStamp, msg.Message.TimeStamp);
                    ++o;
                }
            }
        }

        [Test]
        [Category("failsOnMono")] //because Snappy uses native code
        [TestCase(CompressionCodec.Gzip)]
#if !NET_CORE
        [TestCase(CompressionCodec.Snappy)]
#endif
        [TestCase(CompressionCodec.Lz4)]
        public void TestDeserializeMessageSet_v1compressed(CompressionCodec codec)
        {
            using (var serialized = Pool.Reserve())
            {
                var set = new PartitionData
                {
                    Partition = 42,
                    CompressionCodec = codec,
                    Messages =
                        new[]
                        {
                            new Message { Key = Key, Value = Value },
                            new Message { Key = Key, Value = Value, TimeStamp = 42 },
                            new Message { Key = Key, Value = Value, TimeStamp = 42134 },
                        }
                };
                set.Serialize(serialized, SerializationConfig.ByteArraySerializers, Basics.ApiVersion.V2);
                // Override offset in wrapper msg
                serialized.Position = 4 + 4;
                const long LAST_OFFSET = 87;
                BigEndianConverter.Write(serialized, LAST_OFFSET);

                serialized.Position = 4;

                var deserialized = FetchPartitionResponse.DeserializeMessageSet(serialized,
                    SerializationConfig.ByteArrayDeserializers);
                Assert.AreEqual(3, deserialized.Count);
                int o = 0;
                foreach (var msg in deserialized)
                {
                    Assert.AreEqual(LAST_OFFSET + 1 - deserialized.Count + o, msg.Offset);
                    CollectionAssert.AreEqual(Key, msg.Message.Key as byte[]);
                    CollectionAssert.AreEqual(Value, msg.Message.Value as byte[]);
                    Assert.AreEqual(set.Messages.ElementAt(o).TimeStamp, msg.Message.TimeStamp);
                    ++o;
                }
            }
        }

        private static void CheckHeader(Basics.ApiKey apiKey, short apiVersion, int correlationId, string clientId,
            ReusableMemoryStream stream)
        {
            Assert.AreEqual(stream.Length - 4, BigEndianConverter.ReadInt32(stream)); // Size
            Assert.AreEqual((short) apiKey, BigEndianConverter.ReadInt16(stream));
            Assert.AreEqual(apiVersion, BigEndianConverter.ReadInt16(stream));
            Assert.AreEqual(correlationId, BigEndianConverter.ReadInt32(stream));
            Assert.AreEqual(clientId, Basics.DeserializeString(stream));
        }

        [Test]
        public void TestSerializeMetadataRequest()
        {
            var meta = new TopicRequest { Topics = new[] { "poulpe", "banana" } };
            using (var serialized = meta.Serialize(new ReusableMemoryStream(null), 61, ClientId, null, Basics.ApiVersion.Ignored))
            {
                CheckHeader(Basics.ApiKey.MetadataRequest, 0, 61, TheClientId, serialized);
                Assert.AreEqual(2, BigEndianConverter.ReadInt32(serialized));
                Assert.AreEqual("poulpe", Basics.DeserializeString(serialized));
                Assert.AreEqual("banana", Basics.DeserializeString(serialized));
            }

            meta = new TopicRequest { Topics = null };
            using (var serialized = meta.Serialize(new ReusableMemoryStream(null), 61, ClientId, null, Basics.ApiVersion.Ignored))
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
                        new BrokerMeta { Host = "Host", Id = 100, Port = 18909 },
                        new BrokerMeta { Host = "tsoH", Id = 28, Port = 1 }
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
                                        Replicas = new[] { 100 },
                                        Isr = new int[0]
                                    }
                                }
                        }
                    }
            };
            using (var serialized = new ReusableMemoryStream(null))
            {
                meta.Serialize(serialized, null);
                Assert.AreEqual(74, serialized.Length); // TODO: better check that serialization is correct?

                serialized.Position = 0;
                var metaDeser = MetadataResponse.Deserialize(serialized, null);
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

        private void AssertAreEqual(FetchPartitionData expected, FetchPartitionData result)
        {
            Assert.AreEqual(expected.FetchOffset, result.FetchOffset);
            Assert.AreEqual(expected.MaxBytes, result.MaxBytes);
            Assert.AreEqual(expected.Partition, result.Partition);
        }

        private void AssertAreEqual(TopicData<FetchPartitionData> expected, TopicData<FetchPartitionData> result)
        {
            Assert.AreEqual(expected.TopicName, result.TopicName);
            Assert.AreEqual(expected.PartitionsData.Count(), result.PartitionsData.Count());
            for (int i = 0; i < expected.PartitionsData.Count(); i++)
            {
                AssertAreEqual(expected.PartitionsData.ElementAt(i), result.PartitionsData.ElementAt(i));
            }
        }

        private readonly TopicData<FetchPartitionData> _topicDataOfFetch1 = new TopicData<FetchPartitionData>
        {
            TopicName = "topic1",
            PartitionsData = new[] { new FetchPartitionData { FetchOffset = 1, MaxBytes = 333, Partition = 42 } }
        };

        private readonly TopicData<FetchPartitionData> _topicDataOfFetch2 = new TopicData<FetchPartitionData>
        {
            TopicName = "topic2",
            PartitionsData = new[]
            {
                new FetchPartitionData { FetchOffset = 1, MaxBytes = 333, Partition = 43 },
                new FetchPartitionData { FetchOffset = 2, MaxBytes = 89, Partition = 44 }
            }
        };

        [Test]
        [TestCase(Basics.ApiVersion.V0)]
        [TestCase(Basics.ApiVersion.V2)]
        public void TestSerializeFetchRequest(Basics.ApiVersion version)
        {
            var fetch = new FetchRequest
            {
                MaxWaitTime = 11111,
                MinBytes = 222222,
                TopicsData = new[] { _topicDataOfFetch1, _topicDataOfFetch2 }
            };
            using (var serialized = fetch.Serialize(new ReusableMemoryStream(null), 1234, ClientId, null, version))
            {
                CheckHeader(Basics.ApiKey.FetchRequest, (short)version, 1234, TheClientId, serialized);
                Assert.AreEqual(-1, BigEndianConverter.ReadInt32(serialized)); // ReplicaId
                Assert.AreEqual(fetch.MaxWaitTime, BigEndianConverter.ReadInt32(serialized));
                Assert.AreEqual(fetch.MinBytes, BigEndianConverter.ReadInt32(serialized));
                Assert.AreEqual(2, BigEndianConverter.ReadInt32(serialized)); // 2 elements

                var topicDataResult = new TopicData<FetchPartitionData>();
                topicDataResult.Deserialize(serialized, null, Basics.ApiVersion.Ignored);
                AssertAreEqual(_topicDataOfFetch1, topicDataResult);

                topicDataResult.Deserialize(serialized, null, Basics.ApiVersion.Ignored);
                AssertAreEqual(_topicDataOfFetch2, topicDataResult);
            }
        }

        [Test]
        public void TestSerializeFetchRequestV3()
        {
            var fetch = new FetchRequest
            {
                MaxWaitTime = 11111,
                MinBytes = 222222,
                MaxBytes = 333333,
                TopicsData = new[] { _topicDataOfFetch1, _topicDataOfFetch2 }
            };
            using (var serialized = fetch.Serialize(new ReusableMemoryStream(null), 1234, ClientId, null, Basics.ApiVersion.V3))
            {
                CheckHeader(Basics.ApiKey.FetchRequest, (short)Basics.ApiVersion.V3, 1234, TheClientId, serialized);
                Assert.AreEqual(-1, BigEndianConverter.ReadInt32(serialized)); // ReplicaId
                Assert.AreEqual(fetch.MaxWaitTime, BigEndianConverter.ReadInt32(serialized));
                Assert.AreEqual(fetch.MinBytes, BigEndianConverter.ReadInt32(serialized));
                Assert.AreEqual(fetch.MaxBytes, BigEndianConverter.ReadInt32(serialized));
                Assert.AreEqual(2, BigEndianConverter.ReadInt32(serialized)); // 2 elements

                var topicDataResult = new TopicData<FetchPartitionData>();
                topicDataResult.Deserialize(serialized, null, Basics.ApiVersion.V3);
                AssertAreEqual(_topicDataOfFetch1, topicDataResult);

                topicDataResult.Deserialize(serialized, null, Basics.ApiVersion.V3);
                AssertAreEqual(_topicDataOfFetch2, topicDataResult);
            }
        }

        [Test]
        public void TestSerializeFetchRequestV4()
        {
            var fetch = new FetchRequest
            {
                MaxWaitTime = 11111,
                MinBytes = 222222,
                MaxBytes = 333333,
                IsolationLevel = Basics.IsolationLevel.ReadUncommited,
                TopicsData = new[] { _topicDataOfFetch1, _topicDataOfFetch2 }
            };
            using (var serialized = fetch.Serialize(new ReusableMemoryStream(null), 1234, ClientId, null, Basics.ApiVersion.V4))
            {
                CheckHeader(Basics.ApiKey.FetchRequest, (short)Basics.ApiVersion.V4, 1234, TheClientId, serialized);
                Assert.AreEqual(-1, BigEndianConverter.ReadInt32(serialized)); // ReplicaId
                Assert.AreEqual(fetch.MaxWaitTime, BigEndianConverter.ReadInt32(serialized));
                Assert.AreEqual(fetch.MinBytes, BigEndianConverter.ReadInt32(serialized));
                Assert.AreEqual(fetch.MaxBytes, BigEndianConverter.ReadInt32(serialized));
                Assert.AreEqual(0, serialized.ReadByte()); // IsolationLevel
                Assert.AreEqual(2, BigEndianConverter.ReadInt32(serialized)); // 2 elements

                var topicDataResult = new TopicData<FetchPartitionData>();
                topicDataResult.Deserialize(serialized, null, Basics.ApiVersion.V4);
                AssertAreEqual(_topicDataOfFetch1, topicDataResult);

                topicDataResult.Deserialize(serialized, null, Basics.ApiVersion.V4);
                AssertAreEqual(_topicDataOfFetch2, topicDataResult);
            }
        }

        [Test]
        public void TestSerializeFetchRequestV5()
        {
            var fetch = new FetchRequest
            {
                MaxWaitTime = 11111,
                MinBytes = 222222,
                MaxBytes = 333333,
                IsolationLevel = Basics.IsolationLevel.ReadUncommited,
                TopicsData = new[] { _topicDataOfFetch1, _topicDataOfFetch2 }
            };
            using (var serialized = fetch.Serialize(new ReusableMemoryStream(null), 1234, ClientId, null, Basics.ApiVersion.V5))
            {
                CheckHeader(Basics.ApiKey.FetchRequest, (short)Basics.ApiVersion.V5, 1234, TheClientId, serialized);
                Assert.AreEqual(-1, BigEndianConverter.ReadInt32(serialized)); // ReplicaId
                Assert.AreEqual(fetch.MaxWaitTime, BigEndianConverter.ReadInt32(serialized));
                Assert.AreEqual(fetch.MinBytes, BigEndianConverter.ReadInt32(serialized));
                Assert.AreEqual(fetch.MaxBytes, BigEndianConverter.ReadInt32(serialized));
                Assert.AreEqual(0, serialized.ReadByte()); // IsolationLevel
                Assert.AreEqual(2, BigEndianConverter.ReadInt32(serialized)); // 2 elements

                var topicDataResult = new TopicData<FetchPartitionData>();
                topicDataResult.Deserialize(serialized, null, Basics.ApiVersion.V5);
                AssertAreEqual(_topicDataOfFetch1, topicDataResult);
                Assert.That(topicDataResult.PartitionsData.All(pd => pd.LogStartOffset == 0));

                topicDataResult.Deserialize(serialized, null, Basics.ApiVersion.V5);
                AssertAreEqual(_topicDataOfFetch2, topicDataResult);
                Assert.That(topicDataResult.PartitionsData.All(pd => pd.LogStartOffset == 0));
            }
        }

        private static TopicData<PartitionData> CreateTopicDataPartitionData(CompressionCodec codec, ICollection<KafkaRecordHeader> headers = null)
        {
            return new TopicData<PartitionData>
            {
                TopicName = "barbu",
                PartitionsData = new[]
                {
                    new PartitionData
                    {
                        Partition = 22,
                        CompressionCodec = codec,
                        Messages = new[]
                        {
                            new Message { Value = TheValue, Headers = headers },
                            new Message { Value = TheValue, Headers = headers }
                        },
                    }
                }
            };
        }

        private readonly TopicData<PartitionData> _defaultTopicDataPartitionData = CreateTopicDataPartitionData(CompressionCodec.None);


        [Test]
        public void TestSerializeProduceRequest_V0()
        {
            var produce = new ProduceRequest
            {
                Timeout = 1223,
                RequiredAcks = 1,
                TopicsData = new[] { _defaultTopicDataPartitionData }
            };
            var config = new SerializationConfig();
            config.SetSerializersForTopic("barbu", new StringSerializer(), new StringSerializer());
            config.SetDeserializersForTopic("barbu", new StringDeserializer(), new StringDeserializer());
            using (var serialized = produce.Serialize(new ReusableMemoryStream(null), 321, ClientId, config, Basics.ApiVersion.V0))
            {
                CheckHeader(Basics.ApiKey.ProduceRequest, 0, 321, TheClientId, serialized);
                Assert.AreEqual(produce.RequiredAcks, BigEndianConverter.ReadInt16(serialized));
                Assert.AreEqual(produce.Timeout, BigEndianConverter.ReadInt32(serialized));
                Assert.AreEqual(1, BigEndianConverter.ReadInt32(serialized)); // 1 topic data
                Assert.AreEqual("barbu", Basics.DeserializeString(serialized));
                Assert.AreEqual(1, BigEndianConverter.ReadInt32(serialized)); // 1 partition data
                Assert.AreEqual(22, BigEndianConverter.ReadInt32(serialized));
                var msgs = FetchPartitionResponse.DeserializeMessageSet(serialized,
                    config.GetDeserializersForTopic("barbu"));
                Assert.AreEqual(2, msgs.Count);
                Assert.AreEqual(TheValue, msgs[0].Message.Value as string);
                Assert.AreEqual(0, msgs[0].Offset);
                Assert.AreEqual(TheValue, msgs[1].Message.Value as string);
                Assert.AreEqual(1, msgs[1].Offset);
            }
        }

        [Test]
        public void TestSerializeProduceRequest_V2()
        {
            var produce = new ProduceRequest
            {
                Timeout = 1223,
                RequiredAcks = 1,
                TopicsData = new[] { _defaultTopicDataPartitionData }
            };
            var config = new SerializationConfig();
            config.SetSerializersForTopic("barbu", new StringSerializer(), new StringSerializer());
            config.SetDeserializersForTopic("barbu", new StringDeserializer(), new StringDeserializer());
            using (var serialized = produce.Serialize(new ReusableMemoryStream(null), 321, ClientId, config, Basics.ApiVersion.V2))
            {
                CheckHeader(Basics.ApiKey.ProduceRequest, 2, 321, TheClientId, serialized);
                Assert.AreEqual(produce.RequiredAcks, BigEndianConverter.ReadInt16(serialized));
                Assert.AreEqual(produce.Timeout, BigEndianConverter.ReadInt32(serialized));
                Assert.AreEqual(1, BigEndianConverter.ReadInt32(serialized)); // 1 topic data
                Assert.AreEqual("barbu", Basics.DeserializeString(serialized));
                Assert.AreEqual(1, BigEndianConverter.ReadInt32(serialized)); // 1 partition data
                Assert.AreEqual(22, BigEndianConverter.ReadInt32(serialized));
                var msgs = FetchPartitionResponse.DeserializeMessageSet(serialized,
                    config.GetDeserializersForTopic("barbu"));
                Assert.AreEqual(2, msgs.Count);
                Assert.AreEqual(TheValue, msgs[0].Message.Value as string);
                Assert.AreEqual(0, msgs[0].Offset);
                Assert.AreEqual(TheValue, msgs[1].Message.Value as string);
                Assert.AreEqual(1, msgs[1].Offset);
            }
        }

        [TestCase("Transactional Id foo", CompressionCodec.None)]
        [TestCase(null, CompressionCodec.None)]
        [TestCase(null, CompressionCodec.Gzip)]
        [Test]
        public void TestSerializeProduceRequest_V3(string transactionalId, CompressionCodec codec)
        {
            var produce = new ProduceRequest
            {
                Timeout = 1223,
                RequiredAcks = 1,
                TransactionalID = transactionalId,
                TopicsData = new[] {
                    CreateTopicDataPartitionData(codec, new List<KafkaRecordHeader>
                    {
                        new KafkaRecordHeader { Key = TheKey, Value = Value },
                        new KafkaRecordHeader { Key = TheKey, Value = Value }
                    })
                }
            };

            var config = new SerializationConfig();
            config.SetSerializersForTopic("barbu", new StringSerializer(), new StringSerializer());
            config.SetDeserializersForTopic("barbu", new StringDeserializer(), new StringDeserializer());

            using (var serialized = produce.Serialize(Pool.Reserve(), 321, ClientId, config, Basics.ApiVersion.V3))
            {
                CheckHeader(Basics.ApiKey.ProduceRequest, 3, 321, TheClientId, serialized);
                Assert.AreEqual(transactionalId, Basics.DeserializeString(serialized));
                Assert.AreEqual(produce.RequiredAcks, BigEndianConverter.ReadInt16(serialized));
                Assert.AreEqual(produce.Timeout, BigEndianConverter.ReadInt32(serialized));
                Assert.AreEqual(1, BigEndianConverter.ReadInt32(serialized)); // 1 topic data
                Assert.AreEqual("barbu", Basics.DeserializeString(serialized));
                Assert.AreEqual(1, BigEndianConverter.ReadInt32(serialized)); // 1 partition data

                Assert.AreEqual(22, BigEndianConverter.ReadInt32(serialized)); // partition id
                var batchRecordSize = BigEndianConverter.ReadInt32(serialized); // number of bytes to read after this
                Assert.AreEqual(serialized.Length - serialized.Position, batchRecordSize);
                Assert.AreEqual(0, BigEndianConverter.ReadInt64(serialized)); // base offset
                var batchLength = BigEndianConverter.ReadInt32(serialized);
                var startOfBatchForLength = serialized.Position;
                Assert.AreEqual(-1, BigEndianConverter.ReadInt32(serialized)); // partitionLeader Epoch
                Assert.AreEqual(2, serialized.ReadByte()); // magic byte
                var crcParsed = BigEndianConverter.ReadInt32(serialized);
                var crcComputed = (int)Crc32.ComputeCastagnoli(serialized, serialized.Position,
                    serialized.Length - serialized.Position);
                Assert.AreEqual(crcParsed, crcComputed);
                Assert.AreEqual((short)codec, BigEndianConverter.ReadInt16(serialized)); // attributes
                Assert.AreEqual(1, BigEndianConverter.ReadInt32(serialized)); // lastoffsetdelta (2 messages)
                var firstTimeStamp = BigEndianConverter.ReadInt64(serialized);
                var maxTimeStamp = BigEndianConverter.ReadInt64(serialized);
                Assert.AreEqual(firstTimeStamp, maxTimeStamp); // the 2 messages have same timestamp
                Assert.AreEqual(-1L, BigEndianConverter.ReadInt64(serialized)); // producerId
                Assert.AreEqual(-1, BigEndianConverter.ReadInt16(serialized)); // producerEpoch
                Assert.AreEqual(-1, BigEndianConverter.ReadInt32(serialized)); // producerEpoch
                Assert.AreEqual(2, BigEndianConverter.ReadInt32(serialized)); // number of records

                var deserializer = new StringDeserializer();

                var records = Decompress(serialized, codec);

                // First record
                var firstRecordLength = VarIntConverter.ReadInt64(records);
                var startOfFirstRecord = records.Position;
                var recordsCount = 0L;
                Assert.AreEqual(0, records.ReadByte()); // attributes
                Assert.AreEqual(0, VarIntConverter.ReadInt64(records)); // TimeStampDelta
                Assert.AreEqual(0, VarIntConverter.ReadInt64(records)); // offsetDelta
                Assert.AreEqual(-1, VarIntConverter.ReadInt64(records)); // keyLength
                // Since the keyLength is -1, we do not read the key
                Assert.AreEqual(Value.Length, VarIntConverter.ReadInt64(records)); //value length
                Assert.AreEqual(TheValue, deserializer.Deserialize(records, Value.Length));
                recordsCount = VarIntConverter.ReadInt64(records);
                Assert.AreEqual(2L, recordsCount); // Headers count
                for (var i = 0; i < recordsCount; ++i)
                {
                    Assert.AreEqual(TheKey, Basics.DeserializeStringWithVarIntSize(records));
                    CollectionAssert.AreEqual(Value, Basics.DeserializeBytesWithVarIntSize(records));
                }
                var endOfFirstRecord = records.Position;
                Assert.AreEqual(firstRecordLength, endOfFirstRecord - startOfFirstRecord);

                // Second record
                var secondRecordLength = VarIntConverter.ReadInt64(records);
                var startOfSecondRecord = records.Position;
                Assert.AreEqual(0, records.ReadByte()); // attributes
                Assert.AreEqual(0, VarIntConverter.ReadInt64(records)); // TimeStampDelta
                Assert.AreEqual(1, VarIntConverter.ReadInt64(records)); // offsetDelta
                Assert.AreEqual(-1, VarIntConverter.ReadInt64(records)); // keyLength
                // Since the keyLength is -1, we do not read the key
                Assert.AreEqual(Value.Length, VarIntConverter.ReadInt64(records)); //value length
                Assert.AreEqual(TheValue, deserializer.Deserialize(records, Value.Length));
                recordsCount = VarIntConverter.ReadInt64(records);
                Assert.AreEqual(2L, recordsCount); // Headers count
                for (var i = 0; i < recordsCount; ++i)
                {
                    Assert.AreEqual(TheKey, Basics.DeserializeStringWithVarIntSize(records));
                    CollectionAssert.AreEqual(Value, Basics.DeserializeBytesWithVarIntSize(records));
                }
                var endOfSecondRecord = records.Position;
                Assert.AreEqual(secondRecordLength, endOfSecondRecord - startOfSecondRecord);

                var endOfBatchForLength = serialized.Position;
                Assert.AreEqual(batchLength, endOfBatchForLength - startOfBatchForLength);

                if (codec != CompressionCodec.None)
                {
                    records.Dispose();
                }
            }
        }

        private ReusableMemoryStream Decompress(ReusableMemoryStream compressed, CompressionCodec codec)
        {
            if (codec == CompressionCodec.None)
            {
                return compressed;
            }
            if (codec == CompressionCodec.Gzip)
            {
                var recordsBuffer = new MemoryStream(compressed.GetBuffer(), (int) compressed.Position,
                    (int) (compressed.Length - compressed.Position)); // disposed by GzipStream

                var decompressed = compressed.Pool.Reserve();
                using (var zipped = new GZipStream(recordsBuffer, CompressionMode.Decompress))
                {
                    using (var tmp = Pool.Reserve())
                    {
                        zipped.ReusableCopyTo(decompressed, tmp);
                    }

                    compressed.Position += recordsBuffer.Position;
                }

                decompressed.Position = 0;
                return decompressed;
            }

            throw new ArgumentException("invalid codec", nameof(codec));
        }

        [Test]
        public void TestSerializeHeaders()
        {
            var header = new KafkaRecordHeader { Key = "foo", Value = Value };

            using (var serialized = Pool.Reserve())
            {
                Record.SerializeHeader(serialized, header);
                serialized.Position = 0;
                var actual = Record.DeserializeHeader(serialized);
                Assert.AreEqual(header.Key, actual.Key);
                CollectionAssert.AreEqual(header.Value, actual.Value);

                header = new KafkaRecordHeader { Key = null, Value = Value };
                Assert.Throws<ArgumentNullException>(() => Record.SerializeHeader(serialized, header));

                header = new KafkaRecordHeader { Key = "foo", Value = null };
                Assert.Throws<ArgumentNullException>(() => Record.SerializeHeader(serialized, header));
            }
        }

        [Test]
        public void TestSerializedSizeOfHeader()
        {
            var header = new KafkaRecordHeader { Key = null, Value = Value };
            Assert.Throws<ArgumentNullException>(() => Record.SerializedSizeOfHeader(header));

            header = new KafkaRecordHeader { Key = "foo", Value = null };
            Assert.Throws<ArgumentNullException>(() => Record.SerializedSizeOfHeader(header));
        }

        [Test]
        public void TestSerializeOffsetRequest_V0()
        {
            var offset = new OffsetRequest
            {
                TopicsData =
                    new[]
                    {
                        new TopicData<OffsetPartitionData>
                        {
                            TopicName = "boloss",
                            PartitionsData =
                                new[]
                                { new OffsetPartitionData { MaxNumberOfOffsets = 3, Partition = 123, Time = 21341 } }
                        }
                    }
            };

            using (var serialized = offset.Serialize(new ReusableMemoryStream(null), 1235, ClientId, null, Basics.ApiVersion.V0))
            {
                CheckHeader(Basics.ApiKey.OffsetRequest, 0, 1235, TheClientId, serialized);
                Assert.AreEqual(-1, BigEndianConverter.ReadInt32(serialized)); // ReplicaId
                Assert.AreEqual(1, BigEndianConverter.ReadInt32(serialized)); // 1 topic data
                Assert.AreEqual(offset.TopicsData.First().TopicName, Basics.DeserializeString(serialized));
                Assert.AreEqual(1, BigEndianConverter.ReadInt32(serialized)); // 1 partition data
                var od = new OffsetPartitionData();
                od.Deserialize(serialized, null, Basics.ApiVersion.V0);
                Assert.AreEqual(123, od.Partition);
                Assert.AreEqual(21341, od.Time);
                Assert.AreEqual(3, od.MaxNumberOfOffsets);
            }
        }

        [Test]
        public void TestSerializeOffsetRequest_V1()
        {
            var offset = new OffsetRequest
            {
                TopicsData =
                    new[]
                    {
                        new TopicData<OffsetPartitionData>
                        {
                            TopicName = "boloss",
                            PartitionsData =
                                new[]
                                { new OffsetPartitionData { MaxNumberOfOffsets = 3, Partition = 123, Time = 21341 } }
                        }
                    }
            };

            using (var serialized = offset.Serialize(new ReusableMemoryStream(null), 1235, ClientId, null, Basics.ApiVersion.V1))
            {
                CheckHeader(Basics.ApiKey.OffsetRequest, 1, 1235, TheClientId, serialized);
                Assert.AreEqual(-1, BigEndianConverter.ReadInt32(serialized)); // ReplicaId
                Assert.AreEqual(1, BigEndianConverter.ReadInt32(serialized)); // 1 topic data
                Assert.AreEqual(offset.TopicsData.First().TopicName, Basics.DeserializeString(serialized));
                Assert.AreEqual(1, BigEndianConverter.ReadInt32(serialized)); // 1 partition data
                var od = new OffsetPartitionData();
                od.Deserialize(serialized, null, Basics.ApiVersion.V1);
                Assert.AreEqual(123, od.Partition);
                Assert.AreEqual(21341, od.Time);
                Assert.AreEqual(0, od.MaxNumberOfOffsets); // This field does not exist anymore, so it should get its default value
            }
        }

        private static void TestCommonResponse<TPartitionResponse>(CommonResponse<TPartitionResponse> response,
            CommonResponse<TPartitionResponse> expected, Func<TPartitionResponse, TPartitionResponse, bool> comparer)
            where TPartitionResponse : IMemoryStreamSerializable, new()
        {
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

        private ProduceResponse CreateProducerResponse(ProducePartitionResponse producePartitionResponse)
        {
            return new ProduceResponse
            {
                ProducePartitionResponse =
                    new CommonResponse<ProducePartitionResponse>
                    {
                        TopicsResponse =
                            new[]
                            {
                                new TopicData<ProducePartitionResponse>
                                {
                                    TopicName = "topic",
                                    PartitionsData =
                                        new[]
                                        {
                                            producePartitionResponse
                                        }
                                }
                            }
                    }
            };
        }

        [Test]
        public void TestDeserializeProduceResponse_V0()
        {
            var response = CreateProducerResponse(new ProducePartitionResponse
            {
                ErrorCode = ErrorCode.InvalidMessage,
                Offset = 2312,
                Partition = 34
            });

            using (var serialized = new ReusableMemoryStream(null))
            {
                Basics.WriteArray(serialized, response.ProducePartitionResponse.TopicsResponse, Basics.ApiVersion.V0);
                serialized.Position = 0;

                var p = new ProduceResponse();
                p.Deserialize(serialized, null, Basics.ApiVersion.V0);
                TestCommonResponse(p.ProducePartitionResponse, response.ProducePartitionResponse,
                    (p1, p2) => p1.Partition == p2.Partition && p1.Offset == p2.Offset && p1.ErrorCode == p2.ErrorCode && p1.Timestamp == 0);
            }
        }

        [Test]
        public void TestDeserializeProduceResponse_v2()
        {
            var response = CreateProducerResponse(new ProducePartitionResponse
            {
                ErrorCode = ErrorCode.InvalidMessage,
                Offset = 2312,
                Partition = 34,
                Timestamp = 42
            });

            using (var serialized = new ReusableMemoryStream(null))
            {
                Basics.WriteArray(serialized, response.ProducePartitionResponse.TopicsResponse, Basics.ApiVersion.V2);
                BigEndianConverter.Write(serialized, 47);
                serialized.Position = 0;

                var p = new ProduceResponse();
                p.Deserialize(serialized, null, Basics.ApiVersion.V2);

                Assert.AreEqual(47, p.ThrottleTime);
                TestCommonResponse(p.ProducePartitionResponse, response.ProducePartitionResponse,
                    (p1, p2) => p1.Partition == p2.Partition && p1.Offset == p2.Offset && p1.ErrorCode == p2.ErrorCode && p1.Timestamp == 42);
            }
        }

        [Test]
        public void TestDeserializeProduceResponse_v5()
        {
            var response = CreateProducerResponse(new ProducePartitionResponse
            {
                ErrorCode = ErrorCode.InvalidMessage,
                Offset = 2312,
                Partition = 34,
                Timestamp = 42,
                LogStartOffset = 12,
            });

            using (var serialized = new ReusableMemoryStream(null))
            {
                Basics.WriteArray(serialized, response.ProducePartitionResponse.TopicsResponse, Basics.ApiVersion.V5);
                BigEndianConverter.Write(serialized, 47);
                serialized.Position = 0;

                var p = new ProduceResponse();
                p.Deserialize(serialized, null, Basics.ApiVersion.V5);

                Assert.AreEqual(47, p.ThrottleTime);
                TestCommonResponse(p.ProducePartitionResponse, response.ProducePartitionResponse,
                    (p1, p2) => p1.Partition == p2.Partition && p1.Offset == p2.Offset && p1.ErrorCode == p2.ErrorCode
                        && p1.Timestamp == 42 && p1.LogStartOffset == 12);
            }
        }

        [Test]
        public void TestDeserializeOffsetResponse_V0()
        {
            var response = new CommonResponse<OffsetPartitionResponse>
            {
                TopicsResponse =
                    new[]
                    {
                        new TopicData<OffsetPartitionResponse>
                        {
                            TopicName = "yoleeroy",
                            PartitionsData =
                                new[]
                                {
                                    new OffsetPartitionResponse
                                    {
                                        ErrorCode = ErrorCode.BrokerNotAvailable,
                                        Partition = 32,
                                        Offsets = new[] { 1L, 142L }
                                    }
                                }
                        }
                    }
            };

            using (var serialized = new ReusableMemoryStream(null))
            {
                Basics.WriteArray(serialized, response.TopicsResponse);
                serialized.Position = 0;

                var o = new CommonResponse<OffsetPartitionResponse>();
                o.Deserialize(serialized, null, Basics.ApiVersion.V0);

                TestCommonResponse(o, response, (p1, p2) =>
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
        public void TestDeserializeOffsetResponse_V1()
        {
            var response = new CommonResponse<OffsetPartitionResponse>
            {
                TopicsResponse =
                    new[]
                    {
                        new TopicData<OffsetPartitionResponse>
                        {
                            TopicName = "yoleeroy",
                            PartitionsData =
                                new[]
                                {
                                    new OffsetPartitionResponse
                                    {
                                        ErrorCode = ErrorCode.BrokerNotAvailable,
                                        Partition = 32,
                                        Timestamp = 42,
                                        Offsets = new[] { 1L, 142L }
                                    }
                                }
                        }
                    }
            };

            using (var serialized = new ReusableMemoryStream(null))
            {
                Basics.WriteArray(serialized, response.TopicsResponse, Basics.ApiVersion.V1);
                serialized.Position = 0;

                var o = new CommonResponse<OffsetPartitionResponse>();
                o.Deserialize(serialized, null, Basics.ApiVersion.V1);

                TestCommonResponse(o, response, (p1, p2) =>
                {
                    if (p1.Partition != p2.Partition)
                        return false;
                    if (p1.ErrorCode != p2.ErrorCode)
                        return false;
                    Assert.AreEqual(p1.Timestamp, p2.Timestamp);
                    Assert.AreEqual(1, p2.Offsets.Length);
                    return true;
                });
            }
        }

        [Test]
        public void TestDeserializeFetchResponse_V0()
        {
            var response = new FetchResponse
            {
                FetchPartitionResponse =
                    new CommonResponse<FetchPartitionResponse>
                    {
                        TopicsResponse =
                            new[]
                            {
                                new TopicData<FetchPartitionResponse>
                                {
                                    TopicName = "Buffy_contre_les_zombies",
                                    PartitionsData =
                                        new[]
                                        {
                                            new FetchPartitionResponse
                                            {
                                                ErrorCode = ErrorCode.NoError,
                                                HighWatermarkOffset = 714,
                                                Partition = 999999,
                                                Messages =
                                                    new List<ResponseMessage>
                                                    {
                                                        new ResponseMessage
                                                        {
                                                            Offset = 44,
                                                            Message = new Message { Key = Key, Value = Value }
                                                        }
                                                    }
                                            }
                                        }
                                }
                            }
                    }
            };

            using (var serialized = new ReusableMemoryStream(null))
            {
                var config = new SerializationConfig();
                Basics.WriteArray(serialized, response.FetchPartitionResponse.TopicsResponse, config, Basics.ApiVersion.Ignored);
                serialized.Position = 0;

                var f = new FetchResponse();
                f.Deserialize(serialized, config, Basics.ApiVersion.V0);

                Assert.AreEqual(0, f.ThrottleTime);
                TestCommonResponse(f.FetchPartitionResponse, response.FetchPartitionResponse, (p1, p2) =>
                {
                    Assert.AreEqual(p1.Partition, p2.Partition);
                    Assert.AreEqual(p1.ErrorCode, p2.ErrorCode);
                    Assert.AreEqual(p1.HighWatermarkOffset, p2.HighWatermarkOffset);
                    Assert.AreEqual(p1.Messages.Count, p2.Messages.Count);
                    foreach (var zipped in p1.Messages.Zip(p2.Messages, Tuple.Create))
                    {
                        Assert.AreEqual(zipped.Item1.Offset, zipped.Item2.Offset);
                        CollectionAssert.AreEqual(zipped.Item1.Message.Key as byte[], zipped.Item2.Message.Key as byte[]);
                        CollectionAssert.AreEqual(zipped.Item1.Message.Value as byte[],
                            zipped.Item2.Message.Value as byte[]);
                    }

                    return true;
                });
            }
        }

        [Test]
        public void TestDeserializeFetchResponse_V2()
        {
            var response = new FetchResponse
            {
                ThrottleTime = 47,
                FetchPartitionResponse =
                    new CommonResponse<FetchPartitionResponse>
                    {
                        TopicsResponse =
                            new[]
                            {
                                new TopicData<FetchPartitionResponse>
                                {
                                    TopicName = "Buffy_contre_les_zombies",
                                    PartitionsData =
                                        new[]
                                        {
                                            new FetchPartitionResponse
                                            {
                                                ErrorCode = ErrorCode.NoError,
                                                HighWatermarkOffset = 714,
                                                Partition = 999999,
                                                Messages =
                                                    new List<ResponseMessage>
                                                    {
                                                        new ResponseMessage
                                                        {
                                                            Offset = 44,
                                                            Message = new Message { Key = Key, Value = Value }
                                                        }
                                                    }
                                            }
                                        }
                                }
                            }
                    }
            };

            using (var serialized = new ReusableMemoryStream(null))
            {
                var config = new SerializationConfig();
                BigEndianConverter.Write(serialized, response.ThrottleTime);
                Basics.WriteArray(serialized, response.FetchPartitionResponse.TopicsResponse, config, Basics.ApiVersion.Ignored);
                serialized.Position = 0;

                var f = new FetchResponse();
                f.Deserialize(serialized, config, Basics.ApiVersion.V1);

                Assert.AreEqual(response.ThrottleTime, f.ThrottleTime);
                TestCommonResponse(f.FetchPartitionResponse, response.FetchPartitionResponse, (p1, p2) =>
                {
                    Assert.AreEqual(p1.Partition, p2.Partition);
                    Assert.AreEqual(p1.ErrorCode, p2.ErrorCode);
                    Assert.AreEqual(p1.HighWatermarkOffset, p2.HighWatermarkOffset);
                    Assert.AreEqual(p1.Messages.Count, p2.Messages.Count);
                    foreach (var zipped in p1.Messages.Zip(p2.Messages, Tuple.Create))
                    {
                        Assert.AreEqual(zipped.Item1.Offset, zipped.Item2.Offset);
                        CollectionAssert.AreEqual(zipped.Item1.Message.Key as byte[], zipped.Item2.Message.Key as byte[]);
                        CollectionAssert.AreEqual(zipped.Item1.Message.Value as byte[],
                            zipped.Item2.Message.Value as byte[]);
                    }

                    return true;
                });
            }
        }

        [TestCase(CompressionCodec.None)]
        [TestCase(CompressionCodec.Gzip)]
        public void TestDeserializeFetchResponse_V4(CompressionCodec compression)
        {
            var response = new FetchResponse
            {
                ThrottleTime = 47,
                FetchPartitionResponse =
                    new CommonResponse<FetchPartitionResponse>
                    {
                        TopicsResponse =
                            new[]
                            {
                                new TopicData<FetchPartitionResponse>
                                {
                                    TopicName = "Buffy_contre_les_zombies",
                                    PartitionsData =
                                        new[]
                                        {
                                            new FetchPartitionResponse
                                            {
                                                ErrorCode = ErrorCode.NoError,
                                                HighWatermarkOffset = 714,
                                                Partition = 999999,
                                                LastStableOffset = 404,
                                                AbortedTransactions = new []
                                                {
                                                    new AbortedTransaction{FirstOffset = 3, ProducerId = 4}, 
                                                },
                                                Messages =
                                                    new List<ResponseMessage>
                                                    {
                                                        new ResponseMessage
                                                        {
                                                            Offset = 0,
                                                            Message = new Message { Key = Key, Value = Value }
                                                        }
                                                    },
                                                Compression = compression
                                            }
                                        }
                                }
                            }
                    }
            };

            using (var serialized = new ReusableMemoryStream(Pool))
            {
                var config = new SerializationConfig();
                BigEndianConverter.Write(serialized, response.ThrottleTime);
                Basics.WriteArray(serialized, response.FetchPartitionResponse.TopicsResponse, config, Basics.ApiVersion.V4);
                serialized.Position = 0;

                var f = new FetchResponse();
                f.Deserialize(serialized, config, Basics.ApiVersion.V4);

                Assert.AreEqual(response.ThrottleTime, f.ThrottleTime);
                TestCommonResponse(f.FetchPartitionResponse, response.FetchPartitionResponse, (p1, p2) =>
                {
                    Assert.AreEqual(p1.Partition, p2.Partition);
                    Assert.AreEqual(p1.ErrorCode, p2.ErrorCode);
                    Assert.AreEqual(p1.HighWatermarkOffset, p2.HighWatermarkOffset);
                    Assert.AreEqual(p1.LastStableOffset, p2.LastStableOffset);
                    Assert.AreEqual(p1.AbortedTransactions.Length, p2.AbortedTransactions.Length);
                    foreach (var zipped in p1.AbortedTransactions.Zip(p2.AbortedTransactions, Tuple.Create))
                    {
                        Assert.AreEqual(zipped.Item1.ProducerId, zipped.Item2.ProducerId);
                        Assert.AreEqual(zipped.Item1.FirstOffset, zipped.Item2.FirstOffset);
                    }
                    Assert.AreEqual(p1.Messages.Count, p2.Messages.Count);
                    foreach (var zipped in p1.Messages.Zip(p2.Messages, Tuple.Create))
                    {
                        Assert.AreEqual(zipped.Item1.Offset, zipped.Item2.Offset);
                        CollectionAssert.AreEqual(zipped.Item1.Message.Key as byte[], zipped.Item2.Message.Key as byte[]);
                        CollectionAssert.AreEqual(zipped.Item1.Message.Value as byte[],
                            zipped.Item2.Message.Value as byte[]);
                    }

                    return true;
                });
            }
        }

        [Test]
        public void TestDeserializeFetchResponse_V5()
        {
            var response = new FetchResponse
            {
                ThrottleTime = 47,
                FetchPartitionResponse =
                    new CommonResponse<FetchPartitionResponse>
                    {
                        TopicsResponse =
                            new[]
                            {
                                new TopicData<FetchPartitionResponse>
                                {
                                    TopicName = "Buffy_contre_les_zombies",
                                    PartitionsData =
                                        new[]
                                        {
                                            new FetchPartitionResponse
                                            {
                                                ErrorCode = ErrorCode.NoError,
                                                HighWatermarkOffset = 714,
                                                Partition = 999999,
                                                LastStableOffset = 404,
                                                LogStartOffset = 200,
                                                AbortedTransactions = new []
                                                {
                                                    new AbortedTransaction{FirstOffset = 3, ProducerId = 4},
                                                },
                                                Messages =
                                                    new List<ResponseMessage>
                                                    {
                                                        new ResponseMessage
                                                        {
                                                            Offset = 0,
                                                            Message = new Message { Key = Key, Value = Value }
                                                        }
                                                    }
                                            }
                                        }
                                }
                            }
                    }
            };

            using (var serialized = new ReusableMemoryStream(null))
            {
                var config = new SerializationConfig();
                BigEndianConverter.Write(serialized, response.ThrottleTime);
                Basics.WriteArray(serialized, response.FetchPartitionResponse.TopicsResponse, config, Basics.ApiVersion.V5);
                serialized.Position = 0;

                var f = new FetchResponse();
                f.Deserialize(serialized, config, Basics.ApiVersion.V5);

                Assert.AreEqual(response.ThrottleTime, f.ThrottleTime);
                TestCommonResponse(f.FetchPartitionResponse, response.FetchPartitionResponse, (p1, p2) =>
                {
                    Assert.AreEqual(p1.Partition, p2.Partition);
                    Assert.AreEqual(p1.ErrorCode, p2.ErrorCode);
                    Assert.AreEqual(p1.HighWatermarkOffset, p2.HighWatermarkOffset);
                    Assert.AreEqual(p1.LastStableOffset, p2.LastStableOffset);
                    Assert.AreEqual(p1.LogStartOffset, p2.LogStartOffset);
                    Assert.AreEqual(p1.AbortedTransactions.Length, p2.AbortedTransactions.Length);
                    foreach (var zipped in p1.AbortedTransactions.Zip(p2.AbortedTransactions, Tuple.Create))
                    {
                        Assert.AreEqual(zipped.Item1.ProducerId, zipped.Item2.ProducerId);
                        Assert.AreEqual(zipped.Item1.FirstOffset, zipped.Item2.FirstOffset);
                    }
                    Assert.AreEqual(p1.Messages.Count, p2.Messages.Count);
                    foreach (var zipped in p1.Messages.Zip(p2.Messages, Tuple.Create))
                    {
                        Assert.AreEqual(zipped.Item1.Offset, zipped.Item2.Offset);
                        CollectionAssert.AreEqual(zipped.Item1.Message.Key as byte[], zipped.Item2.Message.Key as byte[]);
                        CollectionAssert.AreEqual(zipped.Item1.Message.Value as byte[],
                            zipped.Item2.Message.Value as byte[]);
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
            using (var serialized = new ReusableMemoryStream(null))
            {
                Basics.SerializeString(serialized, TheValue);
                Assert.AreEqual(2 + Value.Length, serialized.Length);
                serialized.Position = 0;
                Assert.AreEqual(Value.Length, BigEndianConverter.ReadInt16(serialized));
                CompareArrays(Value, serialized.GetBuffer(), 2);
            }

            // Null string
            using (var serialized = new ReusableMemoryStream(null))
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
            using (var serialized = new ReusableMemoryStream(null))
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
        public void Test005_SerializeBytes()
        {
            // Non null byte[]
            using (var serialized = new ReusableMemoryStream(null))
            {
                Basics.SerializeBytes(serialized, Value);
                Assert.AreEqual(4 + Value.Length, serialized.Length);
                serialized.Position = 0;
                Assert.AreEqual(Value.Length, BigEndianConverter.ReadInt32(serialized));
                CompareArrays(Value, serialized.GetBuffer(), 4);
            }

            // Null byte[]
            using (var serialized = new ReusableMemoryStream(null))
            {
                Basics.SerializeBytes(serialized, null);
                Assert.AreEqual(4, serialized.Length);
                serialized.Position = 0;
                Assert.AreEqual(-1, BigEndianConverter.ReadInt32(serialized));
            }
        }

        [Test]
        public void Test006_DeserializeBytes()
        {
            using (var serialized = new ReusableMemoryStream(null))
            {
                // Non null byte[]
                BigEndianConverter.Write(serialized, Value.Length);
                serialized.Write(Value, 0, Value.Length);
                serialized.Position = 0;

                CompareArrays(Value, Basics.DeserializeBytes(serialized), 0);

                // Null byte[]
                serialized.SetLength(0);
                serialized.WriteByte(0xFF);
                serialized.WriteByte(0xFF);
                serialized.WriteByte(0xFF);
                serialized.WriteByte(0xFF);
                serialized.Position = 0;

                Assert.IsNull(Basics.DeserializeBytes(serialized));
            }
        }

        [Test]
        public void Test001_SerializeArray()
        {
            var array = new[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 109 };
            using (var serialized = new ReusableMemoryStream(null))
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
            using (var serialized = new ReusableMemoryStream(null))
            {
                Basics.WriteArray(serialized, array, BigEndianConverter.Write);
                serialized.Position = 0;

                var deserialized = Basics.DeserializeArray(serialized, BigEndianConverter.ReadInt32);
                CollectionAssert.AreEqual(array, deserialized);
            }

            using (var serialized = new ReusableMemoryStream(null))
            {
                serialized.Write(Basics.MinusOne32, 0, 4);
                serialized.Position = 0;

                var deserialized = Basics.DeserializeArray(serialized, BigEndianConverter.ReadInt32);
                Assert.AreEqual(0, deserialized.Length);
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

            using (var s = new ReusableMemoryStream(null))
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


                var n3 = (((long) b0 << 56)
                    | ((long) b1 << 48 | ((long) b2 << 40) | ((long) b3 << 32) | ((long) b4 << 24) | ((long) b5 << 16)
                        | ((long) b6 << 8) | b7));
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

        [Test]
        public void TestOffsets()
        {
            Assert.That(Offsets.Latest, Is.EqualTo(-1));
            Assert.That(Offsets.Earliest, Is.EqualTo(-2));
        }

        [Test]
        public void TestDefaultSerialization()
        {
            using (var stream = new MemoryStream())
            {
                Assert.That(() => ByteArraySerialization.DefaultSerializer.Serialize("toto", stream),
                    Throws.TypeOf<ArgumentException>());
                Assert.That(() => ByteArraySerialization.DefaultSerializer.Serialize(Value, stream),
                    Is.EqualTo(Value.Length));
                Assert.That(stream.Length, Is.EqualTo(Value.Length));
                CompareArrays(Value, stream.GetBuffer(), 0);

                stream.Position = 0;
                var output = ByteArraySerialization.DefaultDeserializer.Deserialize(stream, Value.Length) as byte[];
                CollectionAssert.AreEqual(Value, output);
            }
        }

        [Test]
        public void TestStringSerialization()
        {
            using (var stream = new MemoryStream())
            {
                var serializer = new StringSerializer();
                Assert.That(() => serializer.Serialize(Value, stream), Throws.TypeOf<ArgumentException>());
                Assert.That(() => serializer.Serialize(TheValue, stream), Is.EqualTo(Value.Length));
                Assert.That(stream.Length, Is.EqualTo(Value.Length));
                CompareArrays(Value, stream.GetBuffer(), 0);

                var deserializer = new StringDeserializer();
                stream.Position = 0;
                var output = deserializer.Deserialize(stream, Value.Length) as string;
                Assert.That(output, Is.EqualTo(TheValue));
            }
        }

        [Test]
        public void TestSerializationConfig()
        {
            var config = new SerializationConfig();
            ISerializer ser = new StringSerializer();
            IDeserializer deser = new StringDeserializer();

            var t1 = Tuple.Create(ser, ser);
            var t2 = Tuple.Create(ser, ser);
            Assert.That(t1, Is.EqualTo(t2));

            var s = config.GetSerializersForTopic("topicnotfound");
            Assert.That(s,
                Is.EqualTo(Tuple.Create(ByteArraySerialization.DefaultSerializer,
                    ByteArraySerialization.DefaultSerializer)));

            var d = config.GetDeserializersForTopic("topicnotfound");
            Assert.That(d,
                Is.EqualTo(Tuple.Create(ByteArraySerialization.DefaultDeserializer,
                    ByteArraySerialization.DefaultDeserializer)));

            config.SetSerializersForTopic("topicnotfound", ser, ser);
            config.SetDeserializersForTopic("topicnotfound", deser, deser);

            s = config.GetSerializersForTopic("topicnotfound");
            Assert.That(s, Is.EqualTo(Tuple.Create(ser, ser)));

            d = config.GetDeserializersForTopic("topicnotfound");
            Assert.That(d, Is.EqualTo(Tuple.Create(deser, deser)));

            var config2 = new SerializationConfig(config);
            s = config2.GetSerializersForTopic("topicnotfound");
            Assert.That(s, Is.EqualTo(Tuple.Create(ser, ser)));

            d = config2.GetDeserializersForTopic("topicnotfound");
            Assert.That(d, Is.EqualTo(Tuple.Create(deser, deser)));

            config2.SetSerializersForTopic("topicnotfound", null, ser);
            config2.SetDeserializersForTopic("topicnotfound", null, deser);

            s = config2.GetSerializersForTopic("topicnotfound");
            Assert.That(s, Is.EqualTo(Tuple.Create(ByteArraySerialization.DefaultSerializer, ser)));

            d = config2.GetDeserializersForTopic("topicnotfound");
            Assert.That(d, Is.EqualTo(Tuple.Create(ByteArraySerialization.DefaultDeserializer, deser)));

            config2.SetSerializersForTopic("topicnotfound", ser, null);
            config2.SetDeserializersForTopic("topicnotfound", deser, null);

            s = config2.GetSerializersForTopic("topicnotfound");
            Assert.That(s, Is.EqualTo(Tuple.Create(ser, ByteArraySerialization.DefaultSerializer)));

            d = config2.GetDeserializersForTopic("topicnotfound");
            Assert.That(d, Is.EqualTo(Tuple.Create(deser, ByteArraySerialization.DefaultDeserializer)));
        }

        [Test]
        public void TestChangeDefaultSerialization()
        {
            var config = new SerializationConfig();
            ISerializer ser = new StringSerializer();
            IDeserializer deser = new StringDeserializer();

            var s = config.GetSerializersForTopic("topicnotfound");
            Assert.That(s,
                Is.EqualTo(Tuple.Create(ByteArraySerialization.DefaultSerializer,
                    ByteArraySerialization.DefaultSerializer)));

            var d = config.GetDeserializersForTopic("topicnotfound");
            Assert.That(d,
                Is.EqualTo(Tuple.Create(ByteArraySerialization.DefaultDeserializer,
                    ByteArraySerialization.DefaultDeserializer)));

            config.SetDefaultSerializers(ser, ser);
            config.SetDefaultDeserializers(deser, deser);

            s = config.GetSerializersForTopic("topicnotfound");
            Assert.That(s, Is.EqualTo(Tuple.Create(ser, ser)));

            d = config.GetDeserializersForTopic("topicnotfound");
            Assert.That(d, Is.EqualTo(Tuple.Create(deser, deser)));
        }

        [Test]
        public void TestUnsupportedMagicByte()
        {
            using (var serialized = new ReusableMemoryStream(null))
            {
                var set = new PartitionData
                {
                    Partition = 42,
                    CompressionCodec = CompressionCodec.None,
                    Messages = new[] { new Message { Key = Key, Value = Value } }
                };
                set.Serialize(serialized, SerializationConfig.ByteArraySerializers, Basics.ApiVersion.Ignored);
                serialized[24] = 8; // set non supported magic byte
                serialized.Position = 4;

                Assert.That(
                    () =>
                        FetchPartitionResponse.DeserializeMessageSet(serialized,
                            SerializationConfig.ByteArrayDeserializers),
                    Throws.InstanceOf<UnsupportedMagicByteVersion>());
            }
        }

        [Test]
        public void TestBadCrc()
        {
            using (var serialized = new ReusableMemoryStream(null))
            {
                var set = new PartitionData
                {
                    Partition = 42,
                    CompressionCodec = CompressionCodec.None,
                    Messages = new[] { new Message { Key = Key, Value = Value } }
                };
                set.Serialize(serialized, SerializationConfig.ByteArraySerializers, Basics.ApiVersion.Ignored);
                serialized[20] = 8; // change crc
                serialized.Position = 4;

                Assert.That(
                    () =>
                        FetchPartitionResponse.DeserializeMessageSet(serialized,
                            SerializationConfig.ByteArrayDeserializers), Throws.InstanceOf<CrcException>());
            }
        }

        [Test]
        public void TestBadCompression()
        {
            using (var serialized = Pool.Reserve())
            {
                var set = new PartitionData
                {
                    Partition = 42,
                    CompressionCodec = CompressionCodec.Gzip,
                    Messages = new[] { new Message { Key = Key, Value = Value } }
                };
                set.Serialize(serialized, SerializationConfig.ByteArraySerializers, Basics.ApiVersion.Ignored);

                // corrupt compressed data
                serialized[37] = 8;

                // recompute crc
                var crc = (int) Crc32.Compute(serialized, 24, serialized.Length - 24);
                serialized.Position = 20;
                BigEndianConverter.Write(serialized, crc);

                // go
                serialized.Position = 4;
                Assert.That(
                    () =>
                        FetchPartitionResponse.DeserializeMessageSet(serialized,
                            SerializationConfig.ByteArrayDeserializers), Throws.InstanceOf<UncompressException>());
            }
        }

        [Test]
        public void TestSerializeJoinConsumerGroupRequest_V0()
        {
            var join = new JoinConsumerGroupRequest
            {
                GroupId = "testgroup",
                MemberId = "member",
                RebalanceTimeout = 42,
                SessionTimeout = 49,
                Subscription = new[] { "topic1", "topic2" }
            };

            using (var serialized = join.Serialize(new ReusableMemoryStream(null), 61, ClientId, null, Basics.ApiVersion.V0))
            {
                CheckHeader(Basics.ApiKey.JoinGroupRequest, 0, 61, TheClientId, serialized);
                Assert.AreEqual("testgroup", Basics.DeserializeString(serialized));
                Assert.AreEqual(49, BigEndianConverter.ReadInt32(serialized));
                Assert.AreEqual("member", Basics.DeserializeString(serialized));
                Assert.AreEqual("consumer", Basics.DeserializeString(serialized));
                Assert.AreEqual(1, BigEndianConverter.ReadInt32(serialized)); // size 1 array
                Assert.AreEqual("kafka-sharp-consumer", Basics.DeserializeString(serialized));
                Assert.AreEqual(serialized.Length - (serialized.Position + 4), BigEndianConverter.ReadInt32(serialized));
                // Protocol metadata "opaque" bytes
                Assert.AreEqual(0, BigEndianConverter.ReadInt16(serialized));
                Assert.AreEqual(2, BigEndianConverter.ReadInt32(serialized)); // size 2 array
                Assert.AreEqual("topic1", Basics.DeserializeString(serialized));
                Assert.AreEqual("topic2", Basics.DeserializeString(serialized));
                Assert.IsNull(Basics.DeserializeBytes(serialized));
            }
        }

        [Test]
        public void TestSerializeJoinConsumerGroupRequest_V1()
        {
            var join = new JoinConsumerGroupRequest
            {
                GroupId = "testgroup",
                MemberId = "member",
                RebalanceTimeout = 42,
                SessionTimeout = 49,
                Subscription = new[] { "topic1", "topic2" }
            };

            using (var serialized = join.Serialize(new ReusableMemoryStream(null), 61, ClientId, null, Basics.ApiVersion.V1))
            {
                CheckHeader(Basics.ApiKey.JoinGroupRequest, 1, 61, TheClientId, serialized);
                Assert.AreEqual("testgroup", Basics.DeserializeString(serialized));
                Assert.AreEqual(49, BigEndianConverter.ReadInt32(serialized));
                Assert.AreEqual(42, BigEndianConverter.ReadInt32(serialized));
                Assert.AreEqual("member", Basics.DeserializeString(serialized));
                Assert.AreEqual("consumer", Basics.DeserializeString(serialized));
                Assert.AreEqual(1, BigEndianConverter.ReadInt32(serialized)); // size 1 array
                Assert.AreEqual("kafka-sharp-consumer", Basics.DeserializeString(serialized));
                Assert.AreEqual(serialized.Length - (serialized.Position + 4), BigEndianConverter.ReadInt32(serialized));
                // Protocol metadata "opaque" bytes
                Assert.AreEqual(0, BigEndianConverter.ReadInt16(serialized));
                Assert.AreEqual(2, BigEndianConverter.ReadInt32(serialized)); // size 2 array
                Assert.AreEqual("topic1", Basics.DeserializeString(serialized));
                Assert.AreEqual("topic2", Basics.DeserializeString(serialized));
                Assert.IsNull(Basics.DeserializeBytes(serialized));
            }
        }

        [Test]
        public void TestDeserializeJoinConsumerGroupResponse()
        {
            var groupResponse = new JoinConsumerGroupResponse
            {
                ErrorCode = ErrorCode.NoError,
                GenerationId = 421,
                GroupMembers = new[] { new GroupMember { MemberId = "member", Metadata = new ConsumerGroupProtocolMetadata() }, },
                GroupProtocol = "protocol",
                LeaderId = "toto",
                MemberId = "member"
            };

            using (var serialized = new ReusableMemoryStream(null))
            {
                groupResponse.Serialize(serialized, null, Basics.ApiVersion.Ignored);
                Assert.AreEqual(56, serialized.Length); // TODO: better check that serialization is correct?

                serialized.Position = 0;
                var joinResp = new JoinConsumerGroupResponse();
                joinResp.Deserialize(serialized, null, Basics.ApiVersion.Ignored);
                Assert.AreEqual(ErrorCode.NoError, joinResp.ErrorCode);
                Assert.AreEqual(421, joinResp.GenerationId);
                Assert.AreEqual("protocol", joinResp.GroupProtocol);
                Assert.AreEqual("toto", joinResp.LeaderId);
                Assert.AreEqual("member", joinResp.MemberId);
                Assert.AreEqual(1, joinResp.GroupMembers.Length);
                Assert.AreEqual("member", joinResp.GroupMembers[0].MemberId);
                Assert.AreEqual(0, joinResp.GroupMembers[0].Metadata.Version);
                CollectionAssert.IsEmpty(joinResp.GroupMembers[0].Metadata.Subscription);
                Assert.IsNull(joinResp.GroupMembers[0].Metadata.UserData);
            }
        }

        [Test]
        public void TestSerializeSyncConsumerGroupRequest()
        {
            var sync = new SyncConsumerGroupRequest
            {
                GroupId = "testgroup",
                MemberId = "member",
                GenerationId = 42,
                GroupAssignment =
                    new[]
                    {
                        new ConsumerGroupAssignment
                        {
                            MemberId = "member",
                            MemberAssignment =
                                new ConsumerGroupMemberAssignment
                                {
                                    Version = 678,
                                    UserData = null,
                                    PartitionAssignments =
                                        new[]
                                        {
                                            new TopicData<PartitionAssignment>
                                            {
                                                TopicName = "topic",
                                                PartitionsData =
                                                    new[]
                                                    {
                                                        new PartitionAssignment { Partition = 727 },
                                                        new PartitionAssignment { Partition = 728 }
                                                    }
                                            }
                                        }
                                }
                        }
                    }
            };

            using (var serialized = sync.Serialize(new ReusableMemoryStream(null), 61, ClientId, null, Basics.ApiVersion.Ignored))
            {
                CheckHeader(Basics.ApiKey.SyncGroupRequest, 0, 61, TheClientId, serialized);
                Assert.AreEqual("testgroup", Basics.DeserializeString(serialized));
                Assert.AreEqual(42, BigEndianConverter.ReadInt32(serialized));
                Assert.AreEqual("member", Basics.DeserializeString(serialized));
                Assert.AreEqual(1, BigEndianConverter.ReadInt32(serialized)); // size 1 array
                Assert.AreEqual("member", Basics.DeserializeString(serialized));
                Assert.AreEqual(serialized.Length - (serialized.Position + 4), BigEndianConverter.ReadInt32(serialized));
                // Member assignment "opaque" bytes
                Assert.AreEqual(678, BigEndianConverter.ReadInt16(serialized));
                Assert.AreEqual(1, BigEndianConverter.ReadInt32(serialized)); // size 1 array
                Assert.AreEqual("topic", Basics.DeserializeString(serialized));
                Assert.AreEqual(2, BigEndianConverter.ReadInt32(serialized)); // size 2 array
                Assert.AreEqual(727, BigEndianConverter.ReadInt32(serialized));
                Assert.AreEqual(728, BigEndianConverter.ReadInt32(serialized));
                Assert.IsNull(Basics.DeserializeBytes(serialized));
            }
        }

        [Test]
        public void TestDeserializeSyncConsumerGroupResponse()
        {
            using (var serialized = new ReusableMemoryStream(null))
            {
                BigEndianConverter.Write(serialized, (short) ErrorCode.NoError);
                Basics.WriteSizeInBytes(serialized, stream =>
                {
                    BigEndianConverter.Write(stream, (short) 48);
                    BigEndianConverter.Write(stream, 1);
                    Basics.SerializeString(stream, "topic");
                    BigEndianConverter.Write(stream, 2);
                    BigEndianConverter.Write(stream, 1);
                    BigEndianConverter.Write(stream, 7);
                    Basics.SerializeBytes(stream, null);
                });

                serialized.Position = 0;
                var syncResp = new SyncConsumerGroupResponse();
                syncResp.Deserialize(serialized, null, Basics.ApiVersion.Ignored);
                Assert.AreEqual(ErrorCode.NoError, syncResp.ErrorCode);
                Assert.AreEqual(48, syncResp.MemberAssignment.Version);
                Assert.IsNull(syncResp.MemberAssignment.UserData);
                var array = syncResp.MemberAssignment.PartitionAssignments.ToArray();
                Assert.AreEqual(1, array.Length);
                Assert.AreEqual("topic", array[0].TopicName);
                var p = array[0].PartitionsData.ToArray();
                Assert.AreEqual(2, p.Length);
                Assert.AreEqual(1, p[0].Partition);
                Assert.AreEqual(7, p[1].Partition);
            }
        }

        [Test]
        public void TestDeserializeSyncConsumerGroupResponseEmptyAssignment()
        {
            using (var serialized = new ReusableMemoryStream(null))
            {
                BigEndianConverter.Write(serialized, (short)ErrorCode.NoError);
                BigEndianConverter.Write(serialized, 0);

                serialized.Position = 0;
                var syncResp = new SyncConsumerGroupResponse();
                syncResp.Deserialize(serialized, null, Basics.ApiVersion.Ignored);
                Assert.AreEqual(ErrorCode.NoError, syncResp.ErrorCode);
                Assert.IsEmpty(syncResp.MemberAssignment.PartitionAssignments);
            }
        }

        [Test]
        public void TestSerializeLeaveGroupRequest()
        {
            var leave = new LeaveGroupRequest { GroupId = "group", MemberId = "member" };
            using (var serialized = leave.Serialize(new ReusableMemoryStream(null), 61, ClientId, null, Basics.ApiVersion.Ignored))
            {
                CheckHeader(Basics.ApiKey.LeaveGroupRequest, 0, 61, TheClientId, serialized);
                Assert.AreEqual("group", Basics.DeserializeString(serialized));
                Assert.AreEqual("member", Basics.DeserializeString(serialized));
            }
        }

        [Test]
        public void TestSerializeHeartbeatGroupRequest()
        {
            var heartbeat = new HeartbeatRequest() { GroupId = "group", MemberId = "member", GenerationId = 32 };
            using (var serialized = heartbeat.Serialize(new ReusableMemoryStream(null), 61, ClientId, null, Basics.ApiVersion.Ignored))
            {
                CheckHeader(Basics.ApiKey.HeartbeatRequest, 0, 61, TheClientId, serialized);
                Assert.AreEqual("group", Basics.DeserializeString(serialized));
                Assert.AreEqual(32, BigEndianConverter.ReadInt32(serialized));
                Assert.AreEqual("member", Basics.DeserializeString(serialized));
            }
        }

        [Test]
        public void TestDeserializeSimpleResponse()
        {
            using (var serialized = new ReusableMemoryStream(null))
            {
                BigEndianConverter.Write(serialized, (short) ErrorCode.GroupAuthorizationFailed);

                serialized.Position = 0;
                var resp = new SimpleResponse();
                resp.Deserialize(serialized, null, Basics.ApiVersion.Ignored);
                Assert.AreEqual(ErrorCode.GroupAuthorizationFailed, resp.ErrorCode);
            }
        }

        [Test]
        public void TestSerializeCoordinatorRequest()
        {
            var coord = new GroupCoordinatorRequest() { GroupId = "group" };
            using (var serialized = coord.Serialize(new ReusableMemoryStream(null), 61, ClientId, null, Basics.ApiVersion.Ignored))
            {
                CheckHeader(Basics.ApiKey.GroupCoordinatorRequest, 0, 61, TheClientId, serialized);
                Assert.AreEqual("group", Basics.DeserializeString(serialized));
            }
        }

        [Test]
        public void TestDeserializeCoordinatoreResponse()
        {
            using (var serialized = new ReusableMemoryStream(null))
            {
                BigEndianConverter.Write(serialized, (short) ErrorCode.NoError);
                BigEndianConverter.Write(serialized, 42);
                Basics.SerializeString(serialized, "host");
                BigEndianConverter.Write(serialized, 9092);

                serialized.Position = 0;
                var resp = new GroupCoordinatorResponse();
                resp.Deserialize(serialized, null, Basics.ApiVersion.Ignored);
                Assert.AreEqual(ErrorCode.NoError, resp.ErrorCode);
                Assert.AreEqual(42, resp.CoordinatorId);
                Assert.AreEqual(9092, resp.CoordinatorPort);
                Assert.AreEqual("host", resp.CoordinatorHost);
            }
        }

        [Test]
        public void TestSerializeOffsetFetchRequest()
        {
            var rq = new OffsetFetchRequest
            {
                ConsumerGroupId = "the group",
                TopicsData =
                    new[]
                    {
                        new TopicData<PartitionAssignment>
                        {
                            TopicName = "the topic",
                            PartitionsData = new[] { new PartitionAssignment { Partition = 28 } }
                        }
                    }
            };

            using (var serialized = rq.Serialize(new ReusableMemoryStream(null), 61, ClientId, null, Basics.ApiVersion.Ignored))
            {
                CheckHeader(Basics.ApiKey.OffsetFetchRequest, 1, 61, TheClientId, serialized);
                Assert.AreEqual("the group", Basics.DeserializeString(serialized));
                Assert.AreEqual(1, BigEndianConverter.ReadInt32(serialized));
                Assert.AreEqual("the topic", Basics.DeserializeString(serialized));
                Assert.AreEqual(1, BigEndianConverter.ReadInt32(serialized));
                Assert.AreEqual(28, BigEndianConverter.ReadInt32(serialized));
            }
        }

        [Test]
        public void TestDeserializeFetchOffsetResponse()
        {
            using (var serialized = new ReusableMemoryStream(null))
            {
                BigEndianConverter.Write(serialized, 1);
                Basics.SerializeString(serialized, "topic");
                BigEndianConverter.Write(serialized, 1);
                BigEndianConverter.Write(serialized, 14);
                BigEndianConverter.Write(serialized, 127L);
                Basics.SerializeString(serialized, "metadata");
                BigEndianConverter.Write(serialized, (short) ErrorCode.NoError);

                serialized.Position = 0;
                var resp = new CommonResponse<PartitionOffsetData>();
                resp.Deserialize(serialized, null, Basics.ApiVersion.Ignored);

                Assert.AreEqual(1, resp.TopicsResponse.Length);
                Assert.AreEqual("topic", resp.TopicsResponse[0].TopicName);
                var p = resp.TopicsResponse[0].PartitionsData.ToArray();
                Assert.AreEqual(1, p.Length);

                Assert.AreEqual(ErrorCode.NoError, p[0].ErrorCode);
                Assert.AreEqual(14, p[0].Partition);
                Assert.AreEqual(127L, p[0].Offset);
                Assert.AreEqual("metadata", p[0].Metadata);
            }
        }

        [Test]
        public void TestSerializeOffsetCommitRequest()
        {
            var rq = new OffsetCommitRequest()
            {
                ConsumerGroupId = "the group",
                ConsumerGroupGenerationId = 42,
                ConsumerId = "consumer",
                RetentionTime = 1000L,
                TopicsData = new[]
                {
                    new TopicData<OffsetCommitPartitionData>
                    {
                        TopicName = "topic",
                        PartitionsData = new []
                        {
                            new OffsetCommitPartitionData
                            {
                                Metadata = "metadata",
                                Offset = 20000L,
                                Partition = 23
                            }
                        }
                    }
                }
            };

            using (var serialized = rq.Serialize(new ReusableMemoryStream(null), 61, ClientId, null, Basics.ApiVersion.Ignored))
            {
                CheckHeader(Basics.ApiKey.OffsetCommitRequest, 2, 61, TheClientId, serialized);
                Assert.AreEqual("the group", Basics.DeserializeString(serialized));
                Assert.AreEqual(42, BigEndianConverter.ReadInt32(serialized));
                Assert.AreEqual("consumer", Basics.DeserializeString(serialized));
                Assert.AreEqual(1000L, BigEndianConverter.ReadInt64(serialized));
                Assert.AreEqual(1, BigEndianConverter.ReadInt32(serialized));
                Assert.AreEqual("topic", Basics.DeserializeString(serialized));
                Assert.AreEqual(1, BigEndianConverter.ReadInt32(serialized));
                Assert.AreEqual(23, BigEndianConverter.ReadInt32(serialized));
                Assert.AreEqual(20000L, BigEndianConverter.ReadInt64(serialized));
                Assert.AreEqual("metadata", Basics.DeserializeString(serialized));
            }
        }

        [Test]
        public void TestDeserializeCommitOffsetResponse()
        {
            using (var serialized = new ReusableMemoryStream(null))
            {
                BigEndianConverter.Write(serialized, 1);
                Basics.SerializeString(serialized, "topic");
                BigEndianConverter.Write(serialized, 1);
                BigEndianConverter.Write(serialized, 14);
                BigEndianConverter.Write(serialized, (short)ErrorCode.NoError);

                serialized.Position = 0;
                var resp = new CommonResponse<PartitionCommitData>();
                resp.Deserialize(serialized, null, Basics.ApiVersion.Ignored);

                Assert.AreEqual(1, resp.TopicsResponse.Length);
                Assert.AreEqual("topic", resp.TopicsResponse[0].TopicName);
                var p = resp.TopicsResponse[0].PartitionsData.ToArray();
                Assert.AreEqual(1, p.Length);

                Assert.AreEqual(ErrorCode.NoError, p[0].ErrorCode);
                Assert.AreEqual(14, p[0].Partition);
            }
        }

        [Test]
        [TestCase(null)]
        [TestCase(new byte[] {})]
        [TestCase(new byte[] { 0x00 })]
        [TestCase(new byte[] { 0xF0, 0x00 })]
        public void TestSerializeBytesWithVarIntSize(byte[] value)
        {
            using (var stream = new ReusableMemoryStream(null))
            {
                Basics.SerializeBytesWithVarIntSize(stream, value);
                stream.Position = 0;
                Assert.AreEqual(value, Basics.DeserializeBytesWithVarIntSize(stream));
            }
        }

        [Test]
        [TestCase(null)]
        [TestCase("")]
        [TestCase("!")]
        [TestCase("not ascii, aé🤷")]
        public void TestSerializeStringWithVarIntSize(string value)
        {
            using (var stream = new ReusableMemoryStream(null))
            {
                Basics.SerializeStringWithVarIntSize(stream, value);
                stream.Position = 0;
                Assert.AreEqual(value, Basics.DeserializeStringWithVarIntSize(stream));
            }
        }

        [Test]
        public void TestWriteObjectNull()
        {
            using (var stream = new ReusableMemoryStream(null))
            {
                Basics.SerializeStringWithVarIntSize(stream, null);
                stream.Position = 0;
                Assert.IsNull(Basics.DeserializeBytesWithVarIntSize(stream));
            }
        }

        [Test]
        public void TestWriteObjectSerializable()
        {
            var testCases = new object[]
            {
                Value, TheValue, new SimpleSerializable(Value), new SimpleSizedSerializable(Value)
            };
            ;
            foreach (object value in testCases)
            {
                using (var stream = new ReusableMemoryStream(Pool))
                {
                    var serializer = new DummySerializer();
                    Basics.WriteObject(stream, value, null);
                    Basics.WriteObject(stream, value, serializer);

                    stream.Position = 0;
                    CollectionAssert.AreEqual(Value, Basics.DeserializeBytesWithVarIntSize(stream));
                    CollectionAssert.AreEqual(Value, Basics.DeserializeBytesWithVarIntSize(stream));
                    Assert.IsFalse(serializer.Called);
                }
            }
        }

        [Test]
        public void TestWriteObjectNotSerializable()
        {
            // A non serializable object will use the serializer
            using (var stream = new ReusableMemoryStream(Pool))
            {
                var serializer = new DummySerializer();
                Basics.WriteObject(stream, new object(), serializer);
                stream.Position = 0;
                CollectionAssert.AreEqual(Value, Basics.DeserializeBytesWithVarIntSize(stream));
                Assert.IsTrue(serializer.Called);
            }

            // A non serializable object will not need to allocate extra buffer if serializer
            // implements ISizableSerializer
            using (var stream = new ReusableMemoryStream(null))
            {
                var serializer = new DummySizableSerializer();
                Basics.WriteObject(stream, new object(), serializer);
                stream.Position = 0;
                CollectionAssert.AreEqual(Value, Basics.DeserializeBytesWithVarIntSize(stream));
                Assert.IsTrue(serializer.Called);
            }
        }

        [Test]
        [TestCase("")]
        [TestCase("!")]
        [TestCase("not ascii, aé🤷")]
        public void TestSizeofSerializedString(string value)
        {
            using (var stream = new ReusableMemoryStream(null))
            {
                Basics.SerializeString(stream, value);
                // size of string is encoded in a short
                Assert.AreEqual(stream.Length, Basics.SizeOfSerializedString(value) + sizeof(short));
            }
        }


        [Test]
        public void TestSizeOfSerializedObject()
        {
            Assert.AreEqual(Value.Length, Basics.SizeOfSerializedObject(Value, null));
            Assert.AreEqual(Basics.SizeOfSerializedString(TheValue), Basics.SizeOfSerializedObject(TheValue, null));
            Assert.AreEqual(Value.Length, Basics.SizeOfSerializedObject(new SimpleSizedSerializable(Value), null));
            Assert.AreEqual(Value.Length, Basics.SizeOfSerializedObject(new object(), new DummySizableSerializer()));
        }

        private class DummySerializer : ISerializer
        {
            public bool Called = false;

            public int Serialize(object input, MemoryStream toStream)
            {
                Called = true;
                var initPosition = toStream.Position;
                toStream.Write(TestSerialization.Value, 0, TestSerialization.Value.Length);
                return (int)(toStream.Position - initPosition);
            }
        }

        private class DummySizableSerializer : DummySerializer, ISizableSerializer
        {
            public long SerializedSize(object input)
            {
                return Value.Length;
            }
        }
    }
}