// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

using System;

namespace Kafka.Public
{
    /// <summary>
    /// Partition Id magic values
    /// </summary>
    public static class Partitions
    {
        public const int None = -1;
        public const int All = -2;
        public const int Any = -3;
    }

    /// <summary>
    /// A typed Kafka record. This is a struct because it just encapsulates
    /// a RawKafkaRecord.
    /// </summary>
    /// <typeparam name="TKey">Key type, this must be a reference type</typeparam>
    /// <typeparam name="TValue">Value type, this must be a reference type</typeparam>
    public struct KafkaRecord<TKey, TValue> where TKey : class where TValue : class
    {
        internal RawKafkaRecord Record { get; set; }

        /// <summary>
        /// The topic of the record.
        /// </summary>
        public string Topic
        {
            get { return Record.Topic; }
        }

        /// <summary>
        /// The key part of the message. Will be null if there is
        /// no key (which is often the case).
        /// </summary>
        public TKey Key
        {
            get { return Record.Key as TKey; }
        }

        /// <summary>
        /// The value part of the message.
        /// </summary>
        public TValue Value
        {
            get { return Record.Value as TValue; }
        }

        /// <summary>
        /// The offset of the message in its partition. You may use this
        /// to save the state of what you have read.
        /// </summary>
        public long Offset
        {
            get { return Record.Offset; }
        }

        /// <summary>
        /// The distance to the end of partition offset.
        /// </summary>
        public long Lag { get { return Record.Lag; } }

        /// <summary>
        /// The partition the message belongs to inside its topic.
        /// </summary>
        public int Partition
        {
            get { return Record.Partition; }
        }

        /// <summary>
        /// Timestamp of the message. If using 0.8.2 compatibility mode, this
        /// is always set to Epoch (1970/01/01 00:00:00 UTC).
        /// </summary>
        public DateTime Timestamp { get { return Record.Timestamp; } }
    }

    /// <summary>
    /// A Kafka record, as got from consuming a topic. This is
    /// what is returned by the consumer.
    /// </summary>
    public class RawKafkaRecord
    {
        public RawKafkaRecord() { }

        // For testing purpose
        public RawKafkaRecord(string topic = null, object key = null, object value = null, long offset = 0,
            long lag = 0, int partition = 0, DateTime? timestamp = null)
        {
            Topic = topic;
            Key = key;
            Value = value;
            Offset = offset;
            Lag = lag;
            Partition = partition;
            Timestamp = timestamp.GetValueOrDefault(DateTime.MinValue);
        }

        /// <summary>
        /// The topic of the record.
        /// </summary>
        public string Topic { get; internal set; }

        /// <summary>
        /// The key part of the message. Will be null if there is
        /// no key (which is often the case).
        /// </summary>
        public object Key { get; internal set; }

        /// <summary>
        /// The value part of the message.
        /// </summary>
        public object Value { get; internal set; }

        /// <summary>
        /// The offset of the message in its partition. You may use this
        /// to save the state of what you have read.
        /// </summary>
        public long Offset { get; internal set; }

        /// <summary>
        /// The distance to the end of partition offset.
        /// </summary>
        public long Lag { get; internal set; }

        /// <summary>
        /// The partition the message belongs to inside its topic.
        /// </summary>
        public int Partition { get; internal set; }

        /// <summary>
        /// Timestamp of the message. If using 0.8.2 compatibility mode, this
        /// is always set to Epoch (1970/01/01 00:00:00 UTC).
        /// </summary>
        public DateTime Timestamp { get; internal set; }
    }

    /// <summary>
    /// Header in a record, as got from consuming a topic.
    /// An header is only available for records encoded using the "Message format" V3
    /// or more (starting kafka 0.11).
    /// </summary>
    public struct KafkaRecordHeader
    {
        /// <summary>
        /// Key of the header.
        /// </summary>
        public string Key { get; set; }

        /// <summary>
        /// Raw value of the header.
        /// </summary>
        public byte[] Value { get; set; }

        /// <summary>
        /// Throws an exception if the values of the objects are invalid.
        /// In particular, both the key and value must not be null.
        /// </summary>
        public void Validate()
        {
            if (Key == null)
            {
                throw new ArgumentNullException(nameof(Key));
            }

            if (Value == null)
            {
                throw new ArgumentNullException(nameof(Value));
            }
        }
    }
}
