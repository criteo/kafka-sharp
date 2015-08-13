// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

namespace Kafka.Public
{
    /// <summary>
    /// A Kafka record, as got from consuming a topic. This is
    /// what is returned by the consumer.
    /// </summary>
    public class KafkaRecord
    {
        /// <summary>
        /// The topic of the record.
        /// </summary>
        public string Topic { get; internal set; }

        /// <summary>
        /// The key part of the message. Will be null if there is
        /// no key (which is often the case).
        /// </summary>
        public byte[] Key { get; internal set; }

        /// <summary>
        /// The value part of the message.
        /// </summary>
        public byte[] Value { get; internal set; }

        /// <summary>
        /// The offset of the message in its partition. You may use this
        /// to save the state of what you have read.
        /// </summary>
        public long Offset { get; internal set; }

        /// <summary>
        /// The partition the message belongs to inside its topic.
        /// </summary>
        public int Partition { get; internal set; }
    }
}
