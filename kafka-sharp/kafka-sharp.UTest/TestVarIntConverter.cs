using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using Kafka.Common;
using System;

namespace tests_kafka_sharp
{
    [TestFixture]
    internal class TestVarIntConverter
    {
        private static readonly IDictionary<long, byte[]> VarIntNumbers = new Dictionary<long, byte[]>
        {
            [0L]  = new byte[] { 0x00 },
            [1L]  = new byte[] { 0x02 },
            [-1L] = new byte[] { 0x01 },
            [-64] = new byte[] { 0x7f },
            [0x7f] = new byte[] {0xfe, 0x01 },
            [0xff] = new byte[] {0xfe, 0x03 },
            [short.MinValue] = new byte[] { 0xff, 0xff, 0x03 },
            [short.MaxValue] = new byte[] { 0xfe, 0xff, 0x03 },
            [short.MaxValue + 1L] = new byte[] { 0x80, 0x80, 0x04 },
            [int.MinValue] = new byte[] { 0xff, 0xff, 0xff, 0xff, 0x0f },
            [int.MaxValue] = new byte[] { 0xfe, 0xff, 0xff, 0xff, 0x0f },
            [int.MaxValue + 1L] = new byte[] { 0x80, 0x80, 0x80, 0x80, 0x10 },
            [long.MinValue] = new byte[] { 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01 },
            [long.MaxValue] = new byte[] { 0xfe, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01 }
        };

        [Test]
        public void TestSizeOfVarInt()
        {
            var buffer = new byte[255];
            var previousPosition = 0L;
            using (var stream = new MemoryStream(buffer))
            {
                foreach (var value in VarIntNumbers.Keys)
                {
                    VarIntConverter.Write(stream, value);
                    Assert.AreEqual(stream.Position - previousPosition, VarIntConverter.SizeOfVarInt(value));
                    previousPosition = stream.Position;
                }
            }
        }

        [Test]
        [TestCase(new byte[] { 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80 })]
        [TestCase(new byte[] { 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x02 })]
        public void TestReadInt64Overflows(byte[] value)
        {
            Assert.Throws<OverflowException>(() => VarIntConverter.ReadInt64(new MemoryStream(value)));
        }

        [Test]
        public void TestReadInt64()
        {
            RunReadIntTestFor(VarIntConverter.ReadInt64, long.MinValue, long.MaxValue);
        }

        [Test]
        public void TestReadInt32()
        {
            RunReadIntTestFor(VarIntConverter.ReadInt32, int.MinValue, int.MaxValue);
        }

        [Test]
        public void TestReadInt16()
        {
            RunReadIntTestFor(VarIntConverter.ReadInt16, short.MinValue, short.MaxValue);
        }

        [Test]
        public void TestReadByte()
        {
            RunReadIntTestFor(VarIntConverter.ReadByte, byte.MinValue, byte.MaxValue, true);
        }

        [Test]
        public void TestReadBool()
        {
            Assert.IsFalse(VarIntConverter.ReadBool(new MemoryStream(new byte[] { 0x00 })));

            Assert.IsTrue(VarIntConverter.ReadBool(new MemoryStream(new byte[] { 0x01 })));
            Assert.IsTrue(VarIntConverter.ReadBool(new MemoryStream(new byte[] { 0x02 })));
            Assert.IsTrue(VarIntConverter.ReadBool(new MemoryStream(new byte[] { 0x80, 0x01 })));
        }

        private void RunReadIntTestFor<TInteger>(Func<MemoryStream, TInteger> readFunc, TInteger minValue,
            TInteger maxValue) where TInteger : IComparable =>
            RunReadIntTestFor(readFunc, minValue, maxValue, true);

        private void RunReadIntTestFor<TInteger>(Func<MemoryStream, TInteger> readFunc, TInteger minValue, TInteger maxValue, bool unsigned)
            where TInteger: IComparable
        {
            foreach (KeyValuePair<long, byte[]> entry in VarIntNumbers)
            {
                if (unsigned && entry.Key < 0)
                {
                    continue;
                }

                // If the value is out of the range of the tested type, an OverflowException must be thrown.
                if (Convert.ToInt64(minValue).CompareTo(entry.Key) <= 0 &&
                    entry.Key.CompareTo(Convert.ToInt64(maxValue)) <= 0)
                {
                    Assert.AreEqual(entry.Key, readFunc(new MemoryStream(entry.Value)));
                }
                else
                {
                    Assert.Throws<OverflowException>(() => readFunc(new MemoryStream(entry.Value)));
                }
            }
        }

        [Test]
        public void TestWriteInt64()
        {
            foreach (KeyValuePair<long, byte[]> entry in VarIntNumbers)
            {
                // 64 bits fit in ceil(64/7) = 10 bytes
                var actual = new byte[10];
                VarIntConverter.Write(new MemoryStream(actual), entry.Key);
                AssertVarIntAreEqual(entry.Value, actual);
            }
        }

        [Test]
        public void TestWriteInt32()
        {
            foreach (KeyValuePair<long, byte[]> entry in VarIntNumbers)
            {
                if (entry.Key < int.MinValue || entry.Key > int.MaxValue)
                {
                    continue;
                }

                var actual = new byte[5];
                VarIntConverter.Write(new MemoryStream(actual), (int)entry.Key);
                AssertVarIntAreEqual(entry.Value, actual);
            }
        }

        [Test]
        public void TestWriteInt16()
        {
            foreach (KeyValuePair<long, byte[]> entry in VarIntNumbers)
            {
                if (entry.Key < short.MinValue || entry.Key > short.MaxValue)
                {
                    continue;
                }

                var actual = new byte[3];
                VarIntConverter.Write(new MemoryStream(actual), (short)entry.Key);
                AssertVarIntAreEqual(entry.Value, actual);
            }
        }

        [Test]
        public void TestWrite()
        {
            foreach (KeyValuePair<long, byte[]> entry in VarIntNumbers)
            {
                if (entry.Key < byte.MinValue || entry.Key > byte.MaxValue)
                {
                    continue;
                }

                var actual = new byte[2];
                VarIntConverter.Write(new MemoryStream(actual), (byte)entry.Key);
                AssertVarIntAreEqual(entry.Value, actual);
            }
        }

        [Test]
        public void TestWriteBool()
        {
            var actual = new byte[4];
            VarIntConverter.Write(new MemoryStream(actual), false);
            AssertVarIntAreEqual(new byte[] { 0x00 }, actual);

            VarIntConverter.Write(new MemoryStream(actual), true);
            AssertVarIntAreEqual(new byte[] { 0x02 }, actual); /* zigzag(1) = 0x02 */
        }

        [Test]
        public void TestAssertVarIntEqual()
        {
            // This test ensures that our assertVarIntEqual is working as expected.
            AssertVarIntAreEqual(new byte[0], new byte[0]);
            AssertVarIntAreEqual(new byte[] {0x01}, new byte[] {0x01});
            AssertVarIntAreEqual(new byte[] { 0x81, 0x01 }, new byte[] { 0x81, 0x01 });
            AssertVarIntAreEqual(new byte[] { 0x01 }, new byte[] { 0x01, 0x00 });
            AssertVarIntAreEqual(new byte[] { 0x01, 0xAF }, new byte[] { 0x01 });

            Assert.Throws<AssertionException>(() => AssertVarIntAreEqual(
                new byte[] {0x01}, new byte[] {0x02}));
            Assert.Throws<AssertionException>(() => AssertVarIntAreEqual(
                new byte[] { 0x81, 0x03 }, new byte[] { 0x81, 0x01 }));
            Assert.Throws<AssertionException>(() => AssertVarIntAreEqual(
                new byte[] { 0x81, 0x01 }, new byte[] { 0x81, 0x81 }));
            Assert.Throws<AssertionException>(() => AssertVarIntAreEqual(
                new byte[0], new byte[] { 0x02 }));

        }
        /// <summary>
        /// Compare two VarInt values in the given buffers. The buffers can be larger than the VarInt
        /// (ie: trailing bytes in both arrays are allowed).
        ///
        /// Throws a NUnit.Framework.AssertionException if the values don't match.
        /// </summary>
        /// <param name="expected">Expected value</param>
        /// <param name="actual">Actual value</param>
        public void AssertVarIntAreEqual(byte[] expected, byte[] actual)
        {
            if (expected.Length == 0 && actual.Length == 0)
            {
                return;
            }

            var length = Math.Min(expected.Length, actual.Length);
            var areEqual = false;
            var lastByteFound = false;
            int i;

            for(i = 0; i < length; ++i)
            {
                lastByteFound = (expected[i] & 0x80) == 0;
                areEqual = expected[i] == actual[i];

                if(!areEqual || lastByteFound)
                {
                    break;
                }

            }

            if(!areEqual || !lastByteFound)
            {
                throw new AssertionException(
                    $"VarInt are not equal at byte {i}, " +
                    $"expected: [{string.Join(", ", expected)}], " +
                    $"actual: [{string.Join(", ", actual)}]");
            }
        }
    }
}
