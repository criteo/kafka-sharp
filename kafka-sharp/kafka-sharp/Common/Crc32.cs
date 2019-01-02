// Copyright (c) Damien Guard.  All rights reserved.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Originally published at http://damieng.com/blog/2006/08/08/calculating_crc32_in_c_and_net

// The original code has been stripped of all non used parts and adapted to our use.


using Kafka.Protocol;

namespace Kafka.Common
{
    /// <summary>
    /// Implements a 32-bit CRC hash algorithm.
    /// </summary>
    internal class Crc32
    {
        public const uint DefaultPolynomial = 0xedb88320u;
        public const uint DefaultSeed = 0xffffffffu;

        public const uint CastagnoliPolynomial = 0x82F63B78u;
        public const uint CastagnoliSeed = DefaultSeed;

        public uint Polynomial { get; private set; }
        public uint Seed { get; private set; }

        private uint[] Table;

        private static readonly Crc32 DefaultCrc32;
        private static readonly Crc32 CastagnoliCrc32;

        /// <summary>
        /// Compute the CRC-32 of the byte sequence using IEEE standard polynomial values.
        /// This is the regular "internet" CRC.
        /// </summary>
        /// <param name="stream">byte stream</param>
        /// <param name="start">start offset of the byte sequence</param>
        /// <param name="size">size of the byte sequence</param>
        /// <returns></returns>
        public static uint Compute(ReusableMemoryStream stream, long start, long size) =>
            DefaultCrc32.ComputeForStream(stream, start, size);

        /// <summary>
        /// Compute the CRC-32 of the byte sequence using Castagnoli polynomial values.
        /// This alternate CRC-32 is often used to compute the CRC as it is often yield
        /// better chances to detect errors in larger payloads.
        ///
        /// In particular, it is used to compute the CRC of a RecordBatch in newer versions
        /// of the Kafka protocol.
        /// </summary>
        /// <param name="stream">byte stream</param>
        /// <param name="start">start offset of the byte sequence</param>
        /// <param name="size">size of the byte sequence</param>
        /// <returns></returns>
        public static uint ComputeCastagnoli(ReusableMemoryStream stream, long start, long size) =>
            CastagnoliCrc32.ComputeForStream(stream, start, size);

        private Crc32(uint polynomial, uint seed)
        {
            Polynomial = polynomial;
            Seed = seed;
            InitializeTable();
        }

        static Crc32()
        {
            DefaultCrc32 = new Crc32(DefaultPolynomial, DefaultSeed);
            CastagnoliCrc32 = new Crc32(CastagnoliPolynomial, CastagnoliSeed);
        }

        private uint ComputeForStream(ReusableMemoryStream stream, long start, long size)
        {
            var crc = Seed;

            var buffer = stream.GetBuffer();
            for (var i = start; i < start + size; ++i)
                crc = (crc >> 8) ^ Table[buffer[i] ^ crc & 0xff];

            return ~crc;
        }

        private void InitializeTable()
        {
            Table = new uint[256];
            for (var i = 0; i < 256; i++)
            {
                var entry = (uint)i;
                for (var j = 0; j < 8; j++)
                    if ((entry & 1) == 1)
                        entry = (entry >> 1) ^ Polynomial;
                    else
                        entry = entry >> 1;
                Table[i] = entry;
            }
        }

        internal static void CheckCrcCastagnoli(int crc, ReusableMemoryStream stream, long crcStartPos, long crcLength = -1)
        {
            CheckCrc(CastagnoliCrc32, crc, stream, crcStartPos, crcLength);
        }

        internal static void CheckCrc(int crc, ReusableMemoryStream stream, long crcStartPos)
        {
            CheckCrc(DefaultCrc32, crc, stream, crcStartPos);
        }

        private static void CheckCrc(Crc32 crcAlgo, int crc, ReusableMemoryStream stream, long crcStartPos, long crcLength = -1)
        {
            var length = crcLength == -1 ? stream.Position - crcStartPos : crcLength;
            var computedCrc = (int)crcAlgo.ComputeForStream(stream, crcStartPos, length);
            if (computedCrc != crc)
            {
                throw new CrcException(
                    string.Format("Corrupt message: CRC32 does not match. Calculated {0} but got {1}", computedCrc,
                        crc));
            }
        }
    }
}

