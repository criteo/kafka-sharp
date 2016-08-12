// Copyright (c) Damien Guard.  All rights reserved.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Originally published at http://damieng.com/blog/2006/08/08/calculating_crc32_in_c_and_net

// The original code has been stripped of all non used parts and adapted to our use.

using System;

namespace Kafka.Common
{
    /// <summary>
    /// Implements a 32-bit CRC hash algorithm.
    /// </summary>
    static class Crc32
    {
        public const uint DefaultPolynomial = 0xedb88320u;
        public const uint DefaultSeed = 0xffffffffu;

        static uint[] _defaultTable;

        static Crc32()
        {
            InitializeTable(DefaultPolynomial);
        }

        public static uint Compute(ReusableMemoryStream stream, long start, long size)
        {
            var crc = DefaultSeed;

            var buffer = stream.GetBuffer();
            for (var i = start; i < start + size; ++i)
                crc = (crc >> 8) ^ _defaultTable[buffer[i] ^ crc & 0xff];

            return ~crc;
        }

        static void InitializeTable(uint polynomial)
        {
            var createTable = new uint[256];
            for (var i = 0; i < 256; i++)
            {
                var entry = (uint)i;
                for (var j = 0; j < 8; j++)
                    if ((entry & 1) == 1)
                        entry = (entry >> 1) ^ polynomial;
                    else
                        entry = entry >> 1;
                createTable[i] = entry;
            }

            if (polynomial == DefaultPolynomial)
                _defaultTable = createTable;
        }
    }
}

