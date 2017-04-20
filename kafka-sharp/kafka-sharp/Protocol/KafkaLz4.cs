// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

using System.IO;
using Kafka.Common;
using LZ4;

namespace Kafka.Protocol
{
    /// <summary>
    /// Compression/decompression of data following the LZ4 frame format:
    /// https://github.com/lz4/lz4/wiki/lz4_Frame_format.md
    /// 
    /// Kafka uses version 1, no block checksum, no block dependency and 64Kb blocks.
    /// 
    /// </summary>
    static class KafkaLz4
    {
        /// <summary>
        /// Harcoded frame descriptor (configuration is static).
        /// </summary>
        private static readonly byte[] FrameDescriptor =
        {
            0x04, 0x22, 0x4D, 0x18, // magic number
            1 << 6 | 1 << 5, // version 1 + block independance
            1 << 6, // 64Kb blocks
            0x82 // second byte of 32bits xxhash checksum of the two previous bytes (see: https://asecuritysite.com/encryption/xxHash and hash "`@" :) )
        };

        private static readonly int[] MaxBlockSizes = {0, 0, 0, 0, 64*1024, 256*1024, 1024*1024, 4*1024*1024};

        private static readonly int MaxCompressedSize;

        private const int BLOCK_SIZE = 64 * 1024;

        static KafkaLz4()
        {
            MaxCompressedSize = LZ4Codec.MaximumOutputLength(BLOCK_SIZE);
        }

        public static void Compress(ReusableMemoryStream target, byte[] body, int count)
        {
            target.Write(FrameDescriptor, 0, FrameDescriptor.Length);
            
            // Blocks
            var left = count;
            while (left >= BLOCK_SIZE)
            {
                BlockCompress(target, body, count - left, BLOCK_SIZE);
                left -= BLOCK_SIZE;
            }

            // Last block if any
            if (left > 0)
            {
                BlockCompress(target, body, count - left, left);
            }

            // EndMark
            target.Write(Basics.Zero32, 0, Basics.Zero32.Length);
            target.SetLength(target.Position);
        }

        private static void BlockCompress(ReusableMemoryStream target, byte[] body, int offset, int count)
        {
            var position = (int) target.Position;
            target.SetLength(target.Length + MaxCompressedSize + 4);
            target.Position = position + 4;

            var size = LZ4Codec.Encode(body, offset, count, target.GetBuffer(), position + 4, MaxCompressedSize);

            if (size >= count)
            {
                // Do not compress block
                // => set block header highest bit to 1 to mark no compression
                LittleEndianWriteUInt32((uint)(count | 1 << 31), target.GetBuffer(), position);

                // Write uncompressed data
                target.Write(body, offset, count);
            }
            else
            {
                LittleEndianWriteUInt32((uint)size, target.GetBuffer(), position);

                // compressed data is already written, just set the position
                target.Position += size;
            }
            target.SetLength(target.Position);
        }

        public static void Uncompress(ReusableMemoryStream target, byte[] body, int offset)
        {
            // 1. Check magic number
            var magic = LittleEndianReadUInt32(body, offset);
            if (magic != 0x184D2204)
            {
                throw new InvalidDataException("Incorrect LZ4 magic number.");
            }

            // 2. FLG
            var flg = body[offset + 4];
            if (flg >> 6 != 1) // version
            {
                throw new InvalidDataException("Invalid LZ4 version.");
            }

            var hasBlockChecksum = (flg >> 4 & 1) != 0;
            var hasContentSize = (flg >> 3 & 1) != 0;
            var hasContentChecksum = (flg >> 2 & 1) != 0; // we don't care anyway

            // 3. BD
            var bd = body[offset + 5];
            var maxBlockSize = MaxBlockSizes[(bd >> 4) & 7];

            // 4. Let's decompress!
            var dataStartIdx = offset + 4 + (hasContentSize ? 11 : 3);
            uint walked;
            while ((walked = UncompressBlock(target, body, dataStartIdx, hasBlockChecksum, maxBlockSize)) > 0)
            {
                dataStartIdx += (int) walked;
            }
        }

        private static uint UncompressBlock(ReusableMemoryStream target, byte[] body, int dataIndex, bool hasChecksum, int blockSize)
        {
            var blockHeader = LittleEndianReadUInt32(body, dataIndex);
            if (blockHeader == 0) // last frame
            {
                return 0;
            }
            
            var size = blockHeader & 0x7FFFFFFF;
            if ((blockHeader & 0x80000000) == 0) // compressed data
            {
                target.SetLength(target.Length + blockSize);
                var dsize = LZ4Codec.Decode(body, dataIndex + 4, (int) size, target.GetBuffer(), (int) target.Position, blockSize);
                if (dsize < blockSize)
                {
                    target.SetLength(target.Length - blockSize + dsize);
                }
                target.Position = target.Length;
            }
            else // uncompressed data
            {
                target.Write(body, dataIndex + 4, (int) size);
            }

            return size + 4 + (hasChecksum ? 4u : 0);
        }

        private static void LittleEndianWriteUInt32(uint u, byte[] target, int offset)
        {
            target[offset + 0] = (byte) (u >> 8*0 & 0xff);
            target[offset + 1] = (byte) (u >> 8*1 & 0xff);
            target[offset + 2] = (byte) (u >> 8*2 & 0xff);
            target[offset + 3] = (byte) (u >> 8*3 & 0xff);
        }

        private static uint LittleEndianReadUInt32(byte[] source, int offset)
        {
            return
                (uint)
                    (source[offset + 3] << 3*8 | source[offset + 2] << 2*8 | source[offset + 1] << 8 | source[offset]);
        }
    }
}