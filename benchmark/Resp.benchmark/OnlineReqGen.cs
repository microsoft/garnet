// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;
using System.Text;

namespace Resp.benchmark
{
    /// <summary>
    /// Online req generator
    /// </summary>
    public unsafe partial class OnlineReqGen
    {
        static readonly byte[] hexchars = Encoding.ASCII.GetBytes("0123456789abcdef");

        readonly bool randomGen;
        readonly bool zipf;

        /// <summary>
        /// DbSize, ObjectDbSize, NumBuffs
        /// </summary>
        public readonly int DbSize, NumBuffs;

        readonly byte[] ascii_chars = Encoding.ASCII.GetBytes("abcdefghijklmnopqrstvuwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789");
        readonly byte[] number_chars = Encoding.ASCII.GetBytes("0123456789");
        readonly byte[] valueBuffer;
        readonly byte[] keyBuffer;
        readonly byte* keyBufferPtr;

        Random keyRandomGen;
        Random valueRandomGen;
        ZipfGenerator zipfg;
        int keyIndex;

        public readonly int keyLen, valueLen;

        public OnlineReqGen(int thread_id, int DbSize, bool randomGen = true, bool zipf = false, int keyLen = default, int valueLen = default)
        {
            this.randomGen = randomGen;
            this.DbSize = DbSize;
            this.zipf = zipf;

            this.keyLen = Math.Max(NumUtils.NumDigits(DbSize), keyLen);
            this.valueLen = valueLen == default ? 8 : valueLen;
            valueBuffer = new byte[this.valueLen];
            keyBuffer = GC.AllocateArray<byte>(this.keyLen, true);
            keyBufferPtr = (byte*)Unsafe.AsPointer(ref keyBuffer[0]);

            InitializeRNG(31337 + thread_id, 41337 + thread_id);
        }

        private void InitializeRNG(int keySeed = -1, int valueSeed = -1)
        {
            if (zipf)
                zipfg = new ZipfGenerator(new RandomGenerator(), DbSize, 0.99);
            keyRandomGen = keySeed == -1 ? new Random(Guid.NewGuid().GetHashCode()) : new Random(keySeed);
            valueRandomGen = valueSeed == -1 ? new Random(Guid.NewGuid().GetHashCode()) : new Random(valueSeed);
        }

        /// <summary>
        /// Generate requests
        /// </summary>
        public string GenerateKeyRandom()
        {
            GenerateKeyBytesRandom();
            return Encoding.UTF8.GetString(keyBuffer);
        }

        /// <summary>
        /// Generate requests
        /// </summary>
        public string GenerateKey()
        {
            GenerateKeyBytes();
            return Encoding.UTF8.GetString(keyBuffer);
        }

        /// <summary>
        /// Generate requests
        /// </summary>
        public string GenerateObjectKeyRandom()
        {
            GenerateObjectKeyBytesRandom();
            return Encoding.UTF8.GetString(keyBuffer);
        }

        /// <summary>
        /// Create key with exact index
        /// </summary>
        public string GenerateExactKey(long key)
        {
            int integerLen = NumUtils.NumDigitsInLong(key);
            byte* curr = keyBufferPtr;
            while (curr < keyBufferPtr + keyLen - integerLen)
                *curr++ = (byte)'X';
            NumUtils.LongToBytes(key, integerLen, ref curr);
            return Encoding.UTF8.GetString(keyBuffer);
        }

        /// <summary>
        /// Generate requests
        /// </summary>
        public Memory<byte> GenerateKeyBytesRandom()
        {
            uint key = (uint)(randomGen ? (zipf ? zipfg.Next() : keyRandomGen.Next(DbSize)) : (keyIndex++ % DbSize));
            key *= 20323;
            keyBuffer[0] = (byte)'S';   // Uniquifier to avoid collisions with object keys.
            for (int i = 1; i < keyLen; i++)
            {
                keyBuffer[i] = ascii_chars[key % ascii_chars.Length];
                key *= 3;
            }
            return keyBuffer;
        }

        /// <summary>
        /// Generate requests
        /// </summary>
        public Memory<byte> GenerateKeyBytes()
        {
            int key = randomGen ? (zipf ? zipfg.Next() : keyRandomGen.Next(DbSize)) : (keyIndex++ % DbSize);
            int integerLen = NumUtils.NumDigitsInLong(key);
            byte* curr = keyBufferPtr;
            while (curr < keyBufferPtr + keyLen - integerLen)
                *curr++ = (byte)'X';
            NumUtils.LongToBytes(key, integerLen, ref curr);
            return keyBuffer;
        }

        /// <summary>
        /// Generate requests
        /// </summary>
        public string GenerateObjectEntry()
        {
            GenerateObjectEntryBytesRandom();
            return Encoding.UTF8.GetString(keyBuffer);
        }

        /// <summary>
        /// Generate requests
        /// </summary>
        public Memory<byte> GenerateObjectEntryBytesRandom()
        {
            uint key = (uint)(randomGen ? (zipf ? zipfg.Next() : keyRandomGen.Next(10)) : (keyIndex++ % 10));
            key *= 20323;
            for (int i = 0; i < keyLen; i++)
            {
                keyBuffer[i] = ascii_chars[key % ascii_chars.Length];
                key *= 3;
            }
            return keyBuffer;
        }

        /// <summary>
        /// Generate score: 6 digits, 10 distint values
        /// </summary>
        public string GenerateObjectEntryScore()
        {
            GenerateObjectEntryScoreBytes();
            return Encoding.UTF8.GetString(keyBuffer, 0, 6);
        }

        /// <summary>
        /// Generate score: 6 digits, 10 distint values
        /// </summary>
        public Memory<byte> GenerateObjectEntryScoreBytes()
        {
            uint key = (uint)(randomGen ? (zipf ? zipfg.Next() : keyRandomGen.Next(10)) : (keyIndex++ % 10));
            key *= 20323;
            for (int i = 0; i < 6; i++)
            {
                keyBuffer[i] = number_chars[key % number_chars.Length];
                key *= 3;
            }
            return keyBuffer.AsMemory().Slice(0, 6);
        }

        /// <summary>
        /// Generate requests
        /// </summary>
        public Memory<byte> GenerateObjectKeyBytesRandom()
        {
            uint key = (uint)(randomGen ? (zipf ? zipfg.Next() : keyRandomGen.Next(DbSize)) : (keyIndex++ % DbSize));
            keyBuffer[0] = (byte)'O';   // Uniquifier to avoid collisions with string keys.
            for (int i = 1; i < keyLen; i++)
            {
                keyBuffer[i] = ascii_chars[key % ascii_chars.Length];
                key *= 3;
            }
            return keyBuffer;
        }

        public string GenerateValue()
        {
            int valueLength = 1 + valueRandomGen.Next(valueBuffer.Length);
            for (int i = 0; i < valueLength; i++)
                valueBuffer[i] = ascii_chars[valueRandomGen.Next(ascii_chars.Length)];
            return Encoding.UTF8.GetString(valueBuffer, 0, valueLength);
        }

        public Memory<byte> GenerateValueBytes()
        {
            for (int i = 0; i < valueBuffer.Length; i++)
                valueBuffer[i] = ascii_chars[valueRandomGen.Next(ascii_chars.Length)];
            return valueBuffer;
        }

        private static string PadRandom(int size = 40)
        {
            var NodeId = new byte[size];
            new Random(Guid.NewGuid().GetHashCode()).NextBytes(NodeId);
            for (int i = 0; i < NodeId.Length; i++) NodeId[i] = hexchars[NodeId[i] & 0xf];
            return Encoding.ASCII.GetString(NodeId);
        }

        public string GenerateBitOffset()
        {
            return valueRandomGen.Next(0, (valueLen << 3) - 1).ToString();
        }
    }
}