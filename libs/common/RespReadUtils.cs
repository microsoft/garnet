// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Text;

namespace Garnet.common
{
    /// <summary>
    /// Utilities for reading RESP protocol
    /// </summary>
    public static unsafe class RespReadUtils
    {
        /// <summary>
        /// Get Header length
        /// </summary>
        /// <param name="len"></param>
        /// <param name="ptr"></param>
        /// <param name="end"></param>
        /// <returns></returns>
        public static bool ReadHeaderLength(out int len, ref byte* ptr, byte* end)
        {
            len = -1;
            if (ptr + 3 >= end)
                return false;

            Debug.Assert(*ptr == '$');
            ptr++;
            bool neg = *ptr == '-';
            int ksize = *ptr++ - '0';
            while (*ptr != '\r')
            {
                Debug.Assert(*ptr >= '0' && *ptr <= '9');
                ksize = ksize * 10 + *ptr++ - '0';
                if (ptr >= end)
                    return false;
            }

            ptr += 2;
            if (ptr > end)
                return false;
            Debug.Assert(*(ptr - 2) == '\r');
            Debug.Assert(*(ptr - 1) == '\n');
            len = neg ? -ksize : ksize;
            return true;
        }

        /// <summary>
        /// Read int
        /// </summary>
        public static bool ReadInt(out int number, ref byte* ptr, byte* end)
        {
            number = 0;
            if (ptr + 3 >= end)
                return false;

            Debug.Assert(*ptr == '$');

            ptr++;
            number = *ptr++ - '0';
            while (*ptr != '\r')
            {
                Debug.Assert(*ptr >= '0' && *ptr <= '9');
                number = number * 10 + *ptr++ - '0';
                if (ptr >= end)
                    return false;
            }
            ptr += 2;
            if (ptr > end)
                return false;

            Debug.Assert(*(ptr - 1) == '\n');
            return true;
        }

        /// <summary>
        /// Read signed 64 bit number
        /// </summary>
        public static bool Read64Int(out long number, ref byte* ptr, byte* end)
        {
            number = 0;
            if (ptr + 3 >= end)
                return false;

            Debug.Assert(*ptr == ':');

            ptr++;
            number = *ptr++ - '0';
            while (*ptr != '\r')
            {
                Debug.Assert(*ptr >= '0' && *ptr <= '9');
                number = number * 10 + *ptr++ - '0';
                if (ptr >= end)
                    return false;
            }
            ptr += 2;
            if (ptr > end)
                return false;

            Debug.Assert(*(ptr - 1) == '\n');
            return true;
        }


        /// <summary>
        /// Read the length of an array of bulk strings
        /// </summary>
        /// <param name="number"></param>
        /// <param name="ptr"></param>
        /// <param name="end"></param>
        /// <returns></returns>
        public static bool ReadArrayLength(out int number, ref byte* ptr, byte* end)
        {
            number = 0;
            if (ptr + 3 >= end)
                return false;

            Debug.Assert(*ptr == '*');

            ptr++;
            number = *ptr++ - '0';
            while (*ptr != '\r')
            {
                Debug.Assert(*ptr >= '0' && *ptr <= '9');
                number = number * 10 + *ptr++ - '0';
                if (ptr >= end)
                    return false;
            }
            ptr += 2;
            if (ptr > end)
                return false;

            Debug.Assert(*(ptr - 1) == '\n');
            return true;
        }

        /// <summary>
        /// Read int with length header
        /// </summary>
        public static bool ReadIntWithLengthHeader(out int number, ref byte* ptr, byte* end)
        {
            number = 0;
            if (!ReadInt(out int numberLength, ref ptr, end))
                return false;

            if (ptr + numberLength + 2 > end)
                return false;

            number = (int)NumUtils.BytesToLong(numberLength, ptr);
            ptr += numberLength + 2;
            Debug.Assert(*(ptr - 2) == '\r');
            Debug.Assert(*(ptr - 1) == '\n');
            return true;
        }

        /// <summary>
        /// Read long with length header
        /// </summary>
        public static bool ReadLongWithLengthHeader(out long number, ref byte* ptr, byte* end)
        {
            number = 0;
            if (!ReadInt(out int numberLength, ref ptr, end))
                return false;

            if (ptr + numberLength + 2 > end)
                return false;

            number = NumUtils.BytesToLong(numberLength, ptr);
            ptr += numberLength + 2;
            Debug.Assert(*(ptr - 2) == '\r');
            Debug.Assert(*(ptr - 1) == '\n');
            return true;
        }

        /// <summary>
        /// Read long with length header
        /// </summary>
        public static bool ReadULongWithLengthHeader(out ulong number, ref byte* ptr, byte* end)
        {
            number = 0;
            if (!ReadInt(out int numberLength, ref ptr, end))
                return false;

            if (ptr + numberLength + 2 > end)
                return false;

            number = NumUtils.BytesToULong(numberLength, ptr);
            ptr += numberLength + 2;
            Debug.Assert(*(ptr - 2) == '\r');
            Debug.Assert(*(ptr - 1) == '\n');
            return true;
        }

        /// <summary>
        /// Read byte array with length header
        /// </summary>
        public static bool ReadByteArrayWithLengthHeader(out byte[] result, ref byte* ptr, byte* end)
        {
            result = null;
            if (ptr + 3 >= end)
                return false;

            Debug.Assert(*ptr == '$');
            ptr++;
            bool neg = *ptr == '-';
            int ksize = *ptr++ - '0';
            while (*ptr != '\r')
            {
                Debug.Assert(*ptr >= '0' && *ptr <= '9');
                ksize = ksize * 10 + *ptr++ - '0';
                if (ptr >= end)
                    return false;
            }

            if (neg)
            {
                ptr += 2;
                if (ptr > end) return false;
                return true;
            }

            var keyPtr = ptr + 2;
            ptr = ptr + 2 + ksize + 2;  // for \r\n + key + \r\n
            if (ptr > end)
                return false;

            Debug.Assert(*(ptr + 1 - (2 + ksize + 2)) == '\n');
            Debug.Assert(*(ptr - 2) == '\r');
            Debug.Assert(*(ptr - 1) == '\n');

            result = new Span<byte>(keyPtr, ksize).ToArray();
            return true;
        }

        /// <summary>
        /// Read string with length header
        /// </summary>
        public static bool ReadBoolWithLengthHeader(out bool result, ref byte* ptr, byte* end)
        {
            //$1\r\n1\r\n
            //$1\r\n0\r\n
            result = false;
            if (ptr + 7 >= end)
                return false;

            Debug.Assert(*ptr == '$');
            Debug.Assert(*(ptr + 1) == '1');
            Debug.Assert(*(ptr + 2) == '\r');
            Debug.Assert(*(ptr + 3) == '\n');
            ptr += 4;
            result = *ptr == '1' ? true : false;
            ptr += 3;
            return true;
        }

        /// <summary>
        /// Read string with length header
        /// </summary>
        public static bool ReadStringWithLengthHeader(out string result, ref byte* ptr, byte* end)
        {
            result = null;
            if (ptr + 3 >= end)
                return false;

            Debug.Assert(*ptr == '$');
            ptr++;
            bool neg = *ptr == '-';
            int ksize = *ptr++ - '0';
            while (*ptr != '\r')
            {
                Debug.Assert(*ptr >= '0' && *ptr <= '9');
                ksize = ksize * 10 + *ptr++ - '0';
                if (ptr >= end)
                    return false;
            }

            if (neg)
            {
                ptr += 2;
                if (ptr > end) return false;
                return true;
            }

            var keyPtr = ptr + 2;
            ptr = ptr + 2 + ksize + 2;  // for \r\n + key + \r\n
            if (ptr > end)
                return false;

            Debug.Assert(*(ptr + 1 - (2 + ksize + 2)) == '\n');
            Debug.Assert(*(ptr - 2) == '\r');
            Debug.Assert(*(ptr - 1) == '\n');

#if NETSTANDARD2_1_OR_GREATER || NET5_0_OR_GREATER
            result = Encoding.UTF8.GetString(new Span<byte>(keyPtr, ksize));
#else
            result = Encoding.UTF8.GetString(new Span<byte>(keyPtr, ksize).ToArray());
#endif
            return true;
        }

        /// <summary>
        /// Read string with length header
        /// </summary>
        public static bool ReadStringWithLengthHeader(MemoryPool<byte> pool, out MemoryResult<byte> result, ref byte* ptr, byte* end)
        {
            result = default;
            if (ptr + 3 >= end)
                return false;

            Debug.Assert(*ptr == '$');
            ptr++;
            bool neg = *ptr == '-';
            int ksize = *ptr++ - '0';
            while (*ptr != '\r')
            {
                Debug.Assert(*ptr >= '0' && *ptr <= '9');
                ksize = ksize * 10 + *ptr++ - '0';
                if (ptr >= end)
                    return false;
            }

            if (neg)
            {
                ptr += 2;
                if (ptr > end) return false;
                return true;
            }

            var keyPtr = ptr + 2;
            ptr = ptr + 2 + ksize + 2;  // for \r\n + key + \r\n
            if (ptr > end)
                return false;

            Debug.Assert(*(ptr + 1 - (2 + ksize + 2)) == '\n');
            Debug.Assert(*(ptr - 2) == '\r');
            Debug.Assert(*(ptr - 1) == '\n');

            result = MemoryResult<byte>.Create(pool, ksize);
            new ReadOnlySpan<byte>(keyPtr, ksize).CopyTo(result.Span);
            return true;
        }

        /// <summary>
        /// Read simple string
        /// </summary>
        public static bool ReadSimpleString(out string result, ref byte* ptr, byte* end)
        {
            result = null;
            if (ptr + 2 >= end)
                return false;

            Debug.Assert(*ptr == '+');
            ptr++;

            return ReadString(out result, ref ptr, end);
        }

        /// <summary>
        /// Read error as string
        /// </summary>
        public static bool ReadErrorAsString(out string result, ref byte* ptr, byte* end)
        {
            result = null;
            if (ptr + 2 >= end)
                return false;

            Debug.Assert(*ptr == '-');
            ptr++;

            return ReadString(out result, ref ptr, end);
        }

        /// <summary>
        /// Read integer as string
        /// </summary>
        public static bool ReadIntegerAsString(out string result, ref byte* ptr, byte* end)
        {
            result = null;
            if (ptr + 2 >= end)
                return false;

            Debug.Assert(*ptr == ':');
            ptr++;

            return ReadString(out result, ref ptr, end);
        }

        /// <summary>
        /// Read simple string
        /// </summary>
        public static bool ReadSimpleString(MemoryPool<byte> pool, out MemoryResult<byte> result, ref byte* ptr, byte* end)
        {
            result = default;
            if (ptr + 2 >= end)
                return false;

            Debug.Assert(*ptr == '+');
            ptr++;

            return ReadString(pool, out result, ref ptr, end);
        }

        /// <summary>
        /// Read error as string
        /// </summary>
        public static bool ReadErrorAsString(MemoryPool<byte> pool, out MemoryResult<byte> result, ref byte* ptr, byte* end)
        {
            result = default;
            if (ptr + 2 >= end)
                return false;

            Debug.Assert(*ptr == '-');
            ptr++;

            return ReadString(pool, out result, ref ptr, end);
        }

        /// <summary>
        /// Read integer as string
        /// </summary>
        public static bool ReadIntegerAsString(MemoryPool<byte> pool, out MemoryResult<byte> result, ref byte* ptr, byte* end)
        {
            result = default;
            if (ptr + 2 >= end)
                return false;

            Debug.Assert(*ptr == ':');
            ptr++;

            return ReadString(pool, out result, ref ptr, end);
        }

        /// <summary>
        /// Read array with length header
        /// </summary>
        public static bool ReadArrayWithLengthHeader(out byte[][] result, ref byte* ptr, byte* end)
        {
            result = null;
            if (ptr + 3 >= end)
                return false;

            Debug.Assert(*ptr == '*');
            ptr++;
            bool neg = *ptr == '-';
            int asize = *ptr++ - '0';
            while (*ptr != '\r')
            {
                Debug.Assert(*ptr >= '0' && *ptr <= '9');
                asize = asize * 10 + *ptr++ - '0';
                if (ptr >= end)
                    return false;
            }
            ptr += 2;  // for \r\n
            if (ptr > end)
                return false;

            if (neg)
                return true;

            result = new byte[asize][];
            for (int z = 0; z < asize; z++)
                if (!ReadByteArrayWithLengthHeader(out result[z], ref ptr, end))
                    return false;

            return true;
        }

        /// <summary>
        /// Read string array with length header
        /// </summary>
        public static bool ReadStringArrayWithLengthHeader(out string[] result, ref byte* ptr, byte* end)
        {
            result = null;
            if (ptr + 3 >= end)
                return false;

            Debug.Assert(*ptr == '*');
            ptr++;
            bool neg = *ptr == '-';
            int asize = *ptr++ - '0';
            while (*ptr != '\r')
            {
                Debug.Assert(*ptr >= '0' && *ptr <= '9');
                asize = asize * 10 + *ptr++ - '0';
                if (ptr >= end)
                    return false;
            }
            ptr += 2;  // for \r\n
            if (ptr > end)
                return false;

            if (neg)
                return true;

            result = new string[asize];
            for (int z = 0; z < asize; z++)
            {
                if (*ptr == '$')
                {
                    if (!ReadStringWithLengthHeader(out result[z], ref ptr, end))
                        return false;
                }
                else
                {
                    if (!ReadIntegerAsString(out result[z], ref ptr, end))
                        return false;
                }
            }

            return true;
        }

        /// <summary>
        /// Read string array with length header
        /// </summary>
        public static bool ReadPtrArrayWithLengthHeader(out List<Tuple<long, long>> result, ref byte* ptr, byte* end)
        {
            result = null;
            if (ptr + 3 >= end)
                return false;

            Debug.Assert(*ptr == '*');
            ptr++;
            bool neg = *ptr == '-';
            int asize = *ptr++ - '0';
            while (*ptr != '\r')
            {
                Debug.Assert(*ptr >= '0' && *ptr <= '9');
                asize = asize * 10 + *ptr++ - '0';
                if (ptr >= end)
                    return false;
            }
            ptr += 2;  // for \r\n
            if (ptr > end)
                return false;

            if (neg)
                return true;

            result = new();

            for (int z = 0; z < asize; z++)
            {
                byte* keyPtr = null;
                int ksize = 0;
                if (!ReadPtrWithLengthHeader(ref keyPtr, ref ksize, ref ptr, end))
                    return false;
                result.Add(new Tuple<long, long>(((IntPtr)keyPtr).ToInt64(), ksize));
            }

            return true;
        }

        /// <summary>
        /// Read string array with length header
        /// </summary>
        public static bool ReadStringArrayWithLengthHeader(MemoryPool<byte> pool, out MemoryResult<byte>[] result, ref byte* ptr, byte* end)
        {
            result = null;
            if (ptr + 3 >= end)
                return false;

            Debug.Assert(*ptr == '*');
            ptr++;
            bool neg = *ptr == '-';
            int asize = *ptr++ - '0';
            while (*ptr != '\r')
            {
                Debug.Assert(*ptr >= '0' && *ptr <= '9');
                asize = asize * 10 + *ptr++ - '0';
                if (ptr >= end)
                    return false;
            }
            ptr += 2;  // for \r\n
            if (ptr > end)
                return false;

            if (neg)
                return true;

            result = new MemoryResult<byte>[asize];
            for (int z = 0; z < asize; z++)
            {
                if (*ptr == '$')
                {
                    if (!ReadStringWithLengthHeader(pool, out result[z], ref ptr, end))
                        return false;
                }
                else
                {
                    if (!ReadIntegerAsString(pool, out result[z], ref ptr, end))
                        return false;
                }
            }

            return true;
        }

        /// <summary>
        /// Read double with length header
        /// </summary>
        public static bool ReadDoubleWithLengthHeader(out double result, out bool parsed, ref byte* ptr, byte* end)
        {
            parsed = false;
            if (!ReadByteArrayWithLengthHeader(out var resultBytes, ref ptr, end))
            {
                result = 0;
                return false;
            }
            parsed = double.TryParse(Encoding.ASCII.GetString(resultBytes), out result);
            return true;
        }

        /// <summary>
        /// Read Span of byte with length header
        /// </summary>
        public static bool ReadSpanByteWithLengthHeader(ref Span<byte> result, ref byte* ptr, byte* end)
        {
            if (ptr + 3 >= end)
                return false;

            Debug.Assert(*ptr == '$');
            ptr++;
            int ksize = *ptr++ - '0';
            while (*ptr != '\r')
            {
                Debug.Assert(*ptr >= '0' && *ptr <= '9');
                ksize = ksize * 10 + *ptr++ - '0';
                if (ptr >= end)
                    return false;
            }
            var keyPtr = ptr + 2;
            ptr = ptr + 2 + ksize + 2;  // for \r\n + key + \r\n
            if (ptr > end)
                return false;

            Debug.Assert(*(ptr + 1 - (2 + ksize + 2)) == '\n');
            Debug.Assert(*(ptr - 2) == '\r');
            Debug.Assert(*(ptr - 1) == '\n');

            result = new Span<byte>(keyPtr, ksize);
            return true;
        }

        /// <summary>
        /// Read pointer to byte array, with length header
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool ReadPtrWithLengthHeader(ref byte* result, ref int len, ref byte* ptr, byte* end)
        {
            if (ptr + 3 >= end) // we need at least 3 characters: [$0\r]
                return false;

            Debug.Assert(*ptr == '$');
            ptr++;
            len = *ptr++ - '0';
            while (*ptr != '\r')
            {
                Debug.Assert(*ptr >= '0' && *ptr <= '9');
                len = len * 10 + *ptr++ - '0';
                if (ptr >= end)
                    return false;
            }
            result = ptr + 2;
            ptr = ptr + 2 + len + 2;  // for \r\n + key + \r\n
            if (ptr > end)
                return false;

            Debug.Assert(*(ptr + 1 - (2 + len + 2)) == '\n');
            Debug.Assert(*(ptr - 2) == '\r');
            Debug.Assert(*(ptr - 1) == '\n');

            return true;
        }

        /// <summary>
        /// Read string
        /// </summary>
        private static bool ReadString(out string result, ref byte* ptr, byte* end)
        {
            result = null;
            if (ptr + 1 >= end)
                return false;

            byte* start = ptr;
            while (ptr < end - 1)
            {
                if (*ptr == (byte)'\r' && *(ptr + 1) == (byte)'\n')
                {
#if NETSTANDARD2_1_OR_GREATER || NET5_0_OR_GREATER
                    result = Encoding.UTF8.GetString(new ReadOnlySpan<byte>(start, (int)(ptr - start)));
#else
                    result = Encoding.UTF8.GetString(new ReadOnlySpan<byte>(start, (int)(ptr - start)).ToArray());
#endif
                    ptr += 2;
                    return true;
                }
                ptr++;
            }

            return false;
        }

        /// <summary>
        /// Read string
        /// </summary>
        private static bool ReadString(MemoryPool<byte> pool, out MemoryResult<byte> result, ref byte* ptr, byte* end)
        {
            result = default;
            if (ptr + 1 >= end)
                return false;

            byte* start = ptr;
            while (ptr < end - 1)
            {
                if (*ptr == (byte)'\r' && *(ptr + 1) == (byte)'\n')
                {
                    result = MemoryResult<byte>.Create(pool, (int)(ptr - start));
                    new ReadOnlySpan<byte>(start, result.Length).CopyTo(result.Span);
                    ptr += 2;
                    return true;
                }
                ptr++;
            }

            return false;
        }

        /// <summary>
        /// Read serialized data for migration
        /// </summary>        
        public static bool ReadSerializedSpanByte(ref byte* keyPtr, ref byte keyMetaDataSize, ref byte* valPtr, ref byte valMetaDataSize, ref byte* ptr, byte* end)
        {
            //1. safe read ksize
            if (ptr + sizeof(int) > end)
                return false;
            int ksize = *(int*)(ptr);
            ptr += sizeof(int);

            //2. safe read key bytes
            if (ptr + ksize + 1 > end)
                return false;
            keyPtr = ptr - sizeof(int);
            ptr += ksize;
            keyMetaDataSize = *ptr++;

            //3. safe read vsize
            if (ptr + 4 > end)
                return false;
            int vsize = *(int*)(ptr);
            ptr += sizeof(int);

            //4. safe read value bytes
            if (ptr + vsize + 1 > end)
                return false;
            valPtr = ptr - sizeof(int);
            ptr += vsize;
            valMetaDataSize = *ptr++;

            return true;
        }

        /// <summary>
        /// Read serialized data for migration
        /// </summary>  
        public static bool ReadSerializedData(out byte[] key, out byte[] value, out long expiration, ref byte* ptr, byte* end)
        {
            expiration = -1;
            key = null;
            value = null;

            //1. safe read ksize
            if (ptr + 4 > end)
                return false;
            var keyLen = *(int*)ptr;
            ptr += 4;

            //2. safe read keyPtr
            if (ptr + keyLen > end)
                return false;
            var keyPtr = ptr;
            ptr += keyLen;

            //3. safe read vsize
            if (ptr + 4 > end)
                return false;
            var valLen = *(int*)ptr;
            ptr += 4;

            //4. safe read valPtr
            if (ptr + valLen > end)
                return false;
            var valPtr = ptr;
            ptr += valLen;

            //5. safe read expiration info
            if (ptr + 8 > end)
                return false;
            expiration = *(long*)(ptr);
            ptr += 8;

            key = new byte[keyLen];
            value = new byte[valLen];
            fixed (byte* kPtr = key)
                Buffer.MemoryCopy(keyPtr, kPtr, keyLen, keyLen);
            fixed (byte* vPtr = value)
                Buffer.MemoryCopy(valPtr, vPtr, valLen, valLen);

            return true;
        }
    }
}