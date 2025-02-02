// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers.Binary;
using System.Diagnostics;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Tsavorite.core
{
    /// <summary>
    /// Empty type
    /// </summary>
    public readonly struct Empty
    {
        /// <summary>
        /// Default
        /// </summary>
        public static readonly Empty Default = default;
    }

    /// <summary>
    /// Tsavorite utility functions
    /// </summary>
    public static class Utility
    {
        /// <summary>
        /// Parse size in string notation into long.
        /// Examples: 4k, 4K, 4KB, 4 KB, 8m, 8MB, 12g, 12 GB, 16t, 16 TB, 32p, 32 PB.
        /// </summary>
        /// <param name="value">String version of number</param>
        /// <returns>The number</returns>
        public static long ParseSize(string value)
        {
            char[] suffix = ['k', 'm', 'g', 't', 'p'];
            long result = 0;
            foreach (char c in value)
            {
                if (char.IsDigit(c))
                {
                    result = result * 10 + (byte)c - '0';
                }
                else
                {
                    for (int i = 0; i < suffix.Length; i++)
                    {
                        if (char.ToLower(c) == suffix[i])
                        {
                            result *= (long)Math.Pow(1024, i + 1);
                            return result;
                        }
                    }
                }
            }
            return result;
        }

        /// <summary>
        /// Num bits in the previous power of 2 for specified number
        /// </summary>
        /// <param name="v"></param>
        /// <param name="logger"></param>
        /// <returns></returns>
        internal static int NumBitsPreviousPowerOf2(long v, ILogger logger = null)
        {
            long adjustedSize = PreviousPowerOf2(v);
            if (v != adjustedSize)
                logger?.LogError("Warning: using lower value {adjustedSize} instead of specified value {specifiedValue}", adjustedSize, v);
            return (int)Math.Log(adjustedSize, 2);
        }

        /// <summary>
        /// Previous power of 2
        /// </summary>
        /// <param name="v"></param>
        /// <returns></returns>
        internal static long PreviousPowerOf2(long v)
        {
            v |= v >> 1;
            v |= v >> 2;
            v |= v >> 4;
            v |= v >> 8;
            v |= v >> 16;
            v |= v >> 32;
            return v - (v >> 1);
        }

        /// <summary>
        /// Pretty print value
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        internal static string PrettySize(long value)
        {
            char[] suffix = ['K', 'M', 'G', 'T', 'P'];
            double v = value;
            int exp = 0;
            while (v - Math.Floor(v) > 0)
            {
                if (exp >= 18)
                    break;
                exp += 3;
                v *= 1024;
                v = Math.Round(v, 12);
            }

            while (Math.Floor(v).ToString().Length > 3)
            {
                if (exp <= -18)
                    break;
                exp -= 3;
                v /= 1024;
                v = Math.Round(v, 12);
            }
            if (exp > 0)
                return v.ToString() + suffix[exp / 3 - 1] + "B";
            else if (exp < 0)
                return v.ToString() + suffix[-exp / 3 - 1] + "B";
            return v.ToString() + "B";
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static bool IsReadCache(long address) => (address & Constants.kReadCacheBitMask) != 0;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static long AbsoluteAddress(long address) => address & ~Constants.kReadCacheBitMask;

        /// <summary>Rounds up value to alignment</summary>
        /// <param name="value">Value to be aligned</param>
        /// <param name="alignment">Align to this</param>
        /// <returns>Aligned value</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int RoundUp(int value, int alignment) => (value + (alignment - 1)) & ~(alignment - 1);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static long RoundUp(long value, int alignment)
        {
            Debug.Assert(IsPowerOfTwo(alignment), "RoundUp alignment must be a power of two");
            return (value + (alignment - 1)) & ~(alignment - 1);
        }

        /// <summary>
        /// Is type blittable
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        internal static bool IsBlittable<T>() => !RuntimeHelpers.IsReferenceOrContainsReferences<T>();

        /// <summary>
        /// Get 64-bit hash code for a long value
        /// </summary>
        /// <param name="input"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static long GetHashCode(long input)
        {
            long local_rand = input;
            long local_rand_hash = 8;

            local_rand_hash = 40343 * local_rand_hash + ((local_rand) & 0xFFFF);
            local_rand_hash = 40343 * local_rand_hash + ((local_rand >> 16) & 0xFFFF);
            local_rand_hash = 40343 * local_rand_hash + ((local_rand >> 32) & 0xFFFF);
            local_rand_hash = 40343 * local_rand_hash + (local_rand >> 48);
            local_rand_hash = 40343 * local_rand_hash;

            return (long)Rotr64((ulong)local_rand_hash, 45);
        }

        /// <summary>
        /// Get 64-bit hash code for a byte array
        /// </summary>
        /// <param name="pbString"></param>
        /// <param name="len"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static unsafe long HashBytes(byte* pbString, int len)
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            ulong RotateRight(ulong operand, int shiftCount)
            {
                shiftCount &= 0x3f;

                return
                    (operand >> shiftCount) |
                    (operand << (64 - shiftCount));
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            ulong ReverseByteOrder(ulong operand)
            {
                return
                    (operand >> 56) |
                    ((operand & 0x00ff000000000000) >> 40) |
                    ((operand & 0x0000ff0000000000) >> 24) |
                    ((operand & 0x000000ff00000000) >> 8) |
                    ((operand & 0x00000000ff000000) << 8) |
                    ((operand & 0x0000000000ff0000) << 24) |
                    ((operand & 0x000000000000ff00) << 40) |
                    (operand << 56);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            ulong Mix(ulong value) =>
                        value ^ (value >> 47);

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            long Hash64Len16(ulong u, ulong v, ulong mul)
            {
                ulong a = (u ^ v) * mul;
                a ^= (a >> 47);

                ulong b = (v ^ a) * mul;
                b ^= (b >> 47);
                b *= mul;

                return (long)b;
            }

            const ulong K0 = 0xc3a5c85c97cb3127;
            const ulong K1 = 0xb492b66fbe98f273;
            const ulong K2 = 0x9ae16a3b2f90404f;

            unchecked
            {
                if (len > 32)
                {
                    ulong mul = K2 + (ulong)len * 2;
                    ulong a = Unsafe.Read<ulong>(pbString) * K2;
                    ulong b = Unsafe.Read<ulong>(pbString + 8);
                    ulong c = Unsafe.Read<ulong>(pbString + len - 24);
                    ulong d = Unsafe.Read<ulong>(pbString + len - 32);
                    ulong e = Unsafe.Read<ulong>(pbString + 16) * K2;
                    ulong f = Unsafe.Read<ulong>(pbString + 24) * 9;
                    ulong g = Unsafe.Read<ulong>(pbString + len - 8);
                    ulong h = Unsafe.Read<ulong>(pbString + len - 16) * mul;

                    ulong u = RotateRight(a + g, 43) + (RotateRight(b, 30) + c) * 9;
                    ulong v = ((a + g) ^ d) + f + 1;
                    ulong w = ReverseByteOrder((u + v) * mul) + h;
                    ulong x = RotateRight(e + f, 42) + c;
                    ulong y = (ReverseByteOrder((v + w) * mul) + g) * mul;
                    ulong z = e + f + c;

                    a = ReverseByteOrder((x + z) * mul + y) + b;
                    b = Mix((z + a) * mul + d + h) * mul;
                    return (long)(b + x);
                }
                else if (len > 16)
                {
                    ulong mul = K2 + (ulong)len * 2;
                    ulong a = Unsafe.Read<ulong>(pbString) * K1;
                    ulong b = Unsafe.Read<ulong>(pbString + 8);
                    ulong c = Unsafe.Read<ulong>(pbString + len - 8) * mul;
                    ulong d = Unsafe.Read<ulong>(pbString + len - 16) * K2;

                    return (long)Hash64Len16(
                        RotateRight(a + b, 43) +
                            RotateRight(c, 30) + d,
                        a + RotateRight(b + K2, 18) + c,
                        mul);
                }
                else if (len >= 8)
                {
                    ulong mul = K2 + (ulong)len * 2;
                    ulong a = Unsafe.Read<ulong>(pbString) + K2;
                    ulong b = Unsafe.Read<ulong>(pbString + len - 8);
                    ulong c = RotateRight(b, 37) * mul + a;
                    ulong d = (RotateRight(a, 25) + b) * mul;

                    return Hash64Len16(c, d, mul);
                }
                else if (len >= 4)
                {
                    ulong mul = K2 + (ulong)len * 2;
                    ulong a = Unsafe.Read<uint>(pbString);
                    return Hash64Len16((ulong)len + (a << 3), Unsafe.Read<uint>(pbString + len - 4), mul);
                }
                else if (len > 0)
                {
                    byte a = pbString[0];
                    byte b = pbString[0 + (len >> 1)];
                    byte c = pbString[len - 1];

                    uint y = (uint)a + ((uint)b << 8);
                    uint z = (uint)len + ((uint)c << 2);

                    return (long)(Mix((ulong)(y * K2 ^ z * K0)) * K2);
                }

                return (long)K2;
            }
        }

        /// <summary>
        /// Compute XOR of all provided bytes
        /// </summary>
        /// <param name="src"></param>
        /// <param name="length"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static unsafe ulong XorBytes(byte* src, int length)
        {
            ulong result = 0;
            byte* curr = src;
            byte* end = src + length;
            while (curr + 4 * sizeof(ulong) <= end)
            {
                result ^= *(ulong*)curr;
                result ^= *(1 + (ulong*)curr);
                result ^= *(2 + (ulong*)curr);
                result ^= *(3 + (ulong*)curr);
                curr += 4 * sizeof(ulong);
            }
            while (curr + sizeof(ulong) <= end)
            {
                result ^= *(ulong*)curr;
                curr += sizeof(ulong);
            }
            while (curr + 1 <= end)
            {
                result ^= *curr;
                curr++;
            }

            return result;
        }

        /// <inheritdoc cref="BitOperations.RotateRight(ulong, int)"/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static ulong Rotr64(ulong x, int n) => BitOperations.RotateRight(x, n);

        /// <inheritdoc cref="BitOperations.IsPow2(ulong)"/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool IsPowerOfTwo(long x) => BitOperations.IsPow2(x);

        /// <inheritdoc cref="BitOperations.Log2(uint)"/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int GetLogBase2(int x) => BitOperations.Log2((uint)x);

        /// <inheritdoc cref="BitOperations.Log2(ulong)"/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int GetLogBase2(ulong value) => BitOperations.Log2(value);

        /// <summary>
        /// Check if power of two
        /// </summary>
        /// <param name="x"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool Is32Bit(long x)
        {
            return ((ulong)x < 4294967295ul);
        }

        /// <summary>
        /// A 32-bit murmur3 implementation.
        /// </summary>
        /// <param name="h"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int Murmur3(int h)
        {
            uint a = (uint)h;
            a ^= a >> 16;
            a *= 0x85ebca6b;
            a ^= a >> 13;
            a *= 0xc2b2ae35;
            a ^= a >> 16;
            return (int)a;
        }

        /// <summary>
        /// Updates the variable to newValue only if the current value is smaller than the new value.
        /// </summary>
        /// <param name="variable">The variable to possibly replace</param>
        /// <param name="newValue">The value that replaces the variable if successful</param>
        /// <param name="oldValue">The orignal value in the variable</param>
        /// <returns> if oldValue less than newValue </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool MonotonicUpdate(ref long variable, long newValue, out long oldValue)
        {
            do
            {
                oldValue = variable;
                if (oldValue >= newValue) return false;
            } while (Interlocked.CompareExchange(ref variable, newValue, oldValue) != oldValue);
            return true;
        }

        /// <summary>
        /// Updates the variable to newValue only if the current value is smaller than the new value.
        /// </summary>
        /// <param name="variable">The variable to possibly replace</param>
        /// <param name="newValue">The value that replaces the variable if successful</param>
        /// <param name="oldValue">The orignal value in the variable</param>
        /// <returns>if oldValue less than or equal to newValue</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool MonotonicUpdate(ref int variable, int newValue, out int oldValue)
        {
            do
            {
                oldValue = variable;
                if (oldValue >= newValue) return false;
            } while (Interlocked.CompareExchange(ref variable, newValue, oldValue) != oldValue);
            return true;
        }

        /// <summary>
        /// Throws OperationCanceledException if token cancels before the real task completes.
        /// Doesn't abort the inner task, but allows the calling code to get "unblocked" and react to stuck tasks.
        /// </summary>
        internal static Task<T> WithCancellationAsync<T>(this Task<T> task, CancellationToken token, bool useSynchronizationContext = false)
        {
            if (!token.CanBeCanceled || task.IsCompleted)
            {
                return task;
            }
            else if (token.IsCancellationRequested)
            {
                return Task.FromCanceled<T>(token);
            }

            return SlowWithCancellationAsync(task, token, useSynchronizationContext);
        }

        private static async Task<T> SlowWithCancellationAsync<T>(Task<T> task, CancellationToken token, bool useSynchronizationContext)
        {
            var tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            using (token.Register(s => ((TaskCompletionSource<bool>)s).TrySetResult(true), tcs, useSynchronizationContext))
            {
                if (task != await Task.WhenAny(task, tcs.Task))
                {
                    token.ThrowIfCancellationRequested();
                }
            }

            // make sure any exceptions in the task get unwrapped and exposed to the caller.
            return await task;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public static ulong GetCurrentMilliseconds()
        {
            Debug.Assert(Stopwatch.IsHighResolution, "Expected Stopwatch.IsHighResolution to be true");
            return (ulong)(((double)Stopwatch.GetTimestamp() / Stopwatch.Frequency) * 1000);
        }

        internal static string GetHashString(long hash)
        {
            // The debugger often can't call the Globalization NegativeSign property so ToString() would just display the class name
            var hashSign = hash < 0 ? "-" : string.Empty;
            var absHash = hash >= 0 ? hash : -hash;
            return $"{hashSign}{absHash}";
        }

        internal static string GetHashString(long? hash) => hash.HasValue ? GetHashString(hash.Value) : "null";
    }
}