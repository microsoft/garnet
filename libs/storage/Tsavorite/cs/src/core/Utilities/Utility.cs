// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.ComponentModel;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.IO.Hashing;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
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
            ReadOnlySpan<char> suffix = ['k', 'm', 'g', 't', 'p'];
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
        /// Next power of 2
        /// </summary>
        internal static long NextPowerOf2(long v) => (long)BitOperations.RoundUpToPowerOf2((nuint)v);

        /// <summary>
        /// Pretty print value
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        internal static string PrettySize(long value)
        {
            ReadOnlySpan<char> suffix = ['K', 'M', 'G', 'T', 'P'];
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

        /// <summary>Rounds up <paramref name="value"/> to <paramref name="alignment"/> (which must be a power of two)</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int RoundUp(int value, int alignment)
        {
            Debug.Assert(IsPowerOfTwo(alignment), "RoundUp(int) alignment must be a power of two");
            return (value + (alignment - 1)) & ~(alignment - 1);
        }

        /// <summary>Rounds up <paramref name="value"/> to <paramref name="alignment"/> (which must be a power of two)</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static uint RoundUp(uint value, int alignment)
        {
            Debug.Assert(IsPowerOfTwo(alignment), "RoundUp(uint) alignment must be a power of two");
            return (value + ((uint)alignment - 1)) & ~((uint)alignment - 1);
        }

        /// <summary>Rounds up <paramref name="value"/> to <paramref name="alignment"/> (which must be a power of two)</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static long RoundUp(long value, int alignment)
        {
            Debug.Assert(IsPowerOfTwo(alignment), "RoundUp(long) alignment must be a power of two");
            return (value + (alignment - 1)) & ~(alignment - 1);
        }

        /// <summary>Rounds up <paramref name="value"/> to <paramref name="alignment"/> (which must be a power of two)</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static ulong RoundUp(ulong value, int alignment)
        {
            Debug.Assert(IsPowerOfTwo(alignment), "RoundUp(ulong) alignment must be a power of two");
            return (value + ((uint)alignment - 1)) & ~((uint)alignment - 1);
        }

        /// <summary>Rounds up <paramref name="value"/> to <paramref name="alignment"/> (which must be a power of two)</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static int RoundDown(int value, int alignment)
        {
            Debug.Assert(IsPowerOfTwo(alignment), "RoundDown(int) alignment must be a power of two");
            return value & ~(alignment - 1);
        }

        /// <summary>Rounds up <paramref name="value"/> to <paramref name="alignment"/> (which must be a power of two)</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static uint RoundDown(uint value, int alignment)
        {
            Debug.Assert(IsPowerOfTwo(alignment), "RoundDown(uint) alignment must be a power of two");
            return value & ~((uint)alignment - 1);
        }

        /// <summary>Rounds up <paramref name="value"/> to <paramref name="alignment"/> (which must be a power of two)</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static long RoundDown(long value, int alignment)
        {
            Debug.Assert(IsPowerOfTwo(alignment), "RoundDown(long) alignment must be a power of two");
            return value & ~(alignment - 1);
        }

        /// <summary>Rounds up <paramref name="value"/> to <paramref name="alignment"/> (which must be a power of two)</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static ulong RoundDown(ulong value, int alignment)
        {
            Debug.Assert(IsPowerOfTwo(alignment), "RoundDown(ulong) alignment must be a power of two");
            return value & ~((uint)alignment - 1);
        }

        /// <summary>Verifies that <paramref name="value"/> is aligned to <paramref name="alignment"/> (which must be a power of two)</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static bool IsAligned(long value, int alignment)
        {
            Debug.Assert(IsPowerOfTwo(alignment), "IsAligned(long) alignment must be a power of two");
            return (value & (alignment - 1)) == 0;
        }

        /// <summary>Verifies that <paramref name="value"/> is aligned to <paramref name="alignment"/> (which must be a power of two)</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static bool IsAligned(ulong value, int alignment)
        {
            Debug.Assert(IsPowerOfTwo(alignment), "IsAligned(ulong) alignment must be a power of two");
            return (value & ((uint)alignment - 1)) == 0;
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
        /// Get 64-bit hash code for a byte array. The array does not have to be pinned.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static long HashBytes(ReadOnlySpan<byte> byteSpan)
        {
            unsafe
            {
                fixed (byte* pbString = byteSpan)
                {
                    const long magicno = 40343;
                    char* pwString = (char*)pbString;
                    int len = byteSpan.Length;
                    int cbBuf = len / 2;
                    ulong hashState = (ulong)len;

                    for (int i = 0; i < cbBuf; i++, pwString++)
                        hashState = magicno * hashState + *pwString;

                    if ((len & 1) > 0)
                    {
                        byte* pC = (byte*)pwString;
                        hashState = magicno * hashState + *pC;
                    }

                    return (long)Rotr64(magicno * hashState, 4);
                }
            }
        }

        /// <summary>
        /// Variant 1: Unrolled scalar hash — reads 8 bytes (4 ushorts) at a time via ulong loads.
        /// Produces the SAME hash output as HashBytes. Faster due to fewer memory accesses and reduced loop overhead.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static long HashBytes_Unrolled(ReadOnlySpan<byte> byteSpan)
        {
            unsafe
            {
                fixed (byte* pbString = byteSpan)
                {
                    const ulong magicno = 40343;
                    int len = byteSpan.Length;
                    ulong hashState = (ulong)len;

                    // Process 8 bytes (4 ushorts) at a time using ulong reads
                    ulong* pBlock = (ulong*)pbString;
                    int cbBuf = len / 2;
                    int blockCount = cbBuf / 4;
                    for (int i = 0; i < blockCount; i++, pBlock++)
                    {
                        ulong block = *pBlock;
                        hashState = magicno * hashState + (block & 0xFFFF);
                        hashState = magicno * hashState + ((block >> 16) & 0xFFFF);
                        hashState = magicno * hashState + ((block >> 32) & 0xFFFF);
                        hashState = magicno * hashState + (block >> 48);
                    }

                    // Handle remaining 0-3 ushorts
                    char* pwString = (char*)pBlock;
                    int remaining = cbBuf - blockCount * 4;
                    for (int i = 0; i < remaining; i++, pwString++)
                        hashState = magicno * hashState + *pwString;

                    if ((len & 1) > 0)
                    {
                        byte* pC = (byte*)pwString;
                        hashState = magicno * hashState + *pC;
                    }

                    return (long)Rotr64(magicno * hashState, 4);
                }
            }
        }

        /// <summary>
        /// Variant 2: Multi-lane parallel hash — processes 4 independent hash lanes simultaneously
        /// to break the serial dependency chain. Produces a DIFFERENT hash output than HashBytes.
        /// Faster due to instruction-level parallelism across the 4 independent accumulators.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static long HashBytes_MultiLane(ReadOnlySpan<byte> byteSpan)
        {
            unsafe
            {
                fixed (byte* pbString = byteSpan)
                {
                    const ulong magicno = 40343;
                    const ulong M2 = magicno * magicno;
                    const ulong M3 = M2 * magicno;
                    const ulong M4 = M3 * magicno;

                    char* pwString = (char*)pbString;
                    int len = byteSpan.Length;
                    int cbBuf = len / 2;

                    ulong h0 = (ulong)len;
                    ulong h1 = 0;
                    ulong h2 = 0;
                    ulong h3 = 0;

                    // Process 4 ushorts per iteration across 4 independent lanes
                    int i = 0;
                    int limit = cbBuf - 3;
                    for (; i < limit; i += 4, pwString += 4)
                    {
                        h0 = M4 * h0 + pwString[0];
                        h1 = M4 * h1 + pwString[1];
                        h2 = M4 * h2 + pwString[2];
                        h3 = M4 * h3 + pwString[3];
                    }

                    // Merge lanes: h = h0*M^3 + h1*M^2 + h2*M + h3
                    ulong hashState = M3 * h0 + M2 * h1 + magicno * h2 + h3;

                    // Handle remaining 0-3 ushorts
                    for (; i < cbBuf; i++, pwString++)
                        hashState = magicno * hashState + *pwString;

                    if ((len & 1) > 0)
                    {
                        byte* pC = (byte*)pwString;
                        hashState = magicno * hashState + *pC;
                    }

                    return (long)Rotr64(magicno * hashState, 4);
                }
            }
        }

        /// <summary>
        /// Variant 3: XxHash64-based hash — uses the hardware-accelerated XxHash64 implementation
        /// from System.IO.Hashing. Produces a DIFFERENT hash output than HashBytes.
        /// Fastest for longer keys due to full SIMD vectorization internally.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static long HashBytes_XxHash(ReadOnlySpan<byte> byteSpan)
        {
            return (long)XxHash64.HashToUInt64(byteSpan);
        }

        /// <summary>
        /// Variant 4: Wide-read hash — reads 8 bytes at a time and mixes with minimal multiplies.
        /// Optimized for short keys (≤16 bytes). Produces a DIFFERENT hash output than HashBytes.
        /// Uses 1 multiply per 8 bytes instead of 4, plus a final avalanche mix.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static long HashBytes_WideRead(ReadOnlySpan<byte> byteSpan)
        {
            const ulong prime1 = 0x9E3779B97F4A7C15; // golden ratio * 2^64
            const ulong prime2 = 0xBF58476D1CE4E5B9; // mix constant
            const ulong prime3 = 0x94D049BB133111EB; // mix constant

            unsafe
            {
                fixed (byte* ptr = byteSpan)
                {
                    int len = byteSpan.Length;
                    ulong hash = (ulong)len * prime1;

                    // Process 16-byte chunks (two ulongs)
                    byte* p = ptr;
                    byte* end = ptr + len;
                    while (p + 16 <= end)
                    {
                        ulong a = *(ulong*)p;
                        ulong b = *(ulong*)(p + 8);
                        hash = BitOperations.RotateLeft(hash ^ (a * prime2), 27) * prime1 + prime3;
                        hash = BitOperations.RotateLeft(hash ^ (b * prime2), 27) * prime1 + prime3;
                        p += 16;
                    }

                    // Process remaining 8-byte chunk
                    if (p + 8 <= end)
                    {
                        ulong a = *(ulong*)p;
                        hash = BitOperations.RotateLeft(hash ^ (a * prime2), 27) * prime1 + prime3;
                        p += 8;
                    }

                    // Process remaining 4-byte chunk
                    if (p + 4 <= end)
                    {
                        uint a = *(uint*)p;
                        hash ^= a * prime1;
                        hash = BitOperations.RotateLeft(hash, 23) * prime2 + prime3;
                        p += 4;
                    }

                    // Process remaining bytes
                    while (p < end)
                    {
                        hash ^= *p * prime1;
                        hash = BitOperations.RotateLeft(hash, 11) * prime2;
                        p++;
                    }

                    // Final avalanche
                    hash ^= hash >> 33;
                    hash *= prime2;
                    hash ^= hash >> 29;
                    hash *= prime3;
                    hash ^= hash >> 32;

                    return (long)hash;
                }
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

        /// <inheritdoc cref="BitOperations.Log2(uint)"/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int GetLogBase2(long x) => BitOperations.Log2((ulong)x);

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
                if (oldValue >= newValue)
                    return false;
            } while (Interlocked.CompareExchange(ref variable, newValue, oldValue) != oldValue);
            return true;
        }

        /// <summary>
        /// Updates the variable to newValue only if the current value is smaller than the new value.
        /// </summary>
        /// <param name="variable">The variable to possibly replace</param>
        /// <param name="newValue">The value that replaces the variable if successful</param>
        /// <param name="oldValue">The orignal value in the variable</param>
        /// <returns> if oldValue less than newValue </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool MonotonicUpdate(ref ulong variable, ulong newValue, out ulong oldValue)
        {
            do
            {
                oldValue = variable;
                if (oldValue >= newValue)
                    return false;
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
                if (oldValue >= newValue)
                    return false;
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

        public static string GetCallbackErrorMessage(uint errorCode, uint numBytes, object context)
        {
            string errorMessage;
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                errorMessage = new Win32Exception((int)errorCode).Message;
            }
            else
            {
                // Use strerror for Unix-based systems
                var messagePtr = strerror((int)errorCode);
                errorMessage = Marshal.PtrToStringAnsi(messagePtr);
            }

            return errorMessage;
        }

        [DllImport("libc")]
        private static extern IntPtr strerror(int errnum);

        /// <summary>
        /// Should only be called in Debug.Assert or other DEBUG-conditional code
        /// </summary>
        [MethodImpl(MethodImplOptions.NoInlining)]
        internal static string GetCurrentMethodName([CallerMemberName] string memberName = "") => memberName;

        /// <summary>Throw Tsavorite exception with message. We use a method wrapper so that the caller method can execute inlined.</summary>
        [DoesNotReturn]
        [MethodImpl(MethodImplOptions.NoInlining)]
        internal static void ThrowTsavoriteException(string message) => throw new TsavoriteException(message);
    }
}