// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Text;
using System.Threading;
using Garnet.client;
using Garnet.common;
using Garnet.common.Parsing;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test.Resp
{
    /// <summary>
    /// Tests that client reply parsing commits memory in proportion to the bytes received, not to the
    /// element counts a server declares.
    /// </summary>
    [TestFixture]
    unsafe class RespReadResponseUtilsTests : TestBase
    {
        /// <summary>
        /// A memory pool that counts outstanding rentals.
        /// </summary>
        sealed class CountingPool : MemoryPool<byte>
        {
            int rented;
            int outstanding;

            public int Rented => rented;
            public int Outstanding => outstanding;

            public override int MaxBufferSize => Shared.MaxBufferSize;

            public override IMemoryOwner<byte> Rent(int minBufferSize = -1)
            {
                Interlocked.Increment(ref rented);
                Interlocked.Increment(ref outstanding);
                return new Owner(this, Shared.Rent(minBufferSize));
            }

            protected override void Dispose(bool disposing) { }

            sealed class Owner(CountingPool pool, IMemoryOwner<byte> inner) : IMemoryOwner<byte>
            {
                int disposed;

                public Memory<byte> Memory => inner.Memory;

                public void Dispose()
                {
                    if (Interlocked.Exchange(ref disposed, 1) == 0)
                    {
                        Interlocked.Decrement(ref pool.outstanding);
                        inner.Dispose();
                    }
                }
            }
        }

        static bool ReadStrings(string reply, out string[] result, out int consumed)
        {
            var bytes = Encoding.ASCII.GetBytes(reply);
            fixed (byte* start = bytes)
            {
                var ptr = start;
                var success = RespReadResponseUtils.TryReadStringArrayWithLengthHeader(out result, ref ptr, start + bytes.Length);
                consumed = (int)(ptr - start);
                return success;
            }
        }

        static bool ReadPooled(MemoryPool<byte> pool, string reply, out MemoryResult<byte>[] result, out int consumed)
        {
            var bytes = Encoding.ASCII.GetBytes(reply);
            fixed (byte* start = bytes)
            {
                var ptr = start;
                var success = RespReadResponseUtils.TryReadStringArrayWithLengthHeader(pool, out result, ref ptr, start + bytes.Length);
                consumed = (int)(ptr - start);
                return success;
            }
        }

        static string Nested(int depth, string innermost)
        {
            var sb = new StringBuilder();
            for (var i = 1; i < depth; i++)
                sb.Append("*1\r\n");
            return sb.Append(innermost).ToString();
        }

        [Test]
        public void ArrayRepliesStillParse()
        {
            ClassicAssert.IsTrue(ReadStrings("*4\r\n$1\r\na\r\n+OK\r\n:42\r\n$-1\r\n", out var result, out _));
            CollectionAssert.AreEqual(new[] { "a", "OK", "42", null }, result);

            ClassicAssert.IsTrue(ReadStrings("*2\r\n*2\r\n$1\r\nx\r\n:1\r\n+y\r\n", out result, out _));
            CollectionAssert.AreEqual(new[] { "x, 1", "y" }, result);

            ClassicAssert.IsTrue(ReadStrings("*0\r\n", out result, out _));
            ClassicAssert.AreEqual(0, result.Length);

            ClassicAssert.IsTrue(ReadStrings("*-1\r\n", out result, out _));
            ClassicAssert.IsNull(result);
        }

        [Test]
        public void ANestedNullArrayReadsAsNull()
        {
            ClassicAssert.IsTrue(ReadStrings("*2\r\n*-1\r\n+a\r\n", out var result, out _));
            CollectionAssert.AreEqual(new[] { null, "a" }, result);
        }

        [Test]
        public void EveryTruncationOfAReplyWaitsAndConsumesNothing()
        {
            const string reply = "*3\r\n$3\r\nabc\r\n*2\r\n+x\r\n:7\r\n:9\r\n";
            for (var length = 1; length < reply.Length; length++)
            {
                ClassicAssert.IsFalse(ReadStrings(reply[..length], out var result, out var consumed), $"prefix {length}");
                ClassicAssert.IsNull(result);
                ClassicAssert.AreEqual(0, consumed);
            }

            ClassicAssert.IsTrue(ReadStrings(reply, out var full, out var fullConsumed));
            CollectionAssert.AreEqual(new[] { "abc", "x, 7", "9" }, full);
            ClassicAssert.AreEqual(reply.Length, fullConsumed);
        }

        [TestCase("*268435456\r\n")]
        [TestCase("*1000000\r\n$1\r\na\r\n")]
        public void ADeclaredCountDoesNotAllocateBeforeItsElementsArrive(string header)
        {
            // Warm up so that the measurement covers only the parse itself
            _ = ReadStrings("*1\r\n+a\r\n", out _, out _);
            _ = ReadPooled(MemoryPool<byte>.Shared, "*1\r\n+a\r\n", out var warm, out _);
            warm[0].Dispose();

            var before = GC.GetAllocatedBytesForCurrentThread();
            var bytes = Encoding.ASCII.GetBytes(header);
            var stringsReady = true;
            var pooledReady = true;
            fixed (byte* start = bytes)
            {
                var ptr = start;
                stringsReady = RespReadResponseUtils.TryReadStringArrayWithLengthHeader(out _, ref ptr, start + bytes.Length);
                ptr = start;
                pooledReady = RespReadResponseUtils.TryReadStringArrayWithLengthHeader(MemoryPool<byte>.Shared, out _, ref ptr, start + bytes.Length);
            }
            var allocated = GC.GetAllocatedBytesForCurrentThread() - before;

            ClassicAssert.IsFalse(stringsReady);
            ClassicAssert.IsFalse(pooledReady);
            ClassicAssert.Less(allocated, 4096, "Parsing an incomplete array allocated in proportion to its declared count");
        }

        [TestCase("*2147483647\r\n")]
        [TestCase("*2147483591\r\n")]
        [TestCase("*715827883\r\n")]
        public void ACountNoBufferCanHoldIsAProtocolError(string header)
        {
            _ = Assert.Throws<RespParsingException>(() => ReadStrings(header, out _, out _));
            _ = Assert.Throws<RespParsingException>(() => ReadPooled(MemoryPool<byte>.Shared, header, out _, out _));
        }

        [Test]
        public void NestingIsLimited()
        {
            ClassicAssert.IsTrue(ReadStrings(Nested(RespReadUtils.MaxNestingDepth, "*1\r\n+a\r\n"), out var result, out _));
            CollectionAssert.AreEqual(new[] { "a" }, result);

            _ = Assert.Throws<RespParsingException>(() =>
                ReadStrings(Nested(RespReadUtils.MaxNestingDepth + 1, "*1\r\n+a\r\n"), out _, out _));

            // Rejected from the headers alone, before the rest of a deep reply arrives
            _ = Assert.Throws<RespParsingException>(() => ReadStrings(Nested(100_000, "*1\r\n"), out _, out _));
        }

        [Test]
        public void APooledArrayReturnsItsRentalsUntilItIsComplete()
        {
            var pool = new CountingPool();
            const string reply = "*3\r\n$3\r\nabc\r\n+de\r\n:12\r\n";

            for (var length = 1; length < reply.Length; length++)
            {
                ClassicAssert.IsFalse(ReadPooled(pool, reply[..length], out var partial, out var consumed), $"prefix {length}");
                ClassicAssert.IsNull(partial);
                ClassicAssert.AreEqual(0, consumed);
                ClassicAssert.AreEqual(0, pool.Outstanding, $"prefix {length}");
            }

            ClassicAssert.IsTrue(ReadPooled(pool, reply, out var result, out var fullConsumed));
            ClassicAssert.AreEqual(reply.Length, fullConsumed);
            ClassicAssert.AreEqual(3, result.Length);
            ClassicAssert.AreEqual("abc", Encoding.ASCII.GetString(result[0].Span));
            ClassicAssert.AreEqual("de", Encoding.ASCII.GetString(result[1].Span));
            ClassicAssert.AreEqual("12", Encoding.ASCII.GetString(result[2].Span));
            foreach (var element in result)
                element.Dispose();
            ClassicAssert.AreEqual(0, pool.Outstanding);
        }

        [Test]
        public void APooledArrayRejectsNestedArraysAndReturnsItsRentals()
        {
            var pool = new CountingPool();
            _ = Assert.Throws<RespParsingException>(() => ReadPooled(pool, "*2\r\n+a\r\n*1\r\n+b\r\n", out _, out _));
            ClassicAssert.AreEqual(0, pool.Outstanding);
        }

        [Test]
        public void ABulkStringLongerThanTheLimitIsAProtocolError()
        {
            _ = Assert.Throws<RespParsingException>(() => ReadStrings("*1\r\n$536870913\r\n", out _, out _));

            var bytes = Encoding.ASCII.GetBytes("$536870913\r\n");
            _ = Assert.Throws<RespParsingException>(() =>
            {
                fixed (byte* start = bytes)
                {
                    var ptr = start;
                    _ = RespReadResponseUtils.TryReadStringWithLengthHeader(out _, ref ptr, start + bytes.Length);
                }
            });
        }
    }
}