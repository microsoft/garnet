// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.cluster;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;

namespace Garnet.test.cluster
{
    [TestFixture, NonParallelizable]
    public unsafe class ClusterTryAdvanceSpanByteTests
    {
        /// <summary>
        /// Valid SpanByte: 4-byte header declaring 5 bytes, followed by 5 bytes of payload.
        /// TryAdvanceSpanByte should succeed and advance ptr past the entire record.
        /// </summary>
        [Test]
        public void ValidSpanByte_Succeeds()
        {
            var data = new byte[sizeof(int) + 5];
            fixed (byte* pinned = data)
            {
                // Write a valid SpanByte length header
                *(int*)pinned = 5;
                pinned[4] = 0xAA;
                pinned[5] = 0xBB;
                pinned[6] = 0xCC;
                pinned[7] = 0xDD;
                pinned[8] = 0xEE;

                var ptr = pinned;
                var end = pinned + data.Length;
                var success = ClusterSession.TryAdvanceSpanByte(ref ptr, end, out var result);

                ClassicAssert.IsTrue(success);
                ClassicAssert.IsTrue(result == pinned, "result should point to the start of the SpanByte");
                ClassicAssert.IsTrue(ptr == end, "ptr should advance to end");

                // Verify the SpanByte is correctly interpreted
                ref var sb = ref SpanByte.Reinterpret(result);
                ClassicAssert.AreEqual(5, sb.Length);
                ClassicAssert.AreEqual(9, sb.TotalSize);
            }
        }

        /// <summary>
        /// Buffer has less than sizeof(int) bytes — not enough for the length header.
        /// </summary>
        [Test]
        public void InsufficientHeader_Fails()
        {
            var data = new byte[3]; // Less than sizeof(int)
            fixed (byte* pinned = data)
            {
                var ptr = pinned;
                var end = pinned + data.Length;
                var success = ClusterSession.TryAdvanceSpanByte(ref ptr, end, out var result);

                ClassicAssert.IsFalse(success);
                ClassicAssert.IsTrue(result == null, "result should be null on failure");
                ClassicAssert.IsTrue(ptr == pinned, "ptr should not advance on failure");
            }
        }

        /// <summary>
        /// Empty buffer (ptr == end).
        /// </summary>
        [Test]
        public void EmptyBuffer_Fails()
        {
            var data = new byte[1];
            fixed (byte* pinned = data)
            {
                var ptr = pinned;
                var end = pinned; // zero length
                var success = ClusterSession.TryAdvanceSpanByte(ref ptr, end, out var result);

                ClassicAssert.IsFalse(success);
                ClassicAssert.IsTrue(result == null);
                ClassicAssert.IsTrue(ptr == pinned);
            }
        }

        /// <summary>
        /// Header declares more payload bytes than actually present.
        /// </summary>
        [Test]
        public void DeclaredLengthExceedsPayload_Fails()
        {
            // Header says 16384 bytes but only 10 bytes of actual payload
            var data = new byte[sizeof(int) + 10];
            fixed (byte* pinned = data)
            {
                *(int*)pinned = 16384;

                var ptr = pinned;
                var end = pinned + data.Length;
                var success = ClusterSession.TryAdvanceSpanByte(ref ptr, end, out var result);

                ClassicAssert.IsFalse(success);
                ClassicAssert.IsTrue(result == null, "result should be null when declared length exceeds payload");
                ClassicAssert.IsTrue(ptr == pinned, "ptr should not advance on failure");
            }
        }

        /// <summary>
        /// Header declares exactly 0 payload bytes — should succeed (valid empty SpanByte).
        /// </summary>
        [Test]
        public void ZeroLengthPayload_Succeeds()
        {
            var data = new byte[sizeof(int)];
            fixed (byte* pinned = data)
            {
                *(int*)pinned = 0;

                var ptr = pinned;
                var end = pinned + data.Length;
                var success = ClusterSession.TryAdvanceSpanByte(ref ptr, end, out var result);

                ClassicAssert.IsTrue(success);
                ClassicAssert.IsTrue(result == pinned);
                ClassicAssert.IsTrue(ptr == end, "ptr should advance past the 4-byte header");

                ref var sb = ref SpanByte.Reinterpret(result);
                ClassicAssert.AreEqual(0, sb.Length);
            }
        }

        /// <summary>
        /// Two consecutive valid SpanBytes — verifies ptr advances correctly for chained reads.
        /// </summary>
        [Test]
        public void TwoConsecutiveSpanBytes_BothSucceed()
        {
            // First: 3 bytes payload, Second: 2 bytes payload
            var data = new byte[(sizeof(int) + 3) + (sizeof(int) + 2)];
            fixed (byte* pinned = data)
            {
                // First SpanByte at offset 0
                *(int*)pinned = 3;
                pinned[4] = 0x11;
                pinned[5] = 0x22;
                pinned[6] = 0x33;

                // Second SpanByte at offset 7
                *(int*)(pinned + 7) = 2;
                pinned[11] = 0x44;
                pinned[12] = 0x55;

                var ptr = pinned;
                var end = pinned + data.Length;

                var success1 = ClusterSession.TryAdvanceSpanByte(ref ptr, end, out var result1);
                ClassicAssert.IsTrue(success1);
                ClassicAssert.IsTrue(result1 == pinned);
                ClassicAssert.IsTrue(ptr == pinned + 7, "ptr should be at second SpanByte");

                var success2 = ClusterSession.TryAdvanceSpanByte(ref ptr, end, out var result2);
                ClassicAssert.IsTrue(success2);
                ClassicAssert.IsTrue(result2 == pinned + 7);
                ClassicAssert.IsTrue(ptr == end);
            }
        }

        /// <summary>
        /// First SpanByte is valid but second one's declared length exceeds remaining buffer.
        /// </summary>
        [Test]
        public void SecondSpanByteOverflows_Fails()
        {
            // First: 2 bytes payload (valid), Second: header claims 10000 but only 3 bytes left
            var data = new byte[(sizeof(int) + 2) + (sizeof(int) + 3)];
            fixed (byte* pinned = data)
            {
                // Valid first SpanByte
                *(int*)pinned = 2;
                pinned[4] = 0xAA;
                pinned[5] = 0xBB;

                // Second SpanByte claims 10000 bytes but only 3 remain
                *(int*)(pinned + 6) = 10000;

                var ptr = pinned;
                var end = pinned + data.Length;

                var success1 = ClusterSession.TryAdvanceSpanByte(ref ptr, end, out _);
                ClassicAssert.IsTrue(success1, "First read should succeed");

                var ptrBeforeSecond = ptr;
                var success2 = ClusterSession.TryAdvanceSpanByte(ref ptr, end, out var result2);
                ClassicAssert.IsFalse(success2, "Second read should fail — declared length exceeds buffer");
                ClassicAssert.IsTrue(result2 == null);
                ClassicAssert.IsTrue(ptr == ptrBeforeSecond, "ptr should not advance on failure");
            }
        }

        /// <summary>
        /// Payload fits exactly — boundary condition where ptr + TotalSize == end.
        /// </summary>
        [Test]
        public void ExactFit_Succeeds()
        {
            const int payloadLen = 100;
            var data = new byte[sizeof(int) + payloadLen];
            fixed (byte* pinned = data)
            {
                *(int*)pinned = payloadLen;

                var ptr = pinned;
                var end = pinned + data.Length;
                var success = ClusterSession.TryAdvanceSpanByte(ref ptr, end, out var result);

                ClassicAssert.IsTrue(success);
                ClassicAssert.IsTrue(result == pinned);
                ClassicAssert.IsTrue(ptr == end, "ptr should land exactly at end");
            }
        }

        /// <summary>
        /// Payload is one byte short of fitting — boundary condition where ptr + TotalSize > end by 1.
        /// </summary>
        [Test]
        public void OneByteShort_Fails()
        {
            const int payloadLen = 100;
            var data = new byte[sizeof(int) + payloadLen - 1]; // one byte short
            fixed (byte* pinned = data)
            {
                *(int*)pinned = payloadLen;

                var ptr = pinned;
                var end = pinned + data.Length;
                var success = ClusterSession.TryAdvanceSpanByte(ref ptr, end, out var result);

                ClassicAssert.IsFalse(success);
                ClassicAssert.IsTrue(result == null);
                ClassicAssert.IsTrue(ptr == pinned);
            }
        }

        /// <summary>
        /// SpanByte with namespace bit (bit 29) set — MetadataSize becomes 1.
        /// Length after masking should still be correct, and bounds check must account for TotalSize.
        /// This is the code path used by SSTORE for migrated namespace element keys.
        /// </summary>
        [Test]
        public void NamespaceBitSet_ValidPayload_Succeeds()
        {
            const int payloadLen = 10;
            var data = new byte[sizeof(int) + payloadLen];
            fixed (byte* pinned = data)
            {
                // Set namespace bit (bit 29) + payload length
                *(int*)pinned = (1 << 29) | payloadLen;

                var ptr = pinned;
                var end = pinned + data.Length;
                var success = ClusterSession.TryAdvanceSpanByte(ref ptr, end, out var result);

                ClassicAssert.IsTrue(success);
                ref var sb = ref SpanByte.Reinterpret(result);
                ClassicAssert.AreEqual(payloadLen, sb.Length);
                ClassicAssert.AreEqual(1, sb.MetadataSize, "Namespace bit should give MetadataSize=1");
                ClassicAssert.AreEqual(sizeof(int) + payloadLen, sb.TotalSize);
            }
        }

        /// <summary>
        /// SpanByte with extra-metadata bit (bit 30) set — MetadataSize becomes 8.
        /// Validates that header flag bits don't corrupt the Length or TotalSize calculation.
        /// </summary>
        [Test]
        public void ExtraMetadataBitSet_ValidPayload_Succeeds()
        {
            const int payloadLen = 20;
            var data = new byte[sizeof(int) + payloadLen];
            fixed (byte* pinned = data)
            {
                // Set extra-metadata bit (bit 30) + payload length
                *(int*)pinned = (1 << 30) | payloadLen;

                var ptr = pinned;
                var end = pinned + data.Length;
                var success = ClusterSession.TryAdvanceSpanByte(ref ptr, end, out var result);

                ClassicAssert.IsTrue(success);
                ref var sb = ref SpanByte.Reinterpret(result);
                ClassicAssert.AreEqual(payloadLen, sb.Length);
                ClassicAssert.AreEqual(8, sb.MetadataSize, "ExtraMetadata bit should give MetadataSize=8");
            }
        }

        /// <summary>
        /// SpanByte with both namespace and extra-metadata bits set — MetadataSize becomes 9.
        /// </summary>
        [Test]
        public void BothMetadataBitsSet_ValidPayload_Succeeds()
        {
            const int payloadLen = 20;
            var data = new byte[sizeof(int) + payloadLen];
            fixed (byte* pinned = data)
            {
                *(int*)pinned = (1 << 30) | (1 << 29) | payloadLen;

                var ptr = pinned;
                var end = pinned + data.Length;
                var success = ClusterSession.TryAdvanceSpanByte(ref ptr, end, out var result);

                ClassicAssert.IsTrue(success);
                ref var sb = ref SpanByte.Reinterpret(result);
                ClassicAssert.AreEqual(payloadLen, sb.Length);
                ClassicAssert.AreEqual(9, sb.MetadataSize, "Both bits should give MetadataSize=9");
            }
        }

        /// <summary>
        /// Raw int is 0xFFFFFFFF (-1 signed). After HeaderMask stripping, Length = 0x1FFFFFFF (~537M).
        /// Must be rejected because actual buffer is tiny.
        /// </summary>
        [Test]
        public void RawIntAllOnes_Rejected()
        {
            var data = new byte[sizeof(int) + 16];
            fixed (byte* pinned = data)
            {
                *(int*)pinned = unchecked((int)0xFFFFFFFF); // -1

                var ptr = pinned;
                var end = pinned + data.Length;
                var success = ClusterSession.TryAdvanceSpanByte(ref ptr, end, out var result);

                ClassicAssert.IsFalse(success, "Max masked length (537M) should exceed 20-byte buffer");
                ClassicAssert.IsTrue(result == null);
                ClassicAssert.IsTrue(ptr == pinned);
            }
        }

        /// <summary>
        /// Raw int is INT_MAX (0x7FFFFFFF). After masking, Length = 0x1FFFFFFF (~537M).
        /// Must be rejected against a small buffer.
        /// </summary>
        [Test]
        public void RawIntMaxValue_Rejected()
        {
            var data = new byte[sizeof(int) + 16];
            fixed (byte* pinned = data)
            {
                *(int*)pinned = int.MaxValue;

                var ptr = pinned;
                var end = pinned + data.Length;
                var success = ClusterSession.TryAdvanceSpanByte(ref ptr, end, out var result);

                ClassicAssert.IsFalse(success);
                ClassicAssert.IsTrue(result == null);
                ClassicAssert.IsTrue(ptr == pinned);
            }
        }

        /// <summary>
        /// Header fits (4 bytes available) but declares 1 byte of payload with 0 bytes remaining.
        /// Off-by-one boundary: sizeof(int) bytes in buffer, declared length = 1.
        /// </summary>
        [Test]
        public void HeaderFitsButPayloadDoesNot_Fails()
        {
            var data = new byte[sizeof(int)]; // exactly 4 bytes, no room for payload
            fixed (byte* pinned = data)
            {
                *(int*)pinned = 1; // claims 1 byte of payload

                var ptr = pinned;
                var end = pinned + data.Length;
                var success = ClusterSession.TryAdvanceSpanByte(ref ptr, end, out var result);

                ClassicAssert.IsFalse(success);
                ClassicAssert.IsTrue(result == null);
                ClassicAssert.IsTrue(ptr == pinned);
            }
        }
    }
}
