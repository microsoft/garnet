// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.IO;
using Garnet.test;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test
{
    /// <summary>
    /// Verifies that <see cref="TsavoriteLog"/> uses the full logical-address range when mapping an address to a page, unlike the
    /// main-store allocators. The read-cache indicator is bit 47 (see <see cref="LogAddress"/>); the main-store allocators mask it
    /// off (via <c>LogAddress.GetPageOfAddress</c>), but TsavoriteLog has no read cache and must retain the full address.
    /// </summary>
    [TestFixture]
    internal class TsavoriteLogAddressRangeTests : TestBase
    {
        [Test]
        [Category("TsavoriteLog")]
        public void TsavoriteLogPageIsNotReadCacheBitMaskedTest()
        {
            const int pageSizeBits = 14;

            _ = Directory.CreateDirectory(MethodTestDir);
            using IDevice device = Devices.CreateLogDevice(Path.Join(MethodTestDir, "addr-range.log"), deleteOnClose: true);
            using var log = new TsavoriteLog(new TsavoriteLogSettings
            {
                LogDevice = device,
                PageSizeBits = pageSizeBits,
                MemorySizeBits = 16,
                SegmentSizeBits = 16,
                LogCommitDir = MethodTestDir,
                TryRecoverLatest = false
            });

            const long lowPage = 5;
            long lowAddr = (lowPage << pageSizeBits) | 0x100;   // an address whose read-cache bit (bit 47) is clear
            long highAddr = (1L << 47) | lowAddr;               // the same address with the read-cache bit set

            // TsavoriteLog must NOT mask off bit 47: the computed page retains the high bit, exceeding the old 47-bit (and int) range.
            long expectedUnmaskedPage = highAddr >> pageSizeBits;
            ClassicAssert.AreEqual(expectedUnmaskedPage, log.AllocatorGetPage(highAddr));
            ClassicAssert.Greater(log.AllocatorGetPage(highAddr), (long)int.MaxValue);

            // The masked (main-store) computation that the SpanByte/Object wrappers delegate to would drop bit 47, collapsing to the low page.
            long maskedPage = LogAddress.GetPageOfAddress(highAddr, pageSizeBits);
            ClassicAssert.AreEqual(lowPage, maskedPage);
            ClassicAssert.AreNotEqual(maskedPage, log.AllocatorGetPage(highAddr));

            // Below bit 47, TsavoriteLog and the masked computation agree.
            ClassicAssert.AreEqual(lowPage, log.AllocatorGetPage(lowAddr));
            ClassicAssert.AreEqual(LogAddress.GetPageOfAddress(lowAddr, pageSizeBits), log.AllocatorGetPage(lowAddr));

            DeleteDirectory(MethodTestDir, wait: true);
        }
    }
}