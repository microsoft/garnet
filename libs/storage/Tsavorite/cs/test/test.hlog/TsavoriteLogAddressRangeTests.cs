// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.IO;
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
        private TsavoriteLog log;
        private IDevice device;

        [SetUp]
        public void Setup()
        {
            // Clean up log files from previous test runs in case they weren't cleaned up
            DeleteDirectory(MethodTestDir, wait: true);
        }

        [TearDown]
        public void TearDown()
        {
            log?.Dispose();
            log = null;
            device?.Dispose();
            device = null;

            // Deletes the test directory and verifies there are no leaked LightEpoch instances
            TestUtils.OnTearDown();
        }

        [Test]
        [Category("TsavoriteLog")]
        public void TsavoriteLogPageIsNotReadCacheBitMaskedTest()
        {
            // Page numbers are int, so the page size must be large enough that an address with bit 47 set still maps to a page
            // within int range: 2^47 >> pageSizeBits must be < 2^31, i.e. pageSizeBits >= 17.
            const int pageSizeBits = 17;

            device = Devices.CreateLogDevice(Path.Join(MethodTestDir, "addr-range.log"), deleteOnClose: true);
            log = new TsavoriteLog(new TsavoriteLogSettings
            {
                LogDevice = device,
                PageSizeBits = pageSizeBits,
                MemorySizeBits = 19,
                SegmentSizeBits = 19,
                LogCommitDir = MethodTestDir,
                TryRecoverLatest = false
            });

            const int lowPage = 5;
            long lowAddr = ((long)lowPage << pageSizeBits) | 0x100;   // an address whose read-cache bit (bit 47) is clear
            long highAddr = (1L << 47) | lowAddr;                     // the same address with the read-cache bit set

            // TsavoriteLog must NOT mask off bit 47: the computed page retains the high bit's contribution.
            var expectedUnmaskedPage = (1 << (47 - pageSizeBits)) | lowPage;
            ClassicAssert.AreEqual(expectedUnmaskedPage, log.AllocatorGetPage(highAddr));

            // The masked (main-store) computation that the SpanByte/Object wrappers delegate to would drop bit 47, collapsing to the low page.
            var maskedPage = LogAddress.GetPageOfAddress(highAddr, pageSizeBits);
            ClassicAssert.AreEqual(lowPage, maskedPage);
            ClassicAssert.AreNotEqual(maskedPage, log.AllocatorGetPage(highAddr));

            // Below bit 47, TsavoriteLog and the masked computation agree.
            ClassicAssert.AreEqual(lowPage, log.AllocatorGetPage(lowAddr));
            ClassicAssert.AreEqual(LogAddress.GetPageOfAddress(lowAddr, pageSizeBits), log.AllocatorGetPage(lowAddr));
        }
    }
}