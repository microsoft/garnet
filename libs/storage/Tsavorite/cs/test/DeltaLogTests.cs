// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using Allure.NUnit;
using Garnet.test;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;

namespace Tsavorite.test
{
    [AllureNUnit]
    [TestFixture]
    internal class DeltaLogStandAloneTests : AllureTestBase
    {
        private TsavoriteLog log;
        private IDevice device;

        [SetUp]
        public void Setup()
        {
            // Clean up log files from previous test runs in case they weren't cleaned up
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
        }

        [TearDown]
        public void TearDown()
        {
            log?.Dispose();
            log = null;
            device?.Dispose();
            device = null;

            // Clean up log files
            TestUtils.OnTearDown(waitForDelete: true);
        }

        [Test]
        [Category("TsavoriteLog")]
        [Category("Smoke")]
        public void DeltaLogTest1([Values] TestUtils.TestDeviceType deviceType)
        {
            const int TotalCount = 200;
            string filename = Path.Join(TestUtils.MethodTestDir, $"delta_{deviceType}.log");
            TestUtils.RecreateDirectory(TestUtils.MethodTestDir);

            device = TestUtils.CreateTestDevice(deviceType, filename);
            device.Initialize(-1);
            using DeltaLog deltaLog = new DeltaLog(device, 12, 0);
            Random r = new(20);
            int i;

            SectorAlignedBufferPool bufferPool = new(1, (int)device.SectorSize);
            deltaLog.InitializeForWrites(bufferPool);
            for (i = 0; i < TotalCount; i++)
            {
                int _len = 1 + r.Next(254);
                long address;
                while (true)
                {
                    deltaLog.Allocate(out int maxLen, out address);
                    if (_len <= maxLen) break;
                    deltaLog.Seal(0);
                }
                for (int j = 0; j < _len; j++)
                {
                    unsafe { *(byte*)(address + j) = (byte)_len; }
                }
                deltaLog.Seal(_len, i % 2 == 0 ? DeltaLogEntryType.DELTA : DeltaLogEntryType.CHECKPOINT_METADATA);
            }
            deltaLog.FlushAsync().Wait();

            deltaLog.InitializeForReads();
            r = new(20);
            for (i = 0; deltaLog.GetNext(out long address, out int len, out var type); i++)
            {
                int _len = 1 + r.Next(254);
                ClassicAssert.AreEqual(i % 2 == 0 ? DeltaLogEntryType.DELTA : DeltaLogEntryType.CHECKPOINT_METADATA, type);
                ClassicAssert.AreEqual(len, _len);
                for (int j = 0; j < len; j++)
                {
                    unsafe { ClassicAssert.AreEqual((byte)_len, *(byte*)(address + j)); }
                }
            }
            ClassicAssert.AreEqual(TotalCount, i, $"i={i} and TotalCount={TotalCount}");
            bufferPool.Free();
        }
    }
}