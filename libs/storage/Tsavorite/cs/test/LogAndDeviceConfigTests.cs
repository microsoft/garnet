// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.
using System.IO;
using NUnit.Framework;
using Tsavorite.core;

namespace Tsavorite.test
{
    //* NOTE: 
    //* A lot of various usage of Log config and Device config are in TsavoriteLog.cs so the test here
    //* is for areas / parameters not covered by the tests in other areas of the test system
    //* For completeness, setting other parameters too where possible
    //* However, the verification is pretty light. Just makes sure log file created and things be added and read from it 

    [TestFixture]
    internal class LogAndDeviceConfigTests
    {
        private TsavoriteLog log;
        private IDevice device;
        private string path;
        static readonly byte[] entry = new byte[100];

        [SetUp]
        public void Setup()
        {
            path = TestUtils.MethodTestDir + "/";

            // Clean up log files from previous test runs in case they weren't cleaned up
            TestUtils.DeleteDirectory(path, wait: true);

            // Create devices \ log for test
            device = Devices.CreateLogDevice(path + "DeviceConfig", deleteOnClose: true, recoverDevice: true, preallocateFile: true, capacity: 1 << 30);
            log = new TsavoriteLog(new TsavoriteLogSettings { LogDevice = device, PageSizeBits = 80, MemorySizeBits = 20, GetMemory = null, SegmentSizeBits = 80, MutableFraction = 0.2, LogCommitManager = null });
        }

        [TearDown]
        public void TearDown()
        {
            log?.Dispose();
            log = null;
            device?.Dispose();
            device = null;

            // Clean up log files
            TestUtils.DeleteDirectory(path);
        }

        [Test]
        [Category("TsavoriteLog")]
        [Category("Smoke")]
        public void DeviceAndLogConfig()
        {
            int entryLength = 10;

            // Set Default entry data
            for (int i = 0; i < entryLength; i++)
            {
                entry[i] = (byte)i;
                log.Enqueue(entry);
            }

            // Commit to the log
            log.Commit(true);

            // Verify  
            Assert.IsTrue(File.Exists(path + "/log-commits/commit.1.0"));
            Assert.IsTrue(File.Exists(path + "/DeviceConfig.0"));

            // Read the log just to verify can actually read it
            int currentEntry = 0;
            using (var iter = log.Scan(0, 100_000_000))
            {
                while (iter.GetNext(out byte[] result, out _, out _))
                {
                    Assert.AreEqual(currentEntry, result[currentEntry]);
                    currentEntry++;
                }
            }
        }
    }
}