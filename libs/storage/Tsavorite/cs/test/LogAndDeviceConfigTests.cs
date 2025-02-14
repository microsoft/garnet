// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.
using System.IO;
using NUnit.Framework;
using NUnit.Framework.Legacy;
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
        private TsavoriteAof aof;
        private IDevice device;
        static readonly byte[] entry = new byte[100];

        [SetUp]
        public void Setup()
        {
            // Clean up log files from previous test runs in case they weren't cleaned up
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);

            // Create devices \ log for test
            device = Devices.CreateLogDevice(Path.Join(TestUtils.MethodTestDir, "DeviceConfig"), deleteOnClose: true, recoverDevice: true, preallocateFile: true, capacity: 1L << 30);
            aof = new TsavoriteAof(new TsavoriteAofLogSettings { LogDevice = device, PageSizeBits = 80, MemorySizeBits = 20, GetMemory = null, SegmentSizeBits = 80, MutableFraction = 0.2, LogCommitManager = null });
        }

        [TearDown]
        public void TearDown()
        {
            aof?.Dispose();
            aof = null;
            device?.Dispose();
            device = null;

            // Clean up log files
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
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
                aof.Enqueue(entry);
            }

            // Commit to the log
            aof.Commit(true);

            // Verify  
            ClassicAssert.IsTrue(File.Exists(Path.Join(TestUtils.MethodTestDir, "log-commits", "commit.1.0")));
            ClassicAssert.IsTrue(File.Exists(Path.Join(TestUtils.MethodTestDir, "DeviceConfig.0")));

            // Read the log just to verify can actually read it
            int currentEntry = 0;
            using (var iter = aof.Scan(0, 100_000_000))
            {
                while (iter.GetNext(out byte[] result, out _, out _))
                {
                    ClassicAssert.AreEqual(currentEntry, result[currentEntry]);
                    currentEntry++;
                }
            }
        }
    }
}