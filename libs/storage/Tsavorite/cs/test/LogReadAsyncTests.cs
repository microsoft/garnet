// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Buffers;
using System.IO;
using System.Threading;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;

namespace Tsavorite.test
{
    [TestFixture]
    internal class LogReadAsyncTests
    {
        private TsavoriteLog log;
        private IDevice device;

        public enum ParameterDefaultsIteratorType
        {
            DefaultParams,
            LengthParam,
            TokenParam
        }

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
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
        }

        [Test]
        [Category("TsavoriteLog")]
        [Category("Smoke")]
        public void LogReadAsyncBasicTest([Values] ParameterDefaultsIteratorType iteratorType, [Values] TestUtils.TestDeviceType deviceType)
        {
            int entryLength = 20;
            int numEntries = 500;
            int entryFlag = 9999;
            string filename = Path.Join(TestUtils.MethodTestDir, $"LogReadAsync{deviceType}.log");
            device = TestUtils.CreateTestDevice(deviceType, filename);
            log = new TsavoriteLog(new TsavoriteLogSettings { LogDevice = device, SegmentSizeBits = 22, LogCommitDir = TestUtils.MethodTestDir });

            byte[] entry = new byte[entryLength];

            // Set Default entry data
            for (int i = 0; i < entryLength; i++)
            {
                entry[i] = (byte)i;
            }

            // Enqueue but set each Entry in a way that can differentiate between entries
            for (int i = 0; i < numEntries; i++)
            {
                // Flag one part of entry data that corresponds to index
                if (i < entryLength)
                    entry[i] = (byte)entryFlag;

                // puts back the previous entry value
                if ((i > 0) && (i < entryLength))
                    entry[i - 1] = (byte)(i - 1);

                log.Enqueue(entry);
            }

            // Commit to the log
            log.Commit(true);


            // Read one entry based on different parameters for AsyncReadOnly and verify 
            switch (iteratorType)
            {
                case ParameterDefaultsIteratorType.DefaultParams:
                    // Read one entry and verify
                    var record = log.ReadAsync(log.BeginAddress);
                    var foundFlagged = record.Result.Item1[0];   // 15
                    var foundEntry = record.Result.Item1[1];  // 1
                    var foundTotal = record.Result.Item2;

                    ClassicAssert.AreEqual((byte)entryFlag, foundFlagged, $"Fail reading Flagged Entry");
                    ClassicAssert.AreEqual(1, foundEntry, $"Fail reading Normal Entry");
                    ClassicAssert.AreEqual(entryLength, foundTotal, $"Fail reading Total");

                    break;
                case ParameterDefaultsIteratorType.LengthParam:
                    // Read one entry and verify
                    record = log.ReadAsync(log.BeginAddress, 208);
                    foundFlagged = record.Result.Item1[0];   // 15
                    foundEntry = record.Result.Item1[1];  // 1
                    foundTotal = record.Result.Item2;

                    ClassicAssert.AreEqual((byte)entryFlag, foundFlagged, $"Fail reading Flagged Entry");
                    ClassicAssert.AreEqual(1, foundEntry, $"Fail reading Normal Entry");
                    ClassicAssert.AreEqual(entryLength, foundTotal, $"Fail readingTotal");

                    break;
                case ParameterDefaultsIteratorType.TokenParam:
                    var cts = new CancellationToken();

                    // Read one entry and verify
                    record = log.ReadAsync(log.BeginAddress, 104, cts);
                    foundFlagged = record.Result.Item1[0];   // 15
                    foundEntry = record.Result.Item1[1];  // 1
                    foundTotal = record.Result.Item2;

                    ClassicAssert.AreEqual((byte)entryFlag, foundFlagged, $"Fail readingFlagged Entry");
                    ClassicAssert.AreEqual(1, foundEntry, $"Fail reading Normal Entry");
                    ClassicAssert.AreEqual(entryLength, foundTotal, $"Fail reading Total");

                    // Read one entry as IMemoryOwner and verify
                    var recordMemoryOwner = log.ReadAsync(log.BeginAddress, MemoryPool<byte>.Shared, 104, cts);
                    var foundFlaggedMem = recordMemoryOwner.Result.Item1.Memory.Span[0];   // 15
                    var foundEntryMem = recordMemoryOwner.Result.Item1.Memory.Span[1];  // 1
                    var foundTotalMem = recordMemoryOwner.Result.Item2;

                    ClassicAssert.IsTrue(foundFlagged == foundFlaggedMem, $"MemoryPool-based ReadAsync result does not match that of the byte array one. value: {foundFlaggedMem} expected: {foundFlagged}");
                    ClassicAssert.IsTrue(foundEntry == foundEntryMem, $"MemoryPool-based ReadAsync result does not match that of the byte array one. value: {foundEntryMem} expected: {foundEntry}");
                    ClassicAssert.IsTrue(foundTotal == foundTotalMem, $"MemoryPool-based ReadAsync result does not match that of the byte array one. value: {foundTotalMem} expected: {foundTotal}");

                    break;
                default:
                    Assert.Fail("Unknown case ParameterDefaultsIteratorType.DefaultParams:");
                    break;
            }


        }

    }
}