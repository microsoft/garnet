// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Allure.NUnit;
using Garnet.test;
using Microsoft.Win32.SafeHandles;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;

namespace Tsavorite.test.recovery.sumstore
{
    using StructAllocator = BlittableAllocator<AdId, NumClicks, StoreFunctions<AdId, NumClicks, AdId.Comparer, DefaultRecordDisposer<AdId, NumClicks>>>;
    using StructStoreFunctions = StoreFunctions<AdId, NumClicks, AdId.Comparer, DefaultRecordDisposer<AdId, NumClicks>>;

    [AllureNUnit]
    [TestFixture]
    internal class SharedDirectoryTests : AllureTestBase
    {
        const long NumUniqueKeys = 1L << 5;
        const long KeySpace = 1L << 11;
        const long NumOps = 1L << 10;
        const long CompletePendingInterval = 1L << 10;
        private string sharedLogDirectory;
        TsavoriteTestInstance original;
        TsavoriteTestInstance clone;

        [SetUp]
        public void Setup()
        {
            TestUtils.RecreateDirectory(TestUtils.MethodTestDir);
            sharedLogDirectory = Path.Join(TestUtils.MethodTestDir, "SharedLogs");
            _ = Directory.CreateDirectory(sharedLogDirectory);

            original = new TsavoriteTestInstance();
            clone = new TsavoriteTestInstance();
        }

        [TearDown]
        public void TearDown()
        {
            original.TearDown();
            clone.TearDown();
            TestUtils.OnTearDown();
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("CheckpointRestore")]
        [Category("Smoke")]
        public async ValueTask SharedLogDirectory([Values] bool isAsync)
        {
            original.Initialize(Path.Join(TestUtils.MethodTestDir, "OriginalCheckpoint"), sharedLogDirectory);
            ClassicAssert.IsTrue(SharedDirectoryTests.IsDirectoryEmpty(sharedLogDirectory)); // sanity check
            SharedDirectoryTests.Populate(original.Store);

            // Take checkpoint from original to start the clone from
            ClassicAssert.IsTrue(original.Store.TryInitiateFullCheckpoint(out var checkpointGuid, CheckpointType.FoldOver));
            original.Store.CompleteCheckpointAsync().GetAwaiter().GetResult();

            // Sanity check against original
            ClassicAssert.IsFalse(SharedDirectoryTests.IsDirectoryEmpty(sharedLogDirectory));
            SharedDirectoryTests.Test(original, checkpointGuid);

            // Copy checkpoint directory
            var cloneCheckpointDirectory = Path.Join(TestUtils.MethodTestDir, "CloneCheckpoint");
            CopyDirectory(new DirectoryInfo(original.CheckpointDirectory), new DirectoryInfo(cloneCheckpointDirectory));

            // Recover from original checkpoint
            clone.Initialize(cloneCheckpointDirectory, sharedLogDirectory, populateLogHandles: true);

            if (isAsync)
                await clone.Store.RecoverAsync(checkpointGuid);
            else
                clone.Store.Recover(checkpointGuid);

            // Both sessions should work concurrently
            SharedDirectoryTests.Test(original, checkpointGuid);
            SharedDirectoryTests.Test(clone, checkpointGuid);

            // Dispose original, files should not be deleted on Windows
            original.TearDown();

            if (RuntimeInformation.IsOSPlatform(System.Runtime.InteropServices.OSPlatform.Windows))
            {
                // Clone should still work on Windows
                ClassicAssert.IsFalse(SharedDirectoryTests.IsDirectoryEmpty(sharedLogDirectory));
                SharedDirectoryTests.Test(clone, checkpointGuid);
            }

            clone.TearDown();

            // Files should be deleted after both instances are closed
            ClassicAssert.IsTrue(SharedDirectoryTests.IsDirectoryEmpty(sharedLogDirectory));
        }

        private struct TsavoriteTestInstance
        {
            public string CheckpointDirectory { get; private set; }
            public string LogDirectory { get; private set; }
            public TsavoriteKV<AdId, NumClicks, StructStoreFunctions, StructAllocator> Store { get; private set; }
            public IDevice LogDevice { get; private set; }

            public void Initialize(string checkpointDirectory, string logDirectory, bool populateLogHandles = false)
            {
                if (!RuntimeInformation.IsOSPlatform(System.Runtime.InteropServices.OSPlatform.Windows))
                    populateLogHandles = false;

                CheckpointDirectory = checkpointDirectory;
                LogDirectory = logDirectory;

                string logFileName = "log";
                string deviceFileName = Path.Join(LogDirectory, logFileName);
                KeyValuePair<int, SafeFileHandle>[] initialHandles = null;
                if (populateLogHandles)
                {
                    var segmentIds = new List<int>();
                    foreach (FileInfo item in new DirectoryInfo(logDirectory).GetFiles(logFileName + "*"))
                    {
                        segmentIds.Add(int.Parse(item.Name.Replace(logFileName, "").Replace(".", "")));
                    }
                    segmentIds.Sort();
                    initialHandles = new KeyValuePair<int, SafeFileHandle>[segmentIds.Count];
                    for (int i = 0; i < segmentIds.Count; i++)
                    {
                        var segmentId = segmentIds[i];
#pragma warning disable CA1416 // populateLogHandles will be false for non-windows, so turn off the "not available on all platforms" message
                        var handle = LocalStorageDevice.CreateHandle(segmentId, disableFileBuffering: false, deleteOnClose: true, preallocateFile: false, segmentSize: -1, fileName: deviceFileName, IntPtr.Zero);
#pragma warning restore CA1416
                        initialHandles[i] = new KeyValuePair<int, SafeFileHandle>(segmentId, handle);
                    }
                }

                if (!RuntimeInformation.IsOSPlatform(System.Runtime.InteropServices.OSPlatform.Windows))
                {
                    LogDevice = new ManagedLocalStorageDevice(deviceFileName, deleteOnClose: true);
                }
                else
                {
                    LogDevice = new LocalStorageDevice(deviceFileName, deleteOnClose: true, disableFileBuffering: false, initialLogFileHandles: initialHandles);
                }

                Store = new(new()
                {
                    IndexSize = KeySpace,
                    LogDevice = LogDevice,
                    CheckpointDir = CheckpointDirectory
                }, StoreFunctions<AdId, NumClicks>.Create(new AdId.Comparer())
                    , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
                );
            }

            public void TearDown()
            {
                Store?.Dispose();
                Store = null;
                LogDevice?.Dispose();
                LogDevice = null;
            }
        }

        private static void Populate(TsavoriteKV<AdId, NumClicks, StructStoreFunctions, StructAllocator> store)
        {
            using var session = store.NewSession<AdInput, Output, Empty, Functions>(new Functions());
            var bContext = session.BasicContext;

            // Prepare the dataset
            var inputArray = new AdInput[NumOps];
            for (int i = 0; i < NumOps; i++)
            {
                inputArray[i].adId.adId = i % NumUniqueKeys;
                inputArray[i].numClicks.numClicks = 1;
            }

            // Process the batch of input data
            for (int i = 0; i < NumOps; i++)
            {
                _ = bContext.RMW(ref inputArray[i].adId, ref inputArray[i], Empty.Default);

                if (i % CompletePendingInterval == 0)
                {
                    _ = bContext.CompletePending(false);
                }
            }

            // Make sure operations are completed
            _ = bContext.CompletePending(true);
        }

        private static void Test(TsavoriteTestInstance tsavoriteInstance, Guid checkpointToken)
        {
            var checkpointInfo = default(HybridLogRecoveryInfo);
            checkpointInfo.Recover(checkpointToken,
                new DeviceLogCommitCheckpointManager(
                    new LocalStorageNamedDeviceFactoryCreator(),
                        new DefaultCheckpointNamingScheme(
                          new DirectoryInfo(tsavoriteInstance.CheckpointDirectory).FullName)));

            // Create array for reading
            var inputArray = new AdInput[NumUniqueKeys];
            for (int i = 0; i < NumUniqueKeys; i++)
            {
                inputArray[i].adId.adId = i;
                inputArray[i].numClicks.numClicks = 0;
            }

            var input = default(AdInput);
            var output = default(Output);

            using var session = tsavoriteInstance.Store.NewSession<AdInput, Output, Empty, Functions>(new Functions());
            var bContext = session.BasicContext;

            // Issue read requests
            for (var i = 0; i < NumUniqueKeys; i++)
            {
                var status = bContext.Read(ref inputArray[i].adId, ref input, ref output, Empty.Default);
                ClassicAssert.IsTrue(status.Found);
                inputArray[i].numClicks = output.value;
            }

            // Complete all pending requests
            _ = bContext.CompletePending(true);
            session.Dispose();
        }

        private static bool IsDirectoryEmpty(string path) => !Directory.Exists(path) || !Directory.EnumerateFileSystemEntries(path).Any();

        private static void CopyDirectory(DirectoryInfo source, DirectoryInfo target)
        {
            // Copy each file
            foreach (var file in source.GetFiles())
            {
                _ = file.CopyTo(Path.Combine(target.FullName, file.Name), true);
            }

            // Copy each subdirectory
            foreach (var sourceSubDirectory in source.GetDirectories())
            {
                var targetSubDirectory = target.CreateSubdirectory(sourceSubDirectory.Name);
                CopyDirectory(sourceSubDirectory, targetSubDirectory);
            }
        }
    }
}