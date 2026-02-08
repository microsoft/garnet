// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Allure.NUnit;
using Garnet.test;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;
using Tsavorite.devices;
using Tsavorite.test.recovery;

namespace Tsavorite.test
{
    using LongAllocator = SpanByteAllocator<StoreFunctions<LongKeyComparer, SpanByteRecordDisposer>>;
    using LongStoreFunctions = StoreFunctions<LongKeyComparer, SpanByteRecordDisposer>;

    [AllureNUnit]
    [TestFixture]
    public class CheckpointManagerTests : AllureTestBase
    {
        private readonly Random random = new(0);

        [Test]
        [Category("CheckpointRestore")]
        [Category("Smoke")]
        public async Task CheckpointManagerPurgeCheck([Values] DeviceMode deviceMode)
        {
            DeviceLogCommitCheckpointManager checkpointManager;
            if (deviceMode == DeviceMode.Local)
            {
                checkpointManager = new DeviceLogCommitCheckpointManager(
                    new LocalStorageNamedDeviceFactoryCreator(),
                    new DefaultCheckpointNamingScheme(Path.Join(TestUtils.MethodTestDir, "checkpoints")), false); // PurgeAll deletes this directory
            }
            else
            {
                TestUtils.IgnoreIfNotRunningAzureTests();
                checkpointManager = new DeviceLogCommitCheckpointManager(
                    TestUtils.AzureStorageNamedDeviceFactoryCreator,
                    new AzureCheckpointNamingScheme($"{TestUtils.AzureTestContainer}/{TestUtils.AzureTestDirectory}"), false);
            }

            using (var log = Devices.CreateLogDevice(Path.Join(TestUtils.MethodTestDir, "hlog.log"), deleteOnClose: true))
            {
                TestUtils.RecreateDirectory(TestUtils.MethodTestDir);

                using var store = new TsavoriteKV<LongStoreFunctions, LongAllocator>(
                    new()
                    {
                        IndexSize = 1L << 16,
                        LogDevice = log,
                        MutableFraction = 1,
                        PageSize = 1L << 10,
                        MemorySize = 1L << 20,
                        CheckpointManager = checkpointManager
                    }, StoreFunctions.Create(LongKeyComparer.Instance, SpanByteRecordDisposer.Instance)
                    , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
                );
                using var s = store.NewSession<long, long, Empty, SimpleLongSimpleFunctions>(new SimpleLongSimpleFunctions());
                var bContext = s.BasicContext;

                var logCheckpoints = new Dictionary<Guid, int>();
                var indexCheckpoints = new Dictionary<Guid, int>();
                var fullCheckpoints = new Dictionary<Guid, int>();

                for (var i = 0; i < 10; i++)
                {
                    // Do some dummy update
                    var key = 0L;
                    var value = (long)random.Next();
                    _ = bContext.Upsert(SpanByte.FromPinnedVariable(ref key), SpanByte.FromPinnedVariable(ref value));

                    var checkpointType = random.Next(5);
                    Guid result = default;
                    switch (checkpointType)
                    {
                        case 0:
                            _ = store.TryInitiateHybridLogCheckpoint(out result, CheckpointType.FoldOver);
                            logCheckpoints.Add(result, 0);
                            break;
                        case 1:
                            _ = store.TryInitiateHybridLogCheckpoint(out result, CheckpointType.Snapshot);
                            logCheckpoints.Add(result, 0);
                            break;
                        case 2:
                            _ = store.TryInitiateIndexCheckpoint(out result);
                            indexCheckpoints.Add(result, 0);
                            break;
                        case 3:
                            _ = store.TryInitiateFullCheckpoint(out result, CheckpointType.FoldOver);
                            fullCheckpoints.Add(result, 0);
                            break;
                        case 4:
                            _ = store.TryInitiateFullCheckpoint(out result, CheckpointType.Snapshot);
                            fullCheckpoints.Add(result, 0);
                            break;
                        default:
                            ClassicAssert.True(false);
                            break;
                    }

                    await store.CompleteCheckpointAsync();
                }

                ClassicAssert.AreEqual(checkpointManager.GetLogCheckpointTokens().ToDictionary(guid => guid, _ => 0),
                    logCheckpoints.Union(fullCheckpoints).ToDictionary(e => e.Key, e => e.Value));
                ClassicAssert.AreEqual(checkpointManager.GetIndexCheckpointTokens().ToDictionary(guid => guid, _ => 0),
                    indexCheckpoints.Union(fullCheckpoints).ToDictionary(e => e.Key, e => e.Value));

                if (logCheckpoints.Count != 0)
                {
                    var guid = logCheckpoints.First().Key;
                    checkpointManager.Purge(guid);
                    _ = logCheckpoints.Remove(guid);
                    ClassicAssert.AreEqual(checkpointManager.GetLogCheckpointTokens().ToDictionary(guid => guid, _ => 0),
                        logCheckpoints.Union(fullCheckpoints).ToDictionary(e => e.Key, e => e.Value));
                    ClassicAssert.AreEqual(checkpointManager.GetIndexCheckpointTokens().ToDictionary(guid => guid, _ => 0),
                        indexCheckpoints.Union(fullCheckpoints).ToDictionary(e => e.Key, e => e.Value));
                }

                if (indexCheckpoints.Count != 0)
                {
                    var guid = indexCheckpoints.First().Key;
                    checkpointManager.Purge(guid);
                    _ = indexCheckpoints.Remove(guid);
                    ClassicAssert.AreEqual(checkpointManager.GetLogCheckpointTokens().ToDictionary(guid => guid, _ => 0),
                        logCheckpoints.Union(fullCheckpoints).ToDictionary(e => e.Key, e => e.Value));
                    ClassicAssert.AreEqual(checkpointManager.GetIndexCheckpointTokens().ToDictionary(guid => guid, _ => 0),
                        indexCheckpoints.Union(fullCheckpoints).ToDictionary(e => e.Key, e => e.Value));
                }


                if (fullCheckpoints.Count != 0)
                {
                    var guid = fullCheckpoints.First().Key;
                    checkpointManager.Purge(guid);
                    _ = fullCheckpoints.Remove(guid);
                    ClassicAssert.AreEqual(checkpointManager.GetLogCheckpointTokens().ToDictionary(guid => guid, _ => 0),
                        logCheckpoints.Union(fullCheckpoints).ToDictionary(e => e.Key, e => e.Value));
                    ClassicAssert.AreEqual(checkpointManager.GetIndexCheckpointTokens().ToDictionary(guid => guid, _ => 0),
                        indexCheckpoints.Union(fullCheckpoints).ToDictionary(e => e.Key, e => e.Value));
                }

                checkpointManager.PurgeAll();
                ClassicAssert.IsEmpty(checkpointManager.GetLogCheckpointTokens());
                ClassicAssert.IsEmpty(checkpointManager.GetIndexCheckpointTokens());
            }
            checkpointManager.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
        }
    }
}